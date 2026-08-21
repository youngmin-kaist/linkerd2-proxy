//! Configures and executes the proxy

#![deny(rust_2018_idioms, clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(opaque_hidden_inferred_bound)]
#![forbid(unsafe_code)]

#[cfg(feature = "doca")]
pub mod dmesh;
pub mod dst;
pub mod env;
pub mod identity;
pub mod policy;
pub mod spire;
pub mod tap;
pub mod trace_collector;

pub use self::metrics::Metrics;
use futures::{future, Future, FutureExt};
use linkerd_app_admin as admin;
#[cfg(feature = "doca")]
use linkerd_app_core::svc;
use linkerd_app_core::{
    config::ServerConfig,
    control::{ControlAddr, Metrics as ControlMetrics},
    dns, drain,
    metrics::{legacy::FmtMetrics, prom},
    serve,
    svc::Param,
    tls_info,
    transport::{addrs::*, listen::Bind},
    Error, ProxyRuntime,
};
pub use linkerd_app_core::{metrics, trace, transport::BindTcp, BUILD_INFO};
use linkerd_app_gateway as gateway;
use linkerd_app_inbound::{self as inbound, Inbound};
use linkerd_app_outbound::{self as outbound, Outbound};
pub use linkerd_workers::Workers;
use std::pin::Pin;
use tokio::{
    sync::mpsc,
    time::{self, Duration},
};
use tracing::{debug, error, info, info_span, Instrument};

/// Spawns a sidecar proxy.
///
/// The proxy binds two listeners:
///
/// - a private socket (TCP or UNIX) for outbound requests to other instances;
/// - and a public socket (TCP and optionally TLS) for inbound requests from other
///   instances.
///
/// The public listener forwards requests to a local socket (TCP or UNIX).
///
/// The private listener routes requests to service-discovery-aware load-balancer.
///
#[derive(Clone, Debug)]
pub struct Config {
    pub outbound: outbound::Config,
    pub inbound: inbound::Config,
    pub gateway: gateway::Config,

    pub dns: dns::Config,
    pub identity: identity::Config,
    pub dst: dst::Config,
    pub policy: policy::Config,
    pub admin: admin::Config,
    pub tap: tap::Config,
    pub trace_collector: trace_collector::Config,

    /// Grace period for graceful shutdowns.
    ///
    /// If the proxy does not shut down gracefully within this timeout, it will
    /// terminate forcefully, closing any remaining connections.
    pub shutdown_grace_period: time::Duration,

    /// The inbound policy configuration the per-destination-Pod stores are
    /// built from, kept when the process-wide one is pinned to its default.
    #[cfg(feature = "doca")]
    pub dmesh_inbound_policy: Option<inbound::policy::Config>,
}

pub struct App {
    admin: admin::Task,
    drain: drain::Signal,
    dst: ControlAddr,
    identity: identity::Identity,
    inbound_addr: Local<ServerAddr>,
    trace_collector: trace_collector::TraceCollector,
    outbound_addr: Local<ServerAddr>,
    outbound_addr_additional: Option<Local<ServerAddr>>,
    start_proxy: Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>>,
    tap: tap::Tap,
    #[cfg(feature = "doca")]
    dmesh_outbound: svc::ArcNewTcp<dmesh::DmeshTarget, dmesh_doca::DmeshIo>,
    #[cfg(feature = "doca")]
    dmesh_drain: drain::Watch,
    #[cfg(feature = "doca")]
    dmesh_backends: std::sync::Arc<dmesh_doca::Backends>,
    #[cfg(feature = "doca")]
    dmesh_metrics: std::sync::Arc<dmesh_doca::SessionMetrics>,
    /// Builds one inbound policy store per destination workload.
    ///
    /// A sidecar builds one store for the one workload it is the proxy for.
    /// This proxy is the inbound enforcement point for every Pod its DPU
    /// serves, so a store is bound per registered destination Pod and its
    /// per-port watches are shared by every stream arriving at that Pod: the
    /// cost scales with destination Pods and ports rather than with sessions.
    #[cfg(feature = "doca")]
    dmesh_inbound_policies: DmeshInboundPolicyBuilder,
}

/// A store of inbound policy watches, held per destination workload.
#[cfg(feature = "doca")]
pub type DmeshPolicyStore = std::sync::Arc<dyn inbound::policy::GetPolicy + Send + Sync + 'static>;

/// Whether one destination Pod's inbound policy admits a connection.
///
/// Two inputs decide it and a Pod supplies neither. `client` is the source
/// Pod's real cluster address, because the stock evaluation matches an
/// authorization's `networks` first and an empty match denies — a synthetic
/// address would make every realistic policy refuse every connection.
/// `client_identity` is presented as a TLS *state* rather than as a string,
/// because `Authentication::TlsAuthenticated` matches nothing else; `None` is
/// an established connection carrying no client identity, which is what an
/// unattested source is.
#[cfg(feature = "doca")]
pub fn dmesh_connection_verdict(
    store: &DmeshPolicyStore,
    destination: std::net::SocketAddr,
    client: std::net::SocketAddr,
    client_identity: Option<&str>,
) -> bool {
    use inbound::policy::GetPolicy;
    use linkerd_app_core::{
        identity, tls,
        transport::{ClientAddr, OrigDstAddr, Remote},
    };

    let client_id = client_identity
        .filter(|id| !id.is_empty())
        .and_then(|id| id.parse::<identity::Id>().ok())
        .map(tls::server::ClientId);
    let tls = tls::ConditionalServerTls::Some(tls::ServerTls::Established {
        client_id,
        negotiated_protocol: None,
    });
    store
        .get_policy(OrigDstAddr(destination))
        .admits(Remote(ClientAddr(client)), &tls)
}

#[cfg(feature = "doca")]
pub type DmeshInboundPolicyBuilder =
    std::sync::Arc<dyn Fn(std::sync::Arc<str>) -> DmeshPolicyStore + Send + Sync + 'static>;

// === impl Config ===

impl Config {
    pub fn try_from_env() -> Result<Self, env::EnvError> {
        env::Env.try_config()
    }

    /// The proxy's own inbound and admin listeners are ephemeral and belong to
    /// no Pod, so asking the Kubernetes policy controller about their ports
    /// gets "unknown server" and nothing else. They keep their configured
    /// default policy.
    ///
    /// This is not the inbound enforcement the DPU performs. That is per
    /// destination Pod and fully dynamic: `dmesh_inbound_policy_config` keeps
    /// the discovering configuration this rewrites away, and the per-workload
    /// stores are built from it.
    #[cfg(feature = "doca")]
    pub fn disable_inbound_policy_discovery(&mut self) {
        self.dmesh_inbound_policy = Some(self.inbound.policy.clone());
        let inbound::policy::Config::Discover {
            default,
            cache_max_idle_age,
            opaque_ports,
            ..
        } = self.inbound.policy.clone()
        else {
            return;
        };
        self.inbound.policy = inbound::policy::Config::Fixed {
            default,
            cache_max_idle_age,
            ports: Default::default(),
            opaque_ports,
        };
    }

    /// Build an application.
    ///
    /// It is currently required that this be run on a Tokio runtime, since some
    /// services are created eagerly and must spawn tasks to do so.
    pub async fn build<BIn, BOut, BAdmin>(
        self,
        bind_in: BIn,
        bind_out: BOut,
        bind_admin: BAdmin,
        shutdown_tx: mpsc::UnboundedSender<()>,
        log_level: trace::Handle,
        mut registry: prom::Registry,
    ) -> Result<App, Error>
    where
        BIn: Bind<ServerConfig, BoundAddrs = Local<ServerAddr>> + 'static,
        BIn::Io: linkerd_app_core::io::DmeshSession,
        BIn::Addrs: Param<Remote<ClientAddr>>
            + Param<Local<ServerAddr>>
            + Param<OrigDstAddr>
            + Param<AddrPair>,
        BOut: Bind<ServerConfig, BoundAddrs = DualLocal<ServerAddr>> + 'static,
        BOut::Io: linkerd_app_core::io::DmeshSession,
        BOut::Addrs: Param<Remote<ClientAddr>>
            + Param<Local<ServerAddr>>
            + Param<OrigDstAddr>
            + Param<AddrPair>,
        BAdmin: Bind<ServerConfig, BoundAddrs = Local<ServerAddr>> + Clone + 'static,
        BAdmin::Addrs: Param<Remote<ClientAddr>> + Param<Local<ServerAddr>> + Param<AddrPair>,
    {
        #[cfg(feature = "doca")]
        let dmesh_inbound_policy = self.dmesh_inbound_policy.clone();
        let Config {
            admin,
            dns,
            dst,
            policy,
            identity,
            inbound,
            trace_collector,
            outbound,
            gateway,
            tap,
            ..
        } = self;
        debug!("Building app");
        let (metrics, report) = Metrics::new(admin.metrics_retain_idle);

        debug!("Building DNS client");
        let dns = dns.build(registry.sub_registry_with_prefix("control_dns"));

        // Ensure that we've obtained a valid identity before binding any servers.
        debug!("Building Identity client");
        let identity = {
            let id_metrics = identity::IdentityMetrics::register(
                registry.sub_registry_with_prefix("control_identity"),
            );

            info_span!("identity").in_scope(|| {
                identity.build(
                    dns.resolver("identity"),
                    metrics.control.clone(),
                    id_metrics,
                )
            })?
        };

        let (drain_tx, drain_rx) = drain::channel();

        debug!(config = ?tap, "Building Tap server");
        let tap = {
            let bind = bind_admin.clone();
            info_span!("tap")
                .in_scope(|| tap.build(bind, identity.receiver().server(), drain_rx.clone()))?
        };

        debug!("Building Destination client");
        let dst = {
            let control_metrics =
                ControlMetrics::register(registry.sub_registry_with_prefix("control_destination"));
            let metrics = metrics.control.clone();
            let dns = dns.resolver("destination");
            info_span!("dst").in_scope(|| {
                dst.build(
                    dns,
                    metrics,
                    control_metrics,
                    identity.receiver().new_client(),
                )
            })
        }?;

        debug!("Building Policy client");
        let export_hostname_labels = policy.export_hostname_labels;
        let policies = {
            let control_metrics =
                ControlMetrics::register(registry.sub_registry_with_prefix("control_policy"));
            let dns = dns.resolver("policy");
            let metrics = metrics.control.clone();
            info_span!("policy").in_scope(|| {
                policy.build(
                    dns,
                    metrics,
                    control_metrics,
                    identity.receiver().new_client(),
                )
            })
        }?;

        debug!(config = ?trace_collector, "Building trace collector");
        let trace_collector = {
            let control_metrics = if let Some(prefix) = trace_collector.metrics_prefix() {
                ControlMetrics::register(registry.sub_registry_with_prefix(prefix))
            } else {
                ControlMetrics::register(&mut prom::Registry::default())
            };
            let identity = identity.receiver().new_client();
            let dns = dns.resolver("trace_collector");
            let client_metrics = metrics.control.clone();
            let otel_metrics = metrics.opentelemetry;
            info_span!("tracing").in_scope(|| {
                trace_collector.build(identity, dns, otel_metrics, control_metrics, client_metrics)
            })
        }?;

        let runtime = ProxyRuntime {
            identity: identity.receiver(),
            metrics: metrics.proxy,
            tap: tap.registry(),
            span_sink: trace_collector.span_sink(),
            drain: drain_rx.clone(),
        };
        let inbound = Inbound::new(
            inbound,
            runtime.clone(),
            registry.sub_registry_with_prefix("inbound"),
        );
        #[cfg(feature = "doca")]
        let mut outbound = outbound;
        // The DMesh backend registry and its session counters belong to this
        // worker: the connector takes channels from the same registry the
        // acceptor publishes into, and nothing is shared between workers.
        #[cfg(feature = "doca")]
        let dmesh_handles = {
            let backends = std::sync::Arc::new(dmesh_doca::Backends::new());
            let metrics =
                dmesh_doca::SessionMetrics::register(registry.sub_registry_with_prefix("dmesh"));
            outbound.dmesh = Some(dmesh_doca::Dmesh {
                backends: backends.clone(),
                metrics: metrics.clone(),
            });
            (backends, metrics)
        };
        let outbound = Outbound::new(
            outbound,
            runtime,
            registry.sub_registry_with_prefix("outbound"),
        );

        let inbound_policies = inbound.build_policies(
            policies.workload.clone(),
            policies.client.clone(),
            policies.backoff,
            policies.limits,
        );

        // The same construction, deferred and per workload: the DMesh adapter
        // binds one store to each destination Pod it serves when that Pod
        // registers, and drops it when the registration ends.
        #[cfg(feature = "doca")]
        let dmesh_inbound_policies: DmeshInboundPolicyBuilder = {
            // The discovering configuration, even where the process-wide store
            // was pinned to its default for the proxy's own ephemeral ports.
            let mut inbound = inbound.clone();
            if let Some(config) = dmesh_inbound_policy {
                inbound.set_policy_config(config);
            }
            let client = policies.client.clone();
            let backoff = policies.backoff;
            let limits = policies.limits;
            std::sync::Arc::new(move |workload: std::sync::Arc<str>| {
                std::sync::Arc::new(inbound.build_policies(
                    workload,
                    client.clone(),
                    backoff,
                    limits,
                )) as DmeshPolicyStore
            })
        };

        let outbound_policies = outbound.build_policies(
            policies.workload.clone(),
            policies.client.clone(),
            policies.backoff,
            policies.limits,
            export_hostname_labels,
        );

        let gateway = gateway::Gateway::new(gateway, inbound.clone(), outbound.clone()).stack(
            dst.resolve.clone(),
            dst.profiles.clone(),
            outbound_policies.clone(),
        );

        // Bind the proxy sockets eagerly (so they're reserved and known) but defer building the
        // stacks until the proxy starts running.
        let (inbound_addr, inbound_listen) = bind_in
            .bind(&inbound.config().proxy.server)
            .expect("Failed to bind inbound listener");
        let inbound_metrics = inbound.metrics();
        let inbound = inbound.mk(
            inbound_addr,
            inbound_policies.clone(),
            dst.profiles.clone(),
            gateway.into_inner(),
        );

        let ((outbound_addr, outbound_addr_additional), outbound_listen) = bind_out
            .bind(&outbound.config().proxy.server)
            .expect("Failed to bind outbound listener");
        let outbound_metrics = outbound.metrics();
        // Build the outbound stacks serving DMA frontend connections. Opaque
        // byte streams share one stack per source workload because their
        // session remains attached to the I/O through the connector. HTTP
        // sessions get a session-local stack so physical H1/H2 transports and
        // reconnect caches cannot cross SessionToken boundaries.
        #[cfg(feature = "doca")]
        let dmesh_outbound: svc::ArcNewTcp<dmesh::DmeshTarget, dmesh_doca::DmeshIo> = {
            let template = outbound.clone();
            let profiles = dst.profiles.clone();
            let default_workload = policies.workload.clone();
            let policy_client = policies.client.clone();
            let policy_backoff = policies.backoff;
            let policy_limits = policies.limits;
            let resolve = dst.resolve.clone();
            let metrics = dmesh_handles.1.clone();
            // Keyed by workload, which the per-node registration cap bounds;
            // the guard is a backstop against unbounded workload churn.
            let cache: parking_lot::Mutex<
                std::collections::HashMap<
                    std::sync::Arc<str>,
                    svc::ArcNewTcp<dmesh::DmeshTarget, dmesh_doca::DmeshIo>,
                >,
            > = Default::default();
            const CACHE_CAP: usize = 64;
            svc::ArcNewService::new(move |target: dmesh::DmeshTarget| {
                // A stock sidecar has one process-wide workload. This DPU
                // proxy serves many Pods, so bind each session's policy watch
                // to the workload granted with that Pod's registration. Empty
                // workload values retain the configured default for legacy
                // clients; production registration requires an explicit one.
                let workload = if target.policy_workload().is_empty() {
                    default_workload.clone()
                } else {
                    target.policy_workload().clone()
                };

                /* Opaque forwarding carries SessionToken on the source I/O all
                 * the way to the connector, so its policy/discovery stack may
                 * be shared. HTTP parsing turns the byte stream into requests;
                 * keep that stack session-local so its H1/H2 pools and reconnect
                 * caches can only consume this session's backend channel. */
                let share_this_session = !target.protocol_aware();
                if share_this_session {
                    if let Some(stack) = cache.lock().get(&workload) {
                        metrics.session_stack_cache_hits.inc();
                        return svc::NewService::new_service(stack, target);
                    }
                }

                let started = std::time::Instant::now();
                let mut outbound = template.clone();
                // A shared stack serves many sessions, so no session is baked
                // in; each connection carries its own. A per-session stack
                // bakes its session as a fallback and consistency check.
                outbound.config_mut().dmesh_session = if share_this_session {
                    None
                } else {
                    Some(target.param())
                };
                outbound.config_mut().dmesh_origin = true;
                let configure = started.elapsed();

                let started = std::time::Instant::now();
                let session_policies = outbound.build_policies(
                    workload.clone(),
                    policy_client.clone(),
                    policy_backoff,
                    policy_limits,
                    export_hostname_labels,
                );
                let stack = outbound.mk(profiles.clone(), session_policies, resolve.clone());
                let layers = started.elapsed();

                let started = std::time::Instant::now();
                let service = svc::NewService::new_service(&stack, target);
                metrics.observe_stack_build(configure, layers, started.elapsed());
                if share_this_session {
                    metrics.session_stack_cache_misses.inc();
                    let mut cache = cache.lock();
                    if cache.len() >= CACHE_CAP {
                        tracing::warn!(
                            workloads = cache.len(),
                            "Dropping the shared dmesh stack cache; workload churn exceeded its cap"
                        );
                        cache.clear();
                    }
                    cache.insert(workload, stack);
                }
                service
            })
        };
        #[cfg(feature = "doca")]
        let (dmesh_backends, dmesh_metrics) = dmesh_handles;
        let outbound = outbound.mk(dst.profiles.clone(), outbound_policies, dst.resolve.clone());

        // Keep a drain subscriber for the dmesh acceptor (spawned post-build).
        #[cfg(feature = "doca")]
        let dmesh_drain = drain_rx.clone();

        // Build a task that initializes and runs the proxy stacks.
        let start_proxy = {
            let drain_rx = drain_rx.clone();
            let identity_ready = identity.ready();

            Box::pin(async move {
                Self::await_identity(identity_ready).await;

                tokio::spawn(
                    serve::serve(outbound_listen, outbound, drain_rx.clone().signaled())
                        .instrument(info_span!("outbound").or_current()),
                );

                tokio::spawn(
                    serve::serve(inbound_listen, inbound, drain_rx.signaled())
                        .instrument(info_span!("inbound").or_current()),
                );
            })
        };

        if let Err(error) = metrics::process::register(registry.sub_registry_with_prefix("process"))
        {
            error!(%error, "Failed to register process metrics");
        }
        registry.register("proxy_build_info", "Proxy build info", BUILD_INFO.metric());
        registry.register("rustls_info", "Proxy TLS info", tls_info::metric());

        let admin = {
            let identity = identity.receiver().server();
            let metrics = inbound_metrics.clone();
            let report = inbound_metrics
                .and_report(outbound_metrics)
                .and_report(report)
                // The prom registry reports an "# EOF" at the end of its export, so
                // it should be emitted last.
                .and_report(prom::Report::from(registry));
            info_span!("admin").in_scope(move || {
                admin.build(
                    bind_admin,
                    inbound_policies,
                    identity,
                    report,
                    metrics,
                    log_level,
                    drain_rx,
                    shutdown_tx,
                )
            })?
        };

        Ok(App {
            admin,
            dst: dst.addr,
            drain: drain_tx,
            identity,
            inbound_addr,
            trace_collector,
            outbound_addr,
            outbound_addr_additional,
            start_proxy,
            tap,
            #[cfg(feature = "doca")]
            dmesh_outbound,
            #[cfg(feature = "doca")]
            dmesh_drain,
            #[cfg(feature = "doca")]
            dmesh_backends,
            #[cfg(feature = "doca")]
            dmesh_metrics,
            #[cfg(feature = "doca")]
            dmesh_inbound_policies,
        })
    }

    /// Waits for the proxy's identity to be certified.
    ///
    /// If this does not complete in a timely fashion, warnings are logged every 15s
    async fn await_identity(mut fut: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) {
        const TIMEOUT: time::Duration = time::Duration::from_secs(15);
        loop {
            tokio::select! {
                _ = (&mut fut) => return,
                _ = time::sleep(TIMEOUT) => {
                    tracing::warn!("Waiting for identity to be initialized...");
                }
            }
        }
    }
}

// === impl App ===

impl App {
    pub fn admin_addr(&self) -> Local<ServerAddr> {
        self.admin.listen_addr
    }

    /// Spawn the DMA (dmesh) acceptor: DMA-received connections are driven
    /// through the outbound stack. `events` is the driver's event stream and
    /// `registrar` binds per-connection IO handles back to the driver.
    #[cfg(feature = "doca")]
    pub fn spawn_dmesh(
        &self,
        events: mpsc::UnboundedReceiver<dmesh_doca::DmeshEvent>,
        registrar: dmesh_doca::Registrar,
    ) {
        let outbound = self.dmesh_outbound.clone();
        let shutdown = self.dmesh_drain.clone().signaled();
        let backends = self.dmesh_backends.clone();
        let metrics = self.dmesh_metrics.clone();
        tokio::spawn(
            dmesh::serve(events, registrar, outbound, backends, metrics, shutdown)
                .instrument(info_span!("dmesh").or_current()),
        );
    }

    /// The backend registry the DMesh connector takes channels from.
    #[cfg(feature = "doca")]
    pub fn dmesh_backends(&self) -> std::sync::Arc<dmesh_doca::Backends> {
        self.dmesh_backends.clone()
    }

    /// Session counters shared by the adapter, the acceptor and the connector.
    #[cfg(feature = "doca")]
    pub fn dmesh_metrics(&self) -> std::sync::Arc<dmesh_doca::SessionMetrics> {
        self.dmesh_metrics.clone()
    }

    /// One inbound policy store for one destination workload. Its per-port
    /// watches start on first use and end when the store is dropped, so the
    /// adapter's cache lifetime is the watch lifetime.
    #[cfg(feature = "doca")]
    pub fn dmesh_inbound_policies(&self, workload: std::sync::Arc<str>) -> DmeshPolicyStore {
        (self.dmesh_inbound_policies)(workload)
    }

    /// The builder itself, so a caller can keep binding stores after `spawn`
    /// has consumed the app.
    #[cfg(feature = "doca")]
    pub fn dmesh_inbound_policy_builder(&self) -> DmeshInboundPolicyBuilder {
        self.dmesh_inbound_policies.clone()
    }

    pub fn inbound_addr(&self) -> Local<ServerAddr> {
        self.inbound_addr
    }

    pub fn outbound_addr(&self) -> Local<ServerAddr> {
        self.outbound_addr
    }

    pub fn outbound_addr_additional(&self) -> Option<Local<ServerAddr>> {
        self.outbound_addr_additional
    }

    pub fn tap_addr(&self) -> Option<Local<ServerAddr>> {
        match self.tap {
            tap::Tap::Disabled { .. } => None,
            tap::Tap::Enabled { listen_addr, .. } => Some(listen_addr),
        }
    }

    pub fn dst_addr(&self) -> &ControlAddr {
        &self.dst
    }

    pub fn local_server_name(&self) -> dns::Name {
        self.identity.receiver().server_name().clone()
    }

    pub fn local_tls_id(&self) -> identity::Id {
        self.identity.receiver().local_id().clone()
    }

    pub fn tracing_addr(&self) -> Option<&ControlAddr> {
        match self.trace_collector {
            trace_collector::TraceCollector::Disabled => None,
            crate::trace_collector::TraceCollector::Enabled(ref oc) => Some(&oc.addr),
        }
    }

    pub fn spawn(self) -> drain::Signal {
        let App {
            admin,
            drain,
            identity,
            trace_collector: collector,
            start_proxy,
            tap,
            ..
        } = self;

        // Run a daemon thread for all administrative tasks.
        //
        // The main reactor holds `admin_shutdown_tx` until the reactor drops
        // the task. This causes the daemon reactor to stop.
        let (admin_shutdown_tx, admin_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        debug!("spawning daemon thread");
        tokio::spawn(future::pending().map(|()| drop(admin_shutdown_tx)));
        std::thread::Builder::new()
            .name("admin".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("building admin runtime must succeed");
                rt.block_on(
                    async move {
                        debug!("running admin thread");

                        // Start the admin server to serve the readiness endpoint.
                        tokio::spawn(
                            admin
                                .serve
                                .instrument(info_span!("admin", listen.addr = %admin.listen_addr)),
                        );

                        // Kick off the identity so that the process can become ready.
                        let local = identity.receiver();
                        let local_id = local.local_id().clone();
                        let ready = identity.ready();
                        tokio::spawn(
                            identity
                                .run()
                                .instrument(info_span!("identity").or_current()),
                        );

                        let latch = admin.latch;
                        tokio::spawn(
                            ready
                                .map(move |()| {
                                    latch.release();
                                    info!(id = %local_id, "Certified identity");
                                })
                                .instrument(info_span!("identity").or_current()),
                        );

                        if let tap::Tap::Enabled {
                            registry, serve, ..
                        } = tap
                        {
                            let clean = time::interval(Duration::from_secs(60));
                            let clean = tokio_stream::wrappers::IntervalStream::new(clean);
                            tokio::spawn(
                                registry
                                    .clean(clean)
                                    .instrument(info_span!("tap_clean").or_current()),
                            );
                            tokio::spawn(serve.instrument(info_span!("tap").or_current()));
                        }

                        if let trace_collector::TraceCollector::Enabled(collector) = collector {
                            tokio::spawn(collector.task.instrument(info_span!("tracing")));
                        }

                        // we don't care if the admin shutdown channel is
                        // dropped or actually triggered.
                        let _ = admin_shutdown_rx.await;
                    }
                    .instrument(info_span!("daemon")),
                )
            })
            .expect("admin");

        tokio::spawn(start_proxy);

        drain
    }
}
