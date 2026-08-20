//! Feeds connections received over the DPU DMA path into the outbound stack.
//!
//! This is the DMA analogue of `serve::serve`: instead of a TCP listener it
//! consumes [`dmesh_doca::DmeshEvent`]s from the driver. On each `ConnReady`
//! it builds a [`DmeshTarget`] (carrying the flow's original destination, the
//! routing key normally recovered via `SO_ORIGINAL_DST`) and a
//! [`dmesh_doca::DmeshIo`], registers the IO handle with the driver, then
//! drives the outbound `NewService` on a per-connection task. The outbound
//! stack then applies protocol detection, discovery, load balancing and mTLS
//! exactly as for an intercepted TCP connection.
//!
//! The acceptor owns those tasks. Every one of them is filed under the
//! [`SessionToken`] that opened it, so a close cancels exactly the task it
//! belongs to and a slot handed out again cannot cancel its successor.

use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use dmesh_doca::{
    dmesh_io_pair, BackendKey, Backends, DmeshEvent, DmeshIo, FlowId, Registrar, Registration,
    SessionMetrics, SessionToken,
};
use linkerd_app_core::{
    svc::{self, NewService, Param, ServiceExt},
    transport::addrs::{AddrPair, ClientAddr, OrigDstAddr, Remote, ServerAddr},
};
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};
use tracing::{debug, debug_span, info, warn, Instrument};

/// Server-side target for a DMA-received connection. Mirrors the `Param` impls
/// of `linkerd_proxy_transport::orig_dst::Addrs` that the outbound stack reads.
///
/// The application uses the session to build one complete DMesh-only outbound
/// stack. Every cache in that stack is therefore scoped to this session even
/// though the stock Linkerd target types continue to key by destination.
#[derive(Clone, Debug)]
pub struct DmeshTarget {
    orig_dst: OrigDstAddr,
    client: Remote<ClientAddr>,
    session: SessionToken,
    /// Workload identity granted with the frontend Pod registration. A DPU
    /// proxy serves many workloads, so this cannot be the process-wide policy
    /// workload used by an ordinary one-Pod sidecar.
    policy_workload: Arc<str>,
}

impl Param<OrigDstAddr> for DmeshTarget {
    fn param(&self) -> OrigDstAddr {
        self.orig_dst
    }
}

impl Param<Remote<ClientAddr>> for DmeshTarget {
    fn param(&self) -> Remote<ClientAddr> {
        self.client
    }
}

impl Param<AddrPair> for DmeshTarget {
    fn param(&self) -> AddrPair {
        let Remote(client) = self.client;
        AddrPair(client, ServerAddr(self.orig_dst.into()))
    }
}

impl Param<SessionToken> for DmeshTarget {
    fn param(&self) -> SessionToken {
        self.session
    }
}

impl DmeshTarget {
    fn new(flow: &FlowId, session: SessionToken) -> Self {
        Self {
            orig_dst: OrigDstAddr(SocketAddr::V4(flow.dst)),
            client: Remote(ClientAddr(SocketAddr::V4(flow.src))),
            session,
            policy_workload: Arc::from(flow.workload.as_str()),
        }
    }

    pub(crate) fn policy_workload(&self) -> &Arc<str> {
        &self.policy_workload
    }
}

/// Connection tasks the acceptor owns, and the backend channels it published.
struct Sessions {
    tasks: HashMap<SessionToken, JoinHandle<()>>,
    backends: HashMap<SessionToken, BackendKey>,
    /// Cancelled or finished tasks awaited outside event dispatch.
    joining: JoinSet<()>,
    metrics: Arc<SessionMetrics>,
    registry: Arc<Backends>,
}

impl Sessions {
    fn new(registry: Arc<Backends>, metrics: Arc<SessionMetrics>) -> Self {
        Self {
            tasks: HashMap::new(),
            backends: HashMap::new(),
            joining: JoinSet::new(),
            metrics,
            registry,
        }
    }

    fn track(&mut self, token: SessionToken, task: JoinHandle<()>) {
        if let Some(previous) = self.tasks.insert(token, task) {
            // The datapath must not hand out a token twice; if it did, the
            // older task is the one with no owner left.
            warn!(session = %token, "dmesh session token reused; cancelling the older task");
            previous.abort();
            self.joining.spawn(async move {
                let _ = previous.await;
            });
        } else {
            self.metrics.tasks_live.inc();
        }
    }

    /// Stop one session: evict its backend channel, cancel its task and hand
    /// the join to the reaper. Only the exact token is removed.
    fn close(&mut self, token: SessionToken) {
        if let Some(key) = self.backends.remove(&token) {
            if self.registry.remove(&key).is_some() {
                debug!(backend = %key, "dmesh backend channel withdrawn");
            }
        }
        let Some(task) = self.tasks.remove(&token) else {
            return;
        };
        self.metrics.tasks_live.dec();
        if !task.is_finished() {
            self.metrics.tasks_cancelled.inc();
            task.abort();
        }
        self.joining.spawn(async move {
            let _ = task.await;
        });
    }

    /// A task reported its own completion.
    fn finished(&mut self, token: SessionToken) {
        let Some(task) = self.tasks.remove(&token) else {
            return;
        };
        self.metrics.tasks_live.dec();
        // Receiving the completion message means the future reached its final
        // statement, not that Tokio has necessarily retired the task. Keep the
        // JoinHandle owned until it can be awaited outside event dispatch.
        self.joining.spawn(async move {
            let _ = task.await;
        });
    }

    fn publish_backend(&mut self, key: BackendKey, io: DmeshIo) {
        match self.registry.publish(key, io) {
            Ok(()) => {
                self.backends.insert(key.session, key);
            }
            Err(error) => warn!(backend = %key, %error, "dmesh backend channel refused"),
        }
    }

    /// Cancel every task and wait for all of them to end.
    async fn drain(&mut self) {
        // BACKEND-mode endpoints have a registry entry but no service task.
        // Include both tables so ending the event stream withdraws every
        // endpoint rather than leaving a taken or published channel behind.
        while let Some(token) = self.tasks.keys().next().copied() {
            self.close(token);
        }
        while let Some(token) = self.backends.keys().next().copied() {
            self.close(token);
        }
        while self.joining.join_next().await.is_some() {}
    }
}

/// Serve DMA connections through the outbound stack until the event stream ends
/// or shutdown is signalled. `outbound` is an `ArcNewTcp<DmeshTarget, DmeshIo>`
/// (built via `Outbound::mk` with `I = DmeshIo`).
pub async fn serve<N>(
    mut events: mpsc::UnboundedReceiver<DmeshEvent>,
    registrar: Registrar,
    outbound: N,
    registry: Arc<Backends>,
    metrics: Arc<SessionMetrics>,
    shutdown: impl Future,
) where
    N: NewService<DmeshTarget, Service = svc::BoxTcp<DmeshIo>> + Send + 'static,
{
    let mut sessions = Sessions::new(registry, metrics);
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<SessionToken>();
    tokio::pin!(shutdown);
    loop {
        let ev = tokio::select! {
            _ = &mut shutdown => break,
            Some(token) = done_rx.recv() => {
                // A completion names one generation. A later session holding
                // the same slot keeps its task.
                sessions.finished(token);
                continue;
            }
            Some(_) = sessions.joining.join_next() => continue,
            ev = events.recv() => match ev {
                Some(ev) => ev,
                None => break,
            },
        };

        match ev {
            DmeshEvent::ConnReady(token, flow) => {
                let peer = SocketAddr::V4(flow.src);
                let (io, handle) = dmesh_io_pair(peer, Some(token));
                // Register the IO handle so the driver pumps recv segments into
                // it and picks up the stack's writes.
                if registrar.send(Registration { token, handle }).is_err() {
                    warn!("dmesh driver gone; stopping acceptor");
                    break;
                }

                // BACKEND-mode connections are not inbound flows: the host end
                // provides the service at flow.dst, and the outbound connector
                // (DmeshOrTcp) picks this DmeshIo up from the registry instead
                // of dialing TCP. Nothing to serve here.
                if flow.is_backend {
                    let addr = SocketAddr::V4(flow.dst);
                    info!(session = %token, %addr, "dmesh backend channel ready");
                    sessions.publish_backend(BackendKey::new(addr, token), io);
                    continue;
                }

                let target = DmeshTarget::new(&flow, token);
                let span = debug_span!(
                    "dmesh",
                    session = %Param::<SessionToken>::param(&target),
                    src = %flow.src,
                    orig_dst = %flow.dst
                );
                let svc = outbound.new_service(target);
                let done = done_tx.clone();
                let task = tokio::spawn(
                    async move {
                        match svc.oneshot(io).await {
                            Ok(()) => debug!("dmesh connection closed"),
                            Err(error) => debug!(%error, "dmesh connection failed"),
                        }
                        // Report completion so the acceptor stops owning this
                        // task; a cancelled task never reaches this line.
                        let _ = done.send(token);
                    }
                    .instrument(span),
                );
                sessions.track(token, task);
            }
            DmeshEvent::InfraReady => info!("dmesh infrastructure ready"),
            DmeshEvent::ConnClosed(token) => {
                debug!(session = %token, "dmesh connection closed");
                sessions.close(token);
            }
            DmeshEvent::ConnError(token) => {
                warn!(session = %token, "dmesh connection setup failed");
                sessions.close(token);
            }
            DmeshEvent::Stats {
                elapsed_ms,
                recv_msgs,
                recv_bytes,
                sent_msgs,
                dma_pending,
                dma_dropped,
            } => {
                let secs = elapsed_ms as f64 / 1000.0;
                info!(
                    recv_msgs_per_s = (recv_msgs as f64 / secs) as i64,
                    recv_gbps = format_args!("{:.2}", (recv_bytes as f64 * 8.0) / secs / 1e9),
                    sent_msgs_per_s = (sent_msgs as f64 / secs) as i64,
                    dma_pending,
                    dma_dropped,
                    "dmesh datapath stats"
                )
            }
        }
    }

    // Nothing else owns these tasks: end them before the acceptor returns.
    sessions.drain().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use linkerd_app_core::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::oneshot;

    /// A stack whose connections never finish on their own.
    #[derive(Clone)]
    struct Blocking {
        started: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
    }

    struct Guard(Arc<AtomicUsize>);

    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl NewService<DmeshTarget> for Blocking {
        type Service = svc::BoxTcp<DmeshIo>;

        fn new_service(&self, _: DmeshTarget) -> Self::Service {
            let started = self.started.clone();
            let dropped = self.dropped.clone();
            svc::BoxService::new(svc::mk(move |_io: DmeshIo| {
                started.fetch_add(1, Ordering::SeqCst);
                let guard = Guard(dropped.clone());
                async move {
                    let _guard = guard;
                    futures::future::pending::<()>().await;
                    Ok::<(), Error>(())
                }
            }))
        }
    }

    struct Harness {
        events: mpsc::UnboundedSender<DmeshEvent>,
        registrations: mpsc::UnboundedReceiver<Registration>,
        registry: Arc<Backends>,
        metrics: Arc<SessionMetrics>,
        shutdown: Option<oneshot::Sender<()>>,
        serve: JoinHandle<()>,
    }

    fn flow(port: u16) -> FlowId {
        FlowId {
            src: format!("10.97.0.1:{port}").parse().unwrap(),
            dst: "10.96.0.11:9092".parse().unwrap(),
            workload: "test".to_string(),
            is_backend: false,
        }
    }

    #[test]
    fn target_keeps_the_flow_policy_workload() {
        let flow = flow(1);
        let target = DmeshTarget::new(&flow, SessionToken::new(0, 0, 0));
        assert_eq!(target.policy_workload().as_ref(), "test");
    }

    fn start<N>(outbound: N) -> Harness
    where
        N: NewService<DmeshTarget, Service = svc::BoxTcp<DmeshIo>> + Send + 'static,
    {
        let (events, events_rx) = mpsc::unbounded_channel();
        let (registrar, registrations) = mpsc::unbounded_channel();
        let (shutdown, shutdown_rx) = oneshot::channel();
        let registry = Arc::new(Backends::new());
        let metrics = Arc::new(SessionMetrics::default());
        let serve = tokio::spawn(serve(
            events_rx,
            registrar,
            outbound,
            registry.clone(),
            metrics.clone(),
            async {
                let _ = shutdown_rx.await;
            },
        ));
        Harness {
            events,
            registrations,
            registry,
            metrics,
            shutdown: Some(shutdown),
            serve,
        }
    }

    async fn settle() {
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn close_cancels_the_session_task() {
        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut h = start(Blocking {
            started: started.clone(),
            dropped: dropped.clone(),
        });
        let token = SessionToken::new(0, 0, 0);

        h.events
            .send(DmeshEvent::ConnReady(token, flow(1)))
            .unwrap();
        settle().await;
        assert_eq!(started.load(Ordering::SeqCst), 1);
        assert_eq!(h.metrics.tasks_live.get(), 1);
        assert_eq!(h.registrations.recv().await.unwrap().token, token);

        h.events.send(DmeshEvent::ConnClosed(token)).unwrap();
        settle().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 1, "the task was cancelled");
        assert_eq!(h.metrics.tasks_live.get(), 0);
        assert_eq!(h.metrics.tasks_cancelled.get(), 1);

        h.shutdown.take().unwrap().send(()).unwrap();
        h.serve.await.unwrap();
    }

    #[tokio::test]
    async fn a_reused_slot_is_not_cancelled_by_the_older_close() {
        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut h = start(Blocking {
            started: started.clone(),
            dropped: dropped.clone(),
        });
        let old = SessionToken::new(0, 4, 7);
        let new = SessionToken::new(0, 4, 8);

        h.events.send(DmeshEvent::ConnReady(old, flow(1))).unwrap();
        settle().await;
        h.events.send(DmeshEvent::ConnClosed(old)).unwrap();
        settle().await;
        assert_eq!(dropped.load(Ordering::SeqCst), 1);

        h.events.send(DmeshEvent::ConnReady(new, flow(2))).unwrap();
        settle().await;
        assert_eq!(started.load(Ordering::SeqCst), 2);
        assert_eq!(h.metrics.tasks_live.get(), 1);

        // The closed generation's late close names a session that is gone.
        h.events.send(DmeshEvent::ConnClosed(old)).unwrap();
        settle().await;
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            1,
            "generation 7's close must not cancel generation 8"
        );
        assert_eq!(h.metrics.tasks_live.get(), 1);

        h.shutdown.take().unwrap().send(()).unwrap();
        h.serve.await.unwrap();
        assert_eq!(dropped.load(Ordering::SeqCst), 2, "shutdown ends the rest");
        assert_eq!(h.metrics.tasks_live.get(), 0);
        let _ = h.registrations;
    }

    #[tokio::test]
    async fn a_backend_flow_publishes_and_the_close_withdraws_it() {
        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut h = start(Blocking { started, dropped });
        let token = SessionToken::new(0, 1, 0);
        let addr: SocketAddr = "10.96.0.11:9092".parse().unwrap();
        let backend = FlowId {
            is_backend: true,
            ..flow(3)
        };

        h.events
            .send(DmeshEvent::ConnReady(token, backend))
            .unwrap();
        settle().await;
        assert_eq!(h.registry.sessions_for(&addr), vec![token]);
        assert_eq!(h.metrics.tasks_live.get(), 0, "a backend is not served");

        h.events.send(DmeshEvent::ConnClosed(token)).unwrap();
        settle().await;
        assert!(!h.registry.contains_service(&addr));

        h.shutdown.take().unwrap().send(()).unwrap();
        h.serve.await.unwrap();
        let _ = h.registrations;
    }

    #[tokio::test]
    async fn shutdown_withdraws_a_backend_without_a_service_task() {
        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let mut h = start(Blocking { started, dropped });
        let token = SessionToken::new(0, 3, 0);
        let addr: SocketAddr = "10.96.0.11:9092".parse().unwrap();
        let backend = FlowId {
            is_backend: true,
            ..flow(5)
        };

        h.events
            .send(DmeshEvent::ConnReady(token, backend))
            .unwrap();
        settle().await;
        assert_eq!(h.registry.sessions_for(&addr), vec![token]);

        h.shutdown.take().unwrap().send(()).unwrap();
        h.serve.await.unwrap();
        assert!(h.registry.is_empty(), "shutdown withdrew the backend");
        let _ = h.registrations;
    }

    #[tokio::test]
    async fn a_finished_task_stops_being_owned() {
        struct Done;
        impl NewService<DmeshTarget> for Done {
            type Service = svc::BoxTcp<DmeshIo>;
            fn new_service(&self, _: DmeshTarget) -> Self::Service {
                svc::BoxService::new(svc::mk(|_io: DmeshIo| async { Ok::<(), Error>(()) }))
            }
        }

        let mut h = start(Done);
        let token = SessionToken::new(0, 2, 0);
        h.events
            .send(DmeshEvent::ConnReady(token, flow(4)))
            .unwrap();
        settle().await;
        assert_eq!(h.metrics.tasks_live.get(), 0);
        assert_eq!(
            h.metrics.tasks_cancelled.get(),
            0,
            "a task that ended on its own was not cancelled"
        );

        // A close arriving after the task ended is not an error.
        h.events.send(DmeshEvent::ConnClosed(token)).unwrap();
        settle().await;
        assert_eq!(h.metrics.tasks_cancelled.get(), 0);

        h.shutdown.take().unwrap().send(()).unwrap();
        h.serve.await.unwrap();
        let _ = h.registrations;
    }
}
