//! The main entrypoint for the proxy.

#![deny(rust_2018_idioms, clippy::disallowed_methods, clippy::disallowed_types)]
#![forbid(unsafe_code)]
#![recursion_limit = "256"]

use linkerd_app::{trace, BindTcp, Config, BUILD_INFO};
use linkerd_signal as signal;
use tokio::{sync::mpsc, time};
use tracing::{debug, info, warn};

#[cfg(all(target_os = "linux", target_arch = "x86_64", target_env = "gnu"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod rt;

const EX_USAGE: i32 = 64;

fn main() {
    let trace = match trace::Settings::from_env().init() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("Invalid logging configuration: {e}");
            std::process::exit(EX_USAGE);
        }
    };

    info!(
        "{profile} {version} ({sha}) by {vendor} on {date}",
        date = BUILD_INFO.date,
        sha = BUILD_INFO.git_sha,
        version = BUILD_INFO.version,
        profile = BUILD_INFO.profile,
        vendor = BUILD_INFO.vendor,
    );

    linkerd_rustls::install_default_provider();

    // Stage 1 (synchronous, pre-runtime): open the DOCA device and start the
    // dmesh comch server. The async driver is spawned inside the runtime below.
    #[cfg(feature = "doca")]
    let dmesh_doca = {
        match dmesh_doca::initialize() {
            Ok(report) => info!("{}", report.log_summary()),
            Err(error) => {
                eprintln!("DOCA initialization failure: {error}");
                std::process::exit(1);
            }
        }
        let dev_pci_addr = match std::env::var("LINKERD2_PROXY_DOCA_DEV_PCI_ADDR") {
            Ok(addr) => addr,
            Err(error) => {
                eprintln!(
                    "Invalid DOCA configuration: LINKERD2_PROXY_DOCA_DEV_PCI_ADDR: {error}"
                );
                std::process::exit(EX_USAGE);
            }
        };
        let rep_pci_addr = match std::env::var("LINKERD2_PROXY_DOCA_REP_PCI_ADDR") {
            Ok(addr) => addr,
            Err(error) => {
                eprintln!(
                    "Invalid DOCA configuration: LINKERD2_PROXY_DOCA_REP_PCI_ADDR: {error}"
                );
                std::process::exit(EX_USAGE);
            }
        };
        let server_name = std::env::var("LINKERD2_PROXY_DOCA_SERVER_NAME")
            .unwrap_or_else(|_| "DPUMesh0".to_string());
        match dmesh_doca::DmeshDoca::initialize(&dev_pci_addr, &rep_pci_addr, &server_name) {
            Ok(doca_handle) => {
                info!(server = %server_name, "dmesh comch server started");
                doca_handle
            }
            Err(error) => {
                eprintln!("DOCA comch initialization failure: {error}");
                std::process::exit(1);
            }
        }
    };

    let mut metrics = linkerd_metrics::prom::Registry::default();

    // Load configuration from the environment without binding ports.
    let config = match Config::try_from_env() {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Invalid configuration: {e}");
            std::process::exit(EX_USAGE);
        }
    };

    // Builds a runtime with the appropriate number of cores:
    // `LINKERD2_PROXY_CORES` env or the number of available CPUs (as provided
    // by cgroups, when possible).
    rt::build().block_on(async move {
        // Spawn a task to run in the background, exporting runtime metrics at a regular interval.
        rt::spawn_metrics_exporter(&mut metrics);

        // Stage 2: drive the dmesh control/data paths event-driven (AsyncFd on
        // the DOCA progress-engine fds). Host connections surface as events;
        // each ready connection already has a DPA thread and DMA engine bound
        // by the C state machine.
        #[cfg(feature = "doca")]
        {
            let (dmesh_tx, mut dmesh_rx) = mpsc::unbounded_channel();
            let driver = dmesh_doca::Driver::new(dmesh_doca, dmesh_tx);
            tokio::spawn(async move {
                match driver.run().await {
                    Ok(()) => warn!("dmesh driver exited"),
                    Err(error) => warn!(%error, "dmesh driver failed"),
                }
            });
            tokio::spawn(async move {
                while let Some(ev) = dmesh_rx.recv().await {
                    match ev {
                        dmesh_doca::DmeshEvent::InfraReady => {
                            info!("dmesh infrastructure ready (DPA pool + consumer PE)")
                        }
                        dmesh_doca::DmeshEvent::ConnReady(slot, flow) => info!(
                            slot,
                            src = %flow.src,
                            orig_dst = %flow.dst,
                            workload = %flow.workload,
                            "dmesh connection ready (DPA thread assigned)"
                        ),
                        dmesh_doca::DmeshEvent::ConnClosed(slot) => {
                            info!(slot, "dmesh connection closed")
                        }
                        dmesh_doca::DmeshEvent::ConnError(slot) => {
                            warn!(slot, "dmesh connection setup failed")
                        }
                    }
                }
            });
        }

        let (shutdown_tx, mut shutdown_rx) = mpsc::unbounded_channel();
        let shutdown_grace_period = config.shutdown_grace_period;

        let bind_in = BindTcp::with_orig_dst();
        let bind_out = BindTcp::dual_with_orig_dst();
        let app = match config
            .build(
                bind_in,
                bind_out,
                BindTcp::default(),
                shutdown_tx,
                trace,
                metrics,
            )
            .await
        {
            Ok(app) => app,
            Err(e) => {
                eprintln!("Initialization failure: {e}");
                std::process::exit(1);
            }
        };

        info!("Admin interface on {}", app.admin_addr());
        info!("Inbound interface on {}", app.inbound_addr());
        info!("Outbound interface on {}", app.outbound_addr());
        if let Some(addr) = app.outbound_addr_additional() {
            info!("Outbound interface on {addr}");
        }

        match app.tap_addr() {
            None => info!("Tap DISABLED"),
            Some(addr) => info!("Tap interface on {}", addr),
        }

        // TODO distinguish ServerName and Identity.
        info!("SNI is {}", app.local_server_name());
        info!("Local identity is {}", app.local_tls_id());

        let dst_addr = app.dst_addr();
        match dst_addr.identity.value() {
            None => info!("Destinations resolved via {}", dst_addr.addr),
            Some(tls) => info!(
                "Destinations resolved via {} ({})",
                dst_addr.addr, tls.server_id
            ),
        }

        if let Some(tracing) = app.tracing_addr() {
            match tracing.identity.value() {
                None => info!("Tracing collector at {}", tracing.addr),
                Some(tls) => {
                    info!("Tracing collector at {} ({})", tracing.addr, tls.server_id)
                }
            }
        }

        let drain = app.spawn();
        tokio::select! {
            _ = signal::shutdown() => {
                info!("Received shutdown signal");
            }
            _ = shutdown_rx.recv() => {
                info!("Received shutdown via admin interface");
            }
        }
        match time::timeout(shutdown_grace_period, drain.drain()).await {
            Ok(()) => debug!("Shutdown completed gracefully"),
            Err(_) => warn!(
                "Graceful shutdown did not complete in {shutdown_grace_period:?}, terminating now"
            ),
        }
    });
}
