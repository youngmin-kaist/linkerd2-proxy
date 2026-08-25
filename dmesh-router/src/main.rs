//! `dmesh-router` — a minimal HTTP/2 router on the DPUMesh DMA datapath.
//!
//! It occupies the same position as the DMA-enabled `linkerd2-proxy`: it runs
//! on the DPU, accepts connections that arrive over PCIe DMA from the host
//! (`DMESH_BRIDGE_PORT`, driven by h2load), terminates HTTP/2 on them, and
//! forwards each request to a backend the host provides over another DMA
//! channel (`DMESH_BACKEND_CONNECT`, spliced to nginx). Unlike the proxy it
//! runs no tower stack: no protocol detection, discovery, load balancing,
//! policy, mTLS or telemetry — just hyper's h2 server, a routing-table lookup,
//! and hyper's client. That makes it the floor against which the proxy's L7
//! cost is measured on an identical datapath.
//!
//! Startup mirrors `linkerd2-proxy/src/main.rs`: the DOCA device is opened and
//! the comch server started before the runtime exists, then the driver and the
//! acceptor are spawned onto it.

use std::sync::Arc;

use dmesh_doca::{DmeshDoca, Driver};
use tokio::sync::mpsc;
use tracing::{info, warn};

mod accept;
mod backend;
mod config;
mod proxy;
#[cfg(test)]
mod tests;

use config::Config;

#[cfg(all(
    target_os = "linux",
    any(target_arch = "x86_64", target_arch = "aarch64"),
    target_env = "gnu"
))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_env("DMESH_ROUTER_LOG")
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = match Config::from_env() {
        Ok(cfg) => Arc::new(cfg),
        Err(error) => {
            eprintln!("Invalid configuration: {error}");
            std::process::exit(64); // EX_USAGE
        }
    };

    // Stage 1 (pre-runtime): probe DOCA and start the comch server the host
    // shim connects to. The datapath is driven asynchronously in stage 2.
    match dmesh_doca::initialize() {
        Ok(report) => info!("{}", report.log_summary()),
        Err(error) => {
            eprintln!("DOCA initialization failure: {error}");
            std::process::exit(1);
        }
    }
    let doca = match DmeshDoca::initialize(&cfg.dev_pci, &cfg.rep_pci, &cfg.server_name) {
        Ok(doca) => {
            info!(
                server = %cfg.server_name, dev = %cfg.dev_pci, rep = %cfg.rep_pci,
                backend_proto = ?cfg.backend_proto,
                "dmesh comch server started"
            );
            doca
        }
        Err(error) => {
            eprintln!("DOCA comch initialization failure: {error}");
            std::process::exit(1);
        }
    };

    runtime(cfg.cores).block_on(async move {
        // Stage 2: the driver owns the DOCA progress engines and emits
        // connection events; the acceptor turns them into served connections.
        let (tx, rx) = mpsc::unbounded_channel();
        let (driver, registrar) = Driver::new(doca, tx);
        tokio::spawn(async move {
            match driver.run().await {
                Ok(()) => warn!("dmesh driver exited"),
                Err(error) => warn!(%error, "dmesh driver failed"),
            }
        });

        tokio::select! {
            _ = accept::serve(rx, registrar, cfg) => warn!("dmesh acceptor exited"),
            _ = tokio::signal::ctrl_c() => info!("shutting down"),
        }
    });
}

/// One core (the benchmark configuration) runs on the current-thread scheduler:
/// the driver task and the connection tasks it feeds then never bounce between
/// threads. More cores get the multi-threaded scheduler.
fn runtime(cores: usize) -> tokio::runtime::Runtime {
    let mut builder = if cores <= 1 {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.worker_threads(cores);
        b
    };
    builder
        .enable_all()
        .build()
        .expect("failed to build tokio runtime")
}
