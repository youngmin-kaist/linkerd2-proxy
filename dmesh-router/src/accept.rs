//! Turns driver events into served connections.
//!
//! The DMA analogue of a TCP accept loop, and the direct counterpart of
//! `linkerd_app::dmesh::serve` — except that a ready client connection is
//! handed to `hyper::server::conn::http2` instead of to the outbound tower
//! stack, and a ready backend connection becomes a hyper client instead of an
//! entry in the connector's registry.

use std::sync::Arc;

use dmesh_doca::{dmesh_io_pair, DmeshEvent, Registrar};
use hyper::server::conn::http2;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::sync::mpsc;
use tracing::{debug, debug_span, info, warn, Instrument};

use crate::backend;
use crate::config::Config;
use crate::proxy::{self, Ctx};

/// Terminate HTTP/2 on a client channel and route each stream. Generic over
/// the transport so the same path can be driven over an in-memory pipe.
pub fn serve_client<I>(io: I, ctx: Arc<Ctx>, span: tracing::Span)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let max_streams = ctx.cfg.max_streams;
    let service = service_fn(move |req| proxy::handle(req, ctx.clone()));
    let conn = http2::Builder::new(TokioExecutor::new())
        .max_concurrent_streams(max_streams)
        .serve_connection(TokioIo::new(io), service);
    tokio::spawn(
        async move {
            match conn.await {
                Ok(()) => debug!("dmesh connection closed"),
                Err(error) => debug!(%error, "dmesh connection failed"),
            }
        }
        .instrument(span),
    );
}

pub async fn serve(
    mut events: mpsc::UnboundedReceiver<DmeshEvent>,
    registrar: Registrar,
    cfg: Arc<Config>,
) {
    while let Some(event) = events.recv().await {
        match event {
            DmeshEvent::ConnReady(slot, flow) => {
                let peer = std::net::SocketAddr::V4(flow.src);
                let (io, handle) = dmesh_io_pair(peer);
                // The driver pumps received DMA segments into this handle and
                // publishes what the stack writes; without the registration the
                // connection would never carry bytes.
                if registrar.send((slot, handle)).is_err() {
                    warn!("dmesh driver gone; stopping acceptor");
                    return;
                }

                let dst = std::net::SocketAddr::V4(flow.dst);
                if flow.is_backend {
                    // The host end provides the service at `dst`; hand the
                    // channel to the client registry rather than serving it.
                    let proto = cfg.backend_proto;
                    tokio::spawn(async move { backend::register(dst, io, proto).await });
                    continue;
                }

                info!(slot, src = %flow.src, %dst, workload = %flow.workload,
                      "dmesh client connection ready");
                let ctx = Arc::new(Ctx {
                    cfg: cfg.clone(),
                    dst,
                    slot,
                });
                let span = debug_span!("dmesh", slot, src = %flow.src, dst = %flow.dst);
                serve_client(io, ctx, span);
            }
            DmeshEvent::InfraReady => info!("dmesh infrastructure ready"),
            DmeshEvent::ConnClosed(slot) => debug!(slot, "dmesh connection closed"),
            DmeshEvent::ConnError(slot) => warn!(slot, "dmesh connection setup failed"),
            // Same shape as the proxy's stats line, so benchmark output from
            // the two data planes can be compared directly.
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
}
