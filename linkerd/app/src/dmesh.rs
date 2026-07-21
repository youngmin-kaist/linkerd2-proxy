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

use std::future::Future;
use std::net::SocketAddr;

use linkerd_app_core::{
    svc::{self, NewService, Param, ServiceExt},
    transport::addrs::{AddrPair, ClientAddr, OrigDstAddr, Remote, ServerAddr},
};
use dmesh_doca::{dmesh_io_pair, DmeshEvent, DmeshIo, FlowId, Registrar};
use tokio::sync::mpsc;
use tracing::{debug, debug_span, info, warn, Instrument};

/// Server-side target for a DMA-received connection. Mirrors the `Param` impls
/// of `linkerd_proxy_transport::orig_dst::Addrs` that the outbound stack reads.
#[derive(Clone, Debug)]
pub struct DmeshTarget {
    orig_dst: OrigDstAddr,
    client: Remote<ClientAddr>,
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

impl From<&FlowId> for DmeshTarget {
    fn from(flow: &FlowId) -> Self {
        Self {
            orig_dst: OrigDstAddr(SocketAddr::V4(flow.dst)),
            client: Remote(ClientAddr(SocketAddr::V4(flow.src))),
        }
    }
}

/// Serve DMA connections through the outbound stack until the event stream ends
/// or shutdown is signalled. `outbound` is an `ArcNewTcp<DmeshTarget, DmeshIo>`
/// (built via `Outbound::mk` with `I = DmeshIo`).
pub async fn serve<N>(
    mut events: mpsc::UnboundedReceiver<DmeshEvent>,
    registrar: Registrar,
    outbound: N,
    shutdown: impl Future,
) where
    N: NewService<DmeshTarget, Service = svc::BoxTcp<DmeshIo>> + Send + 'static,
{
    tokio::pin!(shutdown);
    loop {
        let ev = tokio::select! {
            _ = &mut shutdown => return,
            ev = events.recv() => match ev {
                Some(ev) => ev,
                None => return,
            },
        };

        match ev {
            DmeshEvent::ConnReady(slot, flow) => {
                let peer = SocketAddr::V4(flow.src);
                let (io, handle) = dmesh_io_pair(peer);
                // Register the IO handle so the driver pumps recv segments into
                // it and picks up the stack's writes.
                if registrar.send((slot, handle)).is_err() {
                    warn!("dmesh driver gone; stopping acceptor");
                    return;
                }

                let target = DmeshTarget::from(&flow);
                let span = debug_span!("dmesh", slot, src = %flow.src, orig_dst = %flow.dst);
                let svc = outbound.new_service(target);
                tokio::spawn(
                    async move {
                        match svc.oneshot(io).await {
                            Ok(()) => debug!("dmesh connection closed"),
                            Err(error) => debug!(%error, "dmesh connection failed"),
                        }
                    }
                    .instrument(span),
                );
            }
            DmeshEvent::InfraReady => info!("dmesh infrastructure ready"),
            DmeshEvent::ConnClosed(slot) => debug!(slot, "dmesh connection closed"),
            DmeshEvent::ConnError(slot) => warn!(slot, "dmesh connection setup failed"),
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
