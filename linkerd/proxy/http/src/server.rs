use crate::{client_handle::SetClientHandle, h2, BoxBody, ClientHandle, Variant};
use hyper_util::rt::tokio::TokioExecutor;
use linkerd_error::Error;
use linkerd_http_box::BoxRequest;
use linkerd_io::{self as io, PeerAddr};
use linkerd_stack::{layer, ExtractParam, NewService};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tower::Service;
use tracing::{debug, Instrument};

#[cfg(test)]
mod tests;

/// Configures HTTP server behavior.
#[derive(Clone, Debug)]
pub struct Params {
    pub version: Variant,
    pub http2: h2::ServerParams,
    pub drain: drain::Watch,
}

// A stack that builds HTTP servers.
#[derive(Clone, Debug)]
pub struct NewServeHttp<X, N> {
    inner: N,
    params: X,
}

/// Serves HTTP connections with an inner service.
#[derive(Clone, Debug)]
pub struct ServeHttp<N> {
    version: Variant,
    http1: hyper::server::conn::http1::Builder,
    http2: hyper::server::conn::http2::Builder<TokioExecutor>,
    /// Raw params, kept so the nghttp2 engine can consume them directly.
    h2_params: h2::ServerParams,
    inner: N,
    drain: drain::Watch,
}

// === impl NewServeHttp ===

impl<X: Clone, N> NewServeHttp<X, N> {
    pub fn layer(params: X) -> impl layer::Layer<N, Service = Self> + Clone {
        layer::mk(move |inner| Self::new(params.clone(), inner))
    }

    /// Creates a new `ServeHttp`.
    fn new(params: X, inner: N) -> Self {
        Self { inner, params }
    }
}

impl<T, X, N> NewService<T> for NewServeHttp<X, N>
where
    X: ExtractParam<Params, T>,
    N: NewService<T> + Clone,
{
    type Service = ServeHttp<N::Service>;

    fn new_service(&self, target: T) -> Self::Service {
        let Params {
            version,
            http2: h2,
            drain,
        } = self.params.extract_param(&target);
        let h2_params = h2.clone();
        let h2::ServerParams {
            keep_alive,
            flow_control,
            max_concurrent_streams,
            max_frame_size,
            max_header_list_size,
            max_send_buf_size,
            max_pending_accept_reset_streams,
        } = h2;

        let mut http2 = hyper::server::conn::http2::Builder::new(TokioExecutor::new());
        http2.timer(hyper_util::rt::TokioTimer::new());
        match flow_control {
            None => {}
            Some(h2::FlowControl::Adaptive) => {
                http2.adaptive_window(true);
            }
            Some(h2::FlowControl::Fixed {
                initial_stream_window_size,
                initial_connection_window_size,
            }) => {
                http2
                    .initial_stream_window_size(initial_stream_window_size)
                    .initial_connection_window_size(initial_connection_window_size);
            }
        }

        // Configure HTTP/2 PING frames
        if let Some(h2::KeepAlive { timeout, interval }) = keep_alive {
            http2
                .keep_alive_timeout(timeout)
                .keep_alive_interval(interval);
        }

        http2
            .max_concurrent_streams(max_concurrent_streams)
            .max_frame_size(max_frame_size)
            .max_pending_accept_reset_streams(max_pending_accept_reset_streams);
        if let Some(sz) = max_header_list_size {
            http2.max_header_list_size(sz);
        }
        if let Some(sz) = max_send_buf_size {
            http2.max_send_buf_size(sz);
        }

        let mut http1 = hyper::server::conn::http1::Builder::new();
        http1
            .header_read_timeout(None)
            .timer(hyper_util::rt::TokioTimer::new());

        debug!(?version, "Creating HTTP service");
        let inner = self.inner.new_service(target);
        ServeHttp {
            inner,
            version,
            drain,
            http1,
            http2,
            h2_params,
        }
    }
}

/// Engine selection, read once at startup.
fn use_nghttp2() -> bool {
    use std::sync::OnceLock;
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        let on = std::env::var_os("DMESH_NGHTTP2").is_some();
        if on {
            tracing::info!("HTTP/2 server termination: nghttp2 engine");
        }
        on
    })
}

// === impl ServeHttp ===

impl<I, N, S> Service<I> for ServeHttp<N>
where
    I: io::AsyncRead + io::AsyncWrite + PeerAddr + Send + Unpin + 'static,
    N: NewService<ClientHandle, Service = S> + Send + 'static,
    S: Service<http::Request<BoxBody>, Response = http::Response<BoxBody>, Error = Error>
        + Unpin
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = ();
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<(), Error>> + Send + 'static>>;

    fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, io: I) -> Self::Future {
        let version = self.version;
        let drain = self.drain.clone();
        let http1 = self.http1.clone();
        let http2 = self.http2.clone();
        let h2_params = self.h2_params.clone();

        let res = io.peer_addr().map(|pa| {
            let (handle, closed) = ClientHandle::new(pa);
            let svc = self.inner.new_service(handle.clone());
            let svc = SetClientHandle::new(handle, svc);
            (svc, closed)
        });

        Box::pin(
            async move {
                let (svc, closed) = res?;
                debug!(?version, "Handling as HTTP");
                match version {
                    Variant::Http1 => {
                        // Enable support for HTTP upgrades (CONNECT and websockets).
                        let svc = linkerd_http_upgrade::upgrade::Service::new(
                            BoxRequest::new(svc),
                            drain.clone(),
                        );
                        let svc = hyper_util::service::TowerToHyperService::new(svc);
                        let io = hyper_util::rt::TokioIo::new(io);
                        let mut conn = http1.serve_connection(io, svc).with_upgrades();

                        tokio::select! {
                            res = &mut conn => {
                                debug!(?res, "The client is shutting down the connection");
                                res?
                            }
                            shutdown = drain.signaled() => {
                                debug!("The process is shutting down the connection");
                                Pin::new(&mut conn).graceful_shutdown();
                                shutdown.release_after(conn).await?;
                            }
                            () = closed => {
                                debug!("The stack is tearing down the connection");
                                Pin::new(&mut conn).graceful_shutdown();
                                conn.await?;
                            }
                        }
                    }

                    // Both engines are compiled in; DMESH_NGHTTP2=1 selects the
                    // nghttp2 one at startup. Same binary either way, so an A/B
                    // measurement has no build-difference confound.
                    Variant::H2 if use_nghttp2() => {
                        // The nghttp2 engine hands the stack `Request<BoxBody>`
                        // directly, so no BoxRequest/TowerToHyperService/TokioIo
                        // adapters are needed. Drain is delivered to the engine
                        // so it can emit GOAWAY and finish in-flight streams.
                        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
                        let drain = drain.clone();
                        tokio::spawn(async move {
                            tokio::select! {
                                _ = drain.signaled() => {}
                                () = closed => {}
                            }
                            let _ = tx.send(());
                        });
                        linkerd_http_nghttp2::server::serve(
                            io,
                            svc,
                            h2_params,
                            async move {
                                let _ = rx.await;
                            },
                        )
                        .await
                        .map_err(Error::from)?;
                    }

                    Variant::H2 => {
                        let svc =
                            hyper_util::service::TowerToHyperService::new(BoxRequest::new(svc));
                        let io = hyper_util::rt::TokioIo::new(io);
                        let mut conn = http2.serve_connection(io, svc);

                        tokio::select! {
                            res = &mut conn => {
                                debug!(?res, "The client is shutting down the connection");
                                res?
                            }
                            shutdown = drain.signaled() => {
                                debug!("The process is shutting down the connection");
                                Pin::new(&mut conn).graceful_shutdown();
                                shutdown.release_after(conn).await?;
                            }
                            () = closed => {
                                debug!("The stack is tearing down the connection");
                                Pin::new(&mut conn).graceful_shutdown();
                                conn.await?;
                            }
                        }
                    }
                }
                Ok(())
            }
            .instrument(tracing::debug_span!("http").or_current()),
        )
    }
}
