use futures::prelude::*;
use linkerd_duplex::Duplex;
use linkerd_error::{Error, Result};
use linkerd_io::{DmeshSession, DmeshSessionId};
use linkerd_stack::layer;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tower::Service;

#[derive(Clone, Debug)]
pub struct Forward<C> {
    connect: C,
}

/// Like [`Forward`], but hands the server-side connection's DMA session to the
/// connect stack, so a connector that pairs sessions with backend channels can
/// resolve the pairing from the connection itself. Socket-origin connections
/// carry no session and connect exactly as [`Forward`] does.
#[derive(Clone, Debug)]
pub struct ForwardSession<C> {
    connect: C,
}

impl<C> Forward<C> {
    fn new(connect: C) -> Self {
        Self { connect }
    }

    pub fn layer() -> impl layer::Layer<C, Service = Self> + Copy {
        layer::mk(Self::new)
    }
}

impl<C, I> Service<I> for Forward<C>
where
    I: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    C: tower::Service<()> + Send + 'static,
    C::Error: Into<Error>,
    C::Future: Send + 'static,
    C::Response: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Response = ();
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), self::Error>> {
        self.connect.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, src_io: I) -> Self::Future {
        Box::pin(
            self.connect
                .call(())
                .err_into::<Error>()
                .and_then(|dst_io| Duplex::new(src_io, dst_io).err_into::<Error>()),
        )
    }
}

// === impl ForwardSession ===

impl<C> ForwardSession<C> {
    fn new(connect: C) -> Self {
        Self { connect }
    }

    pub fn layer() -> impl layer::Layer<C, Service = Self> + Copy {
        layer::mk(Self::new)
    }
}

impl<C, I> Service<I> for ForwardSession<C>
where
    I: AsyncRead + AsyncWrite + DmeshSession + Send + Unpin + 'static,
    C: tower::Service<Option<DmeshSessionId>> + Send + 'static,
    C::Error: Into<Error>,
    C::Future: Send + 'static,
    C::Response: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    type Response = ();
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), self::Error>> {
        self.connect.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, src_io: I) -> Self::Future {
        let session = src_io.dmesh_session();
        Box::pin(
            self.connect
                .call(session)
                .err_into::<Error>()
                .and_then(|dst_io| Duplex::new(src_io, dst_io).err_into::<Error>()),
        )
    }
}
