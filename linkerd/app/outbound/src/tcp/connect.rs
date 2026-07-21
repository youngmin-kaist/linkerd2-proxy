use crate::Outbound;
use futures::future;
use linkerd_app_core::{
    io, svc, tls,
    transport::{addrs::*, ConnectTcp},
};
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub struct Connect {
    addr: Remote<ServerAddr>,
    tls: tls::ConditionalClientTls,
}

/// Prevents outbound connections on the loopback interface, unless the
/// `allow-loopback` feature is enabled.
#[derive(Clone, Debug)]
pub struct PreventLoopback<S>(S);

/// Physical connector that reaches DMA-provided backends through their
/// registered dmesh channel and everything else via TCP. A backend host
/// process (DMESH_BACKEND_CONNECT) publishes one long-lived DmeshIo per
/// service address into `dmesh_doca::backend`; the h2 client caches the one
/// connection it gets, which matches the one-channel-per-service model.
#[cfg(feature = "doca")]
#[derive(Clone, Debug)]
pub struct DmeshOrTcp(PreventLoopback<ConnectTcp>);

// === impl Outbound ===

#[cfg(not(feature = "doca"))]
impl Outbound<()> {
    pub fn to_tcp_connect(&self) -> Outbound<PreventLoopback<ConnectTcp>> {
        let connect = PreventLoopback(ConnectTcp::new(
            self.config.proxy.connect.keepalive,
            self.config.proxy.connect.user_timeout,
        ));
        self.clone().with_stack(connect)
    }
}

#[cfg(feature = "doca")]
impl Outbound<()> {
    pub fn to_tcp_connect(&self) -> Outbound<DmeshOrTcp> {
        let connect = DmeshOrTcp(PreventLoopback(ConnectTcp::new(
            self.config.proxy.connect.keepalive,
            self.config.proxy.connect.user_timeout,
        )));
        self.clone().with_stack(connect)
    }
}

#[cfg(feature = "doca")]
impl<T> svc::Service<T> for DmeshOrTcp
where
    T: svc::Param<Remote<ServerAddr>>,
{
    type Response = (
        io::EitherIo<io::ScopedIo<tokio::net::TcpStream>, dmesh_doca::DmeshIo>,
        Local<ClientAddr>,
    );
    type Error = io::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = io::Result<Self::Response>> + Send + Sync + 'static>,
    >;

    #[inline]
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Both inner paths (ConnectTcp, registry lookup) are always ready.
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, ep: T) -> Self::Future {
        let Remote(ServerAddr(addr)) = ep.param();
        if let Some(dio) = dmesh_doca::backend::take(&addr) {
            tracing::info!(server.addr = %addr, "Connecting via dmesh DMA backend channel");
            let local = Local(ClientAddr(std::net::SocketAddr::from(([127, 0, 0, 1], 0))));
            return Box::pin(future::ready(Ok((io::EitherIo::Right(dio), local))));
        }
        let fut = self.0.call(ep);
        Box::pin(async move {
            let (tcp, local) = fut.await?;
            Ok((io::EitherIo::Left(tcp), local))
        })
    }
}

// === impl PreventLoopback ===

impl<S> PreventLoopback<S> {
    #[cfg(not(feature = "allow-loopback"))]
    fn check_loopback(Remote(ServerAddr(addr)): Remote<ServerAddr>) -> io::Result<()> {
        if addr.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                "Outbound proxy cannot initiate connections on the loopback interface",
            ));
        }

        Ok(())
    }

    #[cfg(feature = "allow-loopback")]
    // the Result is necessary to have the same type signature regardless of
    // whether or not the `allow-loopback` feature is enabled...
    fn check_loopback(_: Remote<ServerAddr>) -> io::Result<()> {
        Ok(())
    }
}

impl<T, S> svc::Service<T> for PreventLoopback<S>
where
    T: svc::Param<Remote<ServerAddr>>,
    S: svc::Service<T, Error = io::Error>,
{
    type Response = S::Response;
    type Error = io::Error;
    type Future = future::Either<S::Future, future::Ready<io::Result<S::Response>>>;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.0.poll_ready(cx)
    }

    fn call(&mut self, ep: T) -> Self::Future {
        if let Err(e) = Self::check_loopback(ep.param()) {
            return future::Either::Right(future::err(e));
        }

        future::Either::Left(self.0.call(ep))
    }
}

// === impl Connect ===

impl Connect {
    pub fn new(addr: Remote<ServerAddr>, tls: tls::ConditionalClientTls) -> Self {
        Self { addr, tls }
    }
}

impl svc::Param<Remote<ServerAddr>> for Connect {
    fn param(&self) -> Remote<ServerAddr> {
        self.addr
    }
}

impl svc::Param<tls::ConditionalClientTls> for Connect {
    fn param(&self) -> tls::ConditionalClientTls {
        self.tls.clone()
    }
}

#[cfg(test)]
impl Connect {
    pub fn addr(&self) -> &Remote<ServerAddr> {
        &self.addr
    }

    pub fn tls(&self) -> &tls::ConditionalClientTls {
        &self.tls
    }
}
