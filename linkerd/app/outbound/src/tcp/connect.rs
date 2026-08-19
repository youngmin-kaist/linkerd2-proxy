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
/// registered dmesh channel and everything else via TCP.
///
/// A DPUmesh session publishes the endpoint that provides its service into the
/// worker's `Backends`; a DMesh-only outbound stack binds this connector to the
/// frontend's `SessionToken`, so it takes exactly that session's channel. The
/// normal socket-listener stack leaves `session` unset and always dials TCP.
///
/// A service DPUmesh has provided is never dialed over TCP: a stream that took
/// that path would run without the policy the mesh applied, so a missing
/// channel fails the connection and is counted.
#[cfg(feature = "doca")]
#[derive(Clone, Debug)]
pub struct DmeshOrTcp {
    tcp: PreventLoopback<ConnectTcp>,
    dmesh: Option<dmesh_doca::Dmesh>,
    session: Option<dmesh_doca::SessionToken>,
}

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
        let connect = DmeshOrTcp {
            tcp: PreventLoopback(ConnectTcp::new(
                self.config.proxy.connect.keepalive,
                self.config.proxy.connect.user_timeout,
            )),
            dmesh: self.config.dmesh.clone(),
            session: self.config.dmesh_session,
        };
        self.clone().with_stack(connect)
    }
}

/// The channel a DMesh-provided service is reached through.
///
/// `Ok(None)` means the address is not one DPUmesh provides, so the caller
/// dials it over TCP as any stock proxy would.
#[cfg(feature = "doca")]
fn dmesh_channel(
    dmesh: Option<&dmesh_doca::Dmesh>,
    session: Option<dmesh_doca::SessionToken>,
    addr: std::net::SocketAddr,
) -> io::Result<Option<dmesh_doca::DmeshIo>> {
    let (Some(dmesh), Some(session)) = (dmesh, session) else {
        return Ok(None);
    };
    match dmesh.backends.take_session(session, addr) {
        Ok(io) => Ok(Some(io)),
        Err(error) => {
            dmesh.metrics.backend_take_errors.inc();
            if error == dmesh_doca::TakeError::TargetMismatch {
                dmesh.metrics.backend_target_mismatches.inc();
            }
            tracing::warn!(
                server.addr = %addr,
                %session,
                %error,
                "No dmesh backend channel for this session; refusing to dial it over TCP"
            );
            Err(io::Error::new(
                io::ErrorKind::NotConnected,
                format!("dmesh backend channel for {addr} session {session}: {error}"),
            ))
        }
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
        match dmesh_channel(self.dmesh.as_ref(), self.session, addr) {
            Ok(Some(dio)) => {
                let local = Local(ClientAddr(std::net::SocketAddr::from(([127, 0, 0, 1], 0))));
                return Box::pin(future::ready(Ok((io::EitherIo::Right(dio), local))));
            }
            Ok(None) => {}
            Err(error) => return Box::pin(future::ready(Err(error))),
        }
        let fut = self.tcp.call(ep);
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

#[cfg(all(test, feature = "doca"))]
mod dmesh_tests {
    use super::*;
    use dmesh_doca::{BackendKey, Backends, Dmesh, SessionMetrics, SessionToken};
    use std::net::SocketAddr;
    use std::sync::Arc;

    fn dmesh() -> Dmesh {
        Dmesh {
            backends: Arc::new(Backends::new()),
            metrics: Arc::new(SessionMetrics::default()),
        }
    }

    fn publish(d: &Dmesh, addr: SocketAddr, session: SessionToken) -> BackendKey {
        let key = BackendKey::new(addr, session);
        let (io, _handle) = dmesh_doca::dmesh_io_pair(addr);
        d.backends.publish(key, io).unwrap();
        key
    }

    #[test]
    fn two_same_service_sessions_take_their_own_channels() {
        let d = dmesh();
        let addr: SocketAddr = "10.96.0.11:9092".parse().unwrap();
        let first_session = SessionToken::new(0, 0, 0);
        let second_session = SessionToken::new(0, 1, 0);
        let first = publish(&d, addr, first_session);
        let second = publish(&d, addr, second_session);
        assert_ne!(first, second, "distinct backend keys");
        assert_eq!(d.backends.sessions_for(&addr).len(), 2);

        dmesh_channel(Some(&d), Some(second_session), addr)
            .unwrap()
            .unwrap();
        assert_eq!(
            d.backends.take_session(second_session, addr).unwrap_err(),
            dmesh_doca::TakeError::AlreadyTaken
        );
        dmesh_channel(Some(&d), Some(first_session), addr)
            .unwrap()
            .unwrap();
        assert_eq!(
            d.backends.take_session(first_session, addr).unwrap_err(),
            dmesh_doca::TakeError::AlreadyTaken
        );
    }

    #[test]
    fn a_meshed_service_is_not_dialed_over_tcp() {
        let d = dmesh();
        let addr: SocketAddr = "10.96.0.12:9092".parse().unwrap();
        let key = publish(&d, addr, SessionToken::new(0, 0, 0));

        dmesh_channel(Some(&d), Some(key.session), addr)
            .unwrap()
            .unwrap();
        // Taken, then withdrawn by the close: both are refusals, not fallbacks.
        let error = dmesh_channel(Some(&d), Some(key.session), addr).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotConnected);
        d.backends.remove(&key);
        assert!(dmesh_channel(Some(&d), Some(key.session), addr).is_err());
        assert_eq!(d.metrics.backend_take_errors.get(), 2);
    }

    #[test]
    fn an_unmeshed_address_falls_through_to_tcp() {
        let d = dmesh();
        let addr: SocketAddr = "10.96.0.13:9092".parse().unwrap();
        let session = SessionToken::new(0, 0, 0);
        assert!(dmesh_channel(None, Some(session), addr).unwrap().is_none());
        assert!(dmesh_channel(Some(&d), None, addr).unwrap().is_none());
        assert_eq!(d.metrics.backend_take_errors.get(), 0);
    }

    #[test]
    fn a_cross_service_target_has_a_stable_metric() {
        let d = dmesh();
        let service: SocketAddr = "10.96.0.11:9092".parse().unwrap();
        let selected: SocketAddr = "10.244.0.11:9092".parse().unwrap();
        let other: SocketAddr = "10.244.0.99:9092".parse().unwrap();
        let session = SessionToken::new(0, 0, 0);
        let key = BackendKey::new(service, session);
        let (io, _handle) = dmesh_doca::dmesh_io_pair(service);
        d.backends.publish(key, io).unwrap();
        d.backends.place_targets([
            (service, service),
            (selected, service),
            (other, "10.96.0.99:9092".parse().unwrap()),
        ]);

        assert!(dmesh_channel(Some(&d), Some(session), other).is_err());
        assert_eq!(d.metrics.backend_take_errors.get(), 1);
        assert_eq!(d.metrics.backend_target_mismatches.get(), 1);
    }

    /// A close evicts exactly its own generation, so the next session's channel
    /// is the only one on offer without waiting for a reconnect to discover it.
    #[test]
    fn the_next_generation_is_admitted_immediately() {
        let d = dmesh();
        let addr: SocketAddr = "10.96.0.14:9092".parse().unwrap();
        let old = publish(&d, addr, SessionToken::new(0, 0, 0));
        dmesh_channel(Some(&d), Some(old.session), addr)
            .unwrap()
            .unwrap();
        d.backends.remove(&old);

        let new = publish(&d, addr, SessionToken::new(0, 0, 1));
        assert_eq!(d.backends.sessions_for(&addr).len(), 1);
        dmesh_channel(Some(&d), Some(new.session), addr)
            .unwrap()
            .unwrap();
    }
}
