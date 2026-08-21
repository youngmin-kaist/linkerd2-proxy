use super::session::SessionHandle;
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
    /// The DMA session dialing this endpoint; `SessionHandle(None)` for
    /// socket-origin connections.
    session: SessionHandle,
}

/// Prevents outbound connections on the loopback interface, unless the
/// `allow-loopback` feature is enabled.
#[derive(Clone, Debug)]
pub struct PreventLoopback<S>(S);

/// Physical connector that reaches DMA-provided backends through their
/// registered dmesh channel and everything else via TCP.
///
/// A DPUmesh session publishes the endpoint that provides its service into the
/// worker's `Backends`. The session that owns a dial arrives as the
/// [`SessionHandle`] connect parameter, carried down from the frontend
/// connection itself; a stack built for exactly one session additionally bakes
/// that session in as `session`, which serves as a fallback and as a
/// consistency check. The normal socket-listener stack has neither and always
/// dials TCP.
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
    /// True on stacks serving DMA frontend connections. On such stacks a
    /// sessionless dial to a DMesh-provided address is refused instead of
    /// falling through to TCP.
    dmesh_origin: bool,
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
            dmesh_origin: self.config.dmesh_origin,
        };
        self.clone().with_stack(connect)
    }
}

/// The channel a DMesh-provided service is reached through.
///
/// The session is resolved from the connection first (`handed`, carried by the
/// server-side I/O) and from the stack's binding second (`baked`, set when a
/// stack is built for exactly one session). A stack shared by many sessions
/// leaves `baked` unset and relies entirely on what each connection carries.
///
/// `Ok(None)` means the address is not one DPUmesh provides, so the caller
/// dials it over TCP as any stock proxy would. On a DMA-origin stack a
/// sessionless dial to a DMesh-provided address is refused instead: a stream
/// that took TCP would run without the policy the mesh applied.
#[cfg(feature = "doca")]
fn dmesh_channel(
    dmesh: Option<&dmesh_doca::Dmesh>,
    baked: Option<dmesh_doca::SessionToken>,
    handed: SessionHandle,
    dmesh_origin: bool,
    addr: std::net::SocketAddr,
) -> io::Result<Option<dmesh_doca::DmeshIo>> {
    let Some(dmesh) = dmesh else {
        return Ok(None);
    };
    let handed = handed.0.map(dmesh_doca::SessionToken::from);
    if let (Some(carried), Some(bound)) = (handed, baked) {
        if carried != bound {
            dmesh.metrics.backend_session_mismatches.inc();
            tracing::warn!(
                %carried,
                %bound,
                "Connection carried a session other than the one its stack is bound to"
            );
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                format!("dmesh session mismatch: connection {carried}, stack {bound}"),
            ));
        }
    }
    let Some(session) = handed.or(baked) else {
        if dmesh_origin && dmesh.backends.manages(&addr) {
            dmesh.metrics.backend_sessionless_refusals.inc();
            tracing::warn!(
                server.addr = %addr,
                "Sessionless connect to a DMesh-provided address; refusing to dial it over TCP"
            );
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                format!("dmesh-provided address {addr} dialed without a session"),
            ));
        }
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
    T: svc::Param<SessionHandle>,
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
        let handed: SessionHandle = ep.param();
        match dmesh_channel(
            self.dmesh.as_ref(),
            self.session,
            handed,
            self.dmesh_origin,
            addr,
        ) {
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
        Self {
            addr,
            tls,
            session: SessionHandle(None),
        }
    }

    /// Names the DMA session this endpoint is dialed for.
    pub fn with_session(mut self, session: SessionHandle) -> Self {
        self.session = session;
        self
    }
}

impl svc::Param<Remote<ServerAddr>> for Connect {
    fn param(&self) -> Remote<ServerAddr> {
        self.addr
    }
}

impl svc::Param<SessionHandle> for Connect {
    fn param(&self) -> SessionHandle {
        self.session
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
        let (io, _handle) = dmesh_doca::dmesh_io_pair(addr, Some(session));
        d.backends.publish(key, io).unwrap();
        key
    }

    /// A session baked into a single-session stack, with nothing carried.
    fn baked(
        d: &Dmesh,
        session: SessionToken,
        addr: SocketAddr,
    ) -> io::Result<Option<dmesh_doca::DmeshIo>> {
        dmesh_channel(Some(d), Some(session), SessionHandle(None), true, addr)
    }

    /// A session carried by the connection through a shared, unbaked stack.
    fn carried(
        d: &Dmesh,
        session: SessionToken,
        addr: SocketAddr,
    ) -> io::Result<Option<dmesh_doca::DmeshIo>> {
        dmesh_channel(
            Some(d),
            None,
            SessionHandle(Some(session.into())),
            true,
            addr,
        )
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

        baked(&d, second_session, addr).unwrap().unwrap();
        assert_eq!(
            d.backends.take_session(second_session, addr).unwrap_err(),
            dmesh_doca::TakeError::AlreadyTaken
        );
        baked(&d, first_session, addr).unwrap().unwrap();
        assert_eq!(
            d.backends.take_session(first_session, addr).unwrap_err(),
            dmesh_doca::TakeError::AlreadyTaken
        );
    }

    /// The shared-stack analogue of the test above: nothing is baked into the
    /// connector, and each connection's carried session still takes exactly
    /// its own channel.
    #[test]
    fn carried_sessions_take_their_own_channels_through_one_connector() {
        let d = dmesh();
        let addr: SocketAddr = "10.96.0.11:9092".parse().unwrap();
        let first_session = SessionToken::new(0, 0, 0);
        let second_session = SessionToken::new(0, 1, 0);
        publish(&d, addr, first_session);
        publish(&d, addr, second_session);

        carried(&d, second_session, addr).unwrap().unwrap();
        assert_eq!(
            d.backends.take_session(second_session, addr).unwrap_err(),
            dmesh_doca::TakeError::AlreadyTaken
        );
        carried(&d, first_session, addr).unwrap().unwrap();
        assert_eq!(
            d.backends.take_session(first_session, addr).unwrap_err(),
            dmesh_doca::TakeError::AlreadyTaken
        );
        assert_eq!(d.metrics.backend_take_errors.get(), 0);
        assert_eq!(d.metrics.backend_session_mismatches.get(), 0);
    }

    /// A single-session stack must never consume another session's backend.
    #[test]
    fn a_carried_session_mismatch_is_refused() {
        let d = dmesh();
        let addr: SocketAddr = "10.96.0.11:9092".parse().unwrap();
        let baked_session = SessionToken::new(0, 0, 0);
        let carried_session = SessionToken::new(0, 1, 0);
        publish(&d, addr, baked_session);
        publish(&d, addr, carried_session);

        let error = dmesh_channel(
            Some(&d),
            Some(baked_session),
            SessionHandle(Some(carried_session.into())),
            true,
            addr,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotConnected);
        d.backends.take_session(carried_session, addr).unwrap();
        d.backends.take_session(baked_session, addr).unwrap();
        assert_eq!(d.metrics.backend_session_mismatches.get(), 1);
    }

    /// On a DMA-origin stack, a sessionless dial to a provided address is
    /// refused; the socket-listener stack falls through to TCP as ever.
    #[test]
    fn a_sessionless_dial_to_a_provided_address_is_refused() {
        let d = dmesh();
        let addr: SocketAddr = "10.96.0.12:9092".parse().unwrap();
        publish(&d, addr, SessionToken::new(0, 0, 0));

        let error = dmesh_channel(Some(&d), None, SessionHandle(None), true, addr).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotConnected);
        assert_eq!(d.metrics.backend_sessionless_refusals.get(), 1);

        assert!(
            dmesh_channel(Some(&d), None, SessionHandle(None), false, addr)
                .unwrap()
                .is_none()
        );
        assert_eq!(d.metrics.backend_sessionless_refusals.get(), 1);
    }

    #[test]
    fn a_meshed_service_is_not_dialed_over_tcp() {
        let d = dmesh();
        let addr: SocketAddr = "10.96.0.12:9092".parse().unwrap();
        let key = publish(&d, addr, SessionToken::new(0, 0, 0));

        baked(&d, key.session, addr).unwrap().unwrap();
        // Taken, then withdrawn by the close: both are refusals, not fallbacks.
        let error = baked(&d, key.session, addr).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotConnected);
        d.backends.remove(&key);
        assert!(baked(&d, key.session, addr).is_err());
        assert_eq!(d.metrics.backend_take_errors.get(), 2);
    }

    #[test]
    fn an_unmeshed_address_falls_through_to_tcp() {
        let d = dmesh();
        let addr: SocketAddr = "10.96.0.13:9092".parse().unwrap();
        let session = SessionToken::new(0, 0, 0);
        assert!(
            dmesh_channel(None, Some(session), SessionHandle(None), true, addr)
                .unwrap()
                .is_none()
        );
        assert!(
            dmesh_channel(Some(&d), None, SessionHandle(None), true, addr)
                .unwrap()
                .is_none()
        );
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
        let (io, _handle) = dmesh_doca::dmesh_io_pair(service, Some(session));
        d.backends.publish(key, io).unwrap();
        d.backends.place_targets([
            (service, service),
            (selected, service),
            (other, "10.96.0.99:9092".parse().unwrap()),
        ]);

        assert!(baked(&d, session, other).is_err());
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
        baked(&d, old.session, addr).unwrap().unwrap();
        d.backends.remove(&old);

        let new = publish(&d, addr, SessionToken::new(0, 0, 1));
        assert_eq!(d.backends.sessions_for(&addr).len(), 1);
        baked(&d, new.session, addr).unwrap().unwrap();
    }
}
