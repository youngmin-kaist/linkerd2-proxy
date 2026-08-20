//! Carries a server-side connection's DMA session through the endpoint
//! connect stack.
//!
//! [`ForwardSession`](linkerd_app_core::proxy::tcp::ForwardSession) reads the
//! session off the connection and dispatches it — instead of `()` — through
//! the balancer to the per-endpoint [`ThunkSession`], which folds it into a
//! [`SessionEndpoint`] target. The endpoint layers thread that target down as
//! usual until `TaggedTransport` bakes the session into the [`Connect`] the
//! physical connector receives. Socket-origin connections carry no session,
//! so the whole path degenerates to what the stock thunk did.

use linkerd_app_core::{
    io::DmeshSessionId,
    svc::{layer, NewService, Param, Service},
    tls,
    transport::addrs::*,
};
use std::task::{Context, Poll};

/// The session a connect request is made on behalf of, as a stack parameter.
/// `None` for socket-origin connections.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct SessionHandle(pub Option<DmeshSessionId>);

/// An endpoint target paired with the session that is dialing it.
#[derive(Clone, Debug)]
pub struct SessionEndpoint<E> {
    endpoint: E,
    session: SessionHandle,
}

/// A `NewService<E>` that moves targets and inner services into a
/// [`ThunkSession`].
#[derive(Clone, Debug)]
pub struct NewThunkSession<S> {
    inner: S,
}

/// A `Service<Option<DmeshSessionId>>` that pairs a cloned `E`-typed endpoint
/// target with the dispatched session and calls an `S`-typed inner service.
#[derive(Clone, Debug)]
pub struct ThunkSession<E, S> {
    target: E,
    inner: S,
}

// === impl SessionEndpoint ===

impl<E> SessionEndpoint<E> {
    pub fn new(endpoint: E, session: SessionHandle) -> Self {
        Self { endpoint, session }
    }
}

impl<E> Param<SessionHandle> for SessionEndpoint<E> {
    fn param(&self) -> SessionHandle {
        self.session
    }
}

impl<E: Param<Remote<ServerAddr>>> Param<Remote<ServerAddr>> for SessionEndpoint<E> {
    fn param(&self) -> Remote<ServerAddr> {
        self.endpoint.param()
    }
}

impl<E: Param<tls::ConditionalClientTls>> Param<tls::ConditionalClientTls> for SessionEndpoint<E> {
    fn param(&self) -> tls::ConditionalClientTls {
        self.endpoint.param()
    }
}

impl<E, P> Param<Option<P>> for SessionEndpoint<E>
where
    E: Param<Option<P>>,
{
    fn param(&self) -> Option<P> {
        self.endpoint.param()
    }
}

impl<E: Param<linkerd_app_core::transport::labels::Key>>
    Param<linkerd_app_core::transport::labels::Key> for SessionEndpoint<E>
{
    fn param(&self) -> linkerd_app_core::transport::labels::Key {
        self.endpoint.param()
    }
}

impl<E: Param<crate::zone::TcpZoneLabels>> Param<crate::zone::TcpZoneLabels>
    for SessionEndpoint<E>
{
    fn param(&self) -> crate::zone::TcpZoneLabels {
        self.endpoint.param()
    }
}

// === impl NewThunkSession ===

impl<S> NewThunkSession<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }

    pub fn layer() -> impl layer::Layer<S, Service = Self> + Clone {
        layer::mk(Self::new)
    }
}

impl<S: Clone, E> NewService<E> for NewThunkSession<S> {
    type Service = ThunkSession<E, S>;

    fn new_service(&self, target: E) -> Self::Service {
        let inner = self.inner.clone();
        ThunkSession { inner, target }
    }
}

// === impl ThunkSession ===

impl<E, S> Service<Option<DmeshSessionId>> for ThunkSession<E, S>
where
    E: Clone,
    S: Service<SessionEndpoint<E>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), S::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, session: Option<DmeshSessionId>) -> S::Future {
        self.inner.call(SessionEndpoint {
            endpoint: self.target.clone(),
            session: SessionHandle(session),
        })
    }
}

#[cfg(all(test, feature = "doca"))]
mod tests {
    use super::*;
    use linkerd_app_core::{
        io,
        proxy::tcp::ForwardSession,
        svc::{self, layer::Layer, Service},
    };
    use parking_lot::Mutex;
    use std::sync::Arc;

    /// The session survives the protocol-detection wrappers on the server-side
    /// I/O and arrives at the connect stack, paired with the endpoint by the
    /// thunk — the whole data path a shared stack relies on.
    #[tokio::test(flavor = "current_thread")]
    async fn the_carried_session_reaches_the_connect_stack() {
        let token = dmesh_doca::SessionToken::new(1, 2, 3);
        let (src_io, src_handle) =
            dmesh_doca::dmesh_io_pair("10.244.0.7:4140".parse().unwrap(), Some(token));
        // What protocol detection hands the opaq stack.
        let wrapped =
            io::EitherIo::<dmesh_doca::DmeshIo, _>::Right(io::PrefixedIo::new("", src_io));

        let seen: Arc<Mutex<Vec<SessionHandle>>> = Default::default();
        let connect = {
            let seen = seen.clone();
            svc::mk(move |ep: SessionEndpoint<&'static str>| {
                seen.lock().push(svc::Param::param(&ep));
                assert_eq!(ep.endpoint, "endpoint");
                let (dst_io, held) = io::duplex(1024);
                // Dropping the far end here ends the duplex immediately.
                drop(held);
                futures::future::ok::<_, io::Error>(dst_io)
            })
        };
        let thunk = NewThunkSession::new(connect);
        let mut forward =
            ForwardSession::layer().layer(svc::NewService::new_service(&thunk, "endpoint"));

        // End the source side so the Duplex the forwarder drives completes.
        src_handle.close_rx();
        Service::call(&mut forward, wrapped)
            .await
            .expect("forwarding must complete");

        assert_eq!(seen.lock().as_slice(), &[SessionHandle(Some(token.into()))]);
    }
}
