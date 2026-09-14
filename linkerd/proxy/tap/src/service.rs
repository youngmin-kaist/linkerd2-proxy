use crate::{
    grpc::{Tap, TapResponsePayload},
    registry::Registry,
    Inspect,
};
use futures::{future, ready, TryFutureExt};
use linkerd_proxy_http::HasH2Reason;
use linkerd_stack::{layer, NewService};
use pin_project::{pin_project, pinned_drop};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::watch;

/// Makes wrapped Services to record taps.
#[derive(Clone, Debug)]
pub struct NewTapHttp<N> {
    inner: N,
    registry: Registry,
}

/// A middleware that records HTTP taps.
///
/// Holds a local snapshot of the registered taps, refreshed from the
/// registry's watch only when it reports a change (one atomic load per
/// request). When no taps are registered — the overwhelmingly common case —
/// requests take a fast path that neither locks the registry nor boxes a
/// future.
#[derive(Debug)]
pub struct TapHttp<S, I> {
    inner: S,
    inspect: I,
    taps_rx: watch::Receiver<Vec<Tap>>,
    taps: Vec<Tap>,
}

// A `Body` instrumented with taps.
#[pin_project(PinnedDrop, project = BodyProj)]
#[derive(Debug)]
pub struct Body<B>
where
    B: linkerd_proxy_http::Body,
    B::Error: HasH2Reason,
{
    #[pin]
    inner: B,
    taps: Vec<TapResponsePayload>,
}

// === NewTapHttp ===

impl<N> NewTapHttp<N> {
    pub fn layer(registry: Registry) -> impl layer::Layer<N, Service = Self> + Clone {
        layer::mk(move |inner| Self {
            inner,
            registry: registry.clone(),
        })
    }
}

impl<N, I> NewService<I> for NewTapHttp<N>
where
    N: NewService<I>,
    I: Inspect + Clone,
{
    type Service = TapHttp<N::Service, I>;

    fn new_service(&self, target: I) -> Self::Service {
        let mut taps_rx = self.registry.subscribe();
        let taps = taps_rx.borrow_and_update().clone();
        TapHttp {
            inspect: target.clone(),
            inner: self.inner.new_service(target),
            taps_rx,
            taps,
        }
    }
}

impl<S: Clone, I: Clone> Clone for TapHttp<S, I> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            inspect: self.inspect.clone(),
            taps_rx: self.taps_rx.clone(),
            taps: self.taps.clone(),
        }
    }
}

/// Wraps an untapped response body (no taps registered). `Vec::new()` does
/// not allocate, so this is free.
fn untapped<B>(rsp: http::Response<B>) -> http::Response<Body<B>>
where
    B: linkerd_proxy_http::Body,
    B::Error: HasH2Reason,
{
    rsp.map(|inner| Body::new(inner, Vec::new()))
}

// === Service ===

impl<S, I, A, B> tower::Service<http::Request<A>> for TapHttp<S, I>
where
    S: tower::Service<http::Request<A>, Response = http::Response<B>>,
    S::Error: HasH2Reason,
    S::Future: Send + 'static,
    I: Inspect,
    A: linkerd_proxy_http::Body,
    A::Error: HasH2Reason,
    B: linkerd_proxy_http::Body,
    B::Error: HasH2Reason,
{
    type Response = http::Response<Body<B>>;
    type Error = S::Error;
    type Future = future::Either<
        future::MapOk<S::Future, fn(http::Response<B>) -> http::Response<Body<B>>>,
        Pin<Box<dyn Future<Output = Result<Self::Response, S::Error>> + Send + 'static>>,
    >;

    #[inline]
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<A>) -> Self::Future {
        // Refresh the snapshot only when the registry changed. `has_changed`
        // errs only once the registry (sender) is gone; keep the last snapshot.
        if let Ok(true) = self.taps_rx.has_changed() {
            self.taps = self.taps_rx.borrow_and_update().clone();
        }

        // Fast path: no taps registered — no lock, no boxed future.
        if self.taps.is_empty() {
            return future::Either::Left(
                self.inner
                    .call(req)
                    .map_ok(untapped::<B> as fn(http::Response<B>) -> http::Response<Body<B>>),
            );
        }

        // Record the request and obtain request-body and response taps.
        let mut rsp_taps = Vec::new();

        for mut t in self.taps.clone() {
            if let Some(rsp_tap) = t.tap(&req, &self.inspect) {
                rsp_taps.push(rsp_tap);
            }
        }

        let call = self.inner.call(req);
        future::Either::Right(Box::pin(async move {
            match call.await {
                Ok(rsp) => {
                    // Tap the response headers and use the response
                    // body taps to decorate the response body.
                    let taps = rsp_taps.drain(..).map(|t| t.tap(&rsp)).collect::<Vec<_>>();
                    let rsp = rsp.map(|inner| Body::new(inner, taps));
                    Ok(rsp)
                }
                Err(e) => {
                    for tap in rsp_taps.drain(..) {
                        tap.fail(&e);
                    }
                    Err(e)
                }
            }
        }))
    }
}

// === impl Body ===

impl<B> Body<B>
where
    B: linkerd_proxy_http::Body,
    B::Error: HasH2Reason,
{
    fn new(inner: B, mut taps: Vec<TapResponsePayload>) -> Self {
        // If the body is already finished, record the end of the stream.
        if inner.is_end_stream() {
            taps.drain(..).for_each(|t| t.eos(None));
        }

        Self { inner, taps }
    }
}

impl<B> Default for Body<B>
where
    B: linkerd_proxy_http::Body + Default,
    B::Error: HasH2Reason,
{
    fn default() -> Self {
        Self {
            inner: B::default(),
            taps: Vec::default(),
        }
    }
}

impl<B> linkerd_proxy_http::Body for Body<B>
where
    B: linkerd_proxy_http::Body,
    B::Error: HasH2Reason,
{
    type Data = B::Data;
    type Error = B::Error;

    #[inline]
    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let BodyProj { mut inner, taps } = self.project();

        // Poll the inner body for the next frame.
        let frame = match ready!(inner.as_mut().poll_frame(cx)) {
            Some(Ok(frame)) => frame,
            Some(Err(error)) => {
                // If an error occurred, we have reached the end of the stream.
                taps.drain(..).for_each(|t| t.fail(&error));
                return Poll::Ready(Some(Err(error)));
            }
            None => {
                // If there is not another frame, we have reached the end of the stream.
                taps.drain(..).for_each(|t| t.eos(None));
                return Poll::Ready(None);
            }
        };

        // If we received a trailers frame, we have reached the end of the stream.
        if let trailers @ Some(_) = frame.trailers_ref() {
            taps.drain(..).for_each(|t| t.eos(trailers));
            return Poll::Ready(Some(Ok(frame)));
        }

        // Otherwise, we *may* reached the end of the stream. If so, there are no trailers.
        if inner.is_end_stream() {
            taps.drain(..).for_each(|t| t.eos(None));
        }

        Poll::Ready(Some(Ok(frame)))
    }

    #[inline]
    fn size_hint(&self) -> hyper::body::SizeHint {
        self.inner.size_hint()
    }
}

#[pinned_drop]
impl<B> PinnedDrop for Body<B>
where
    B: linkerd_proxy_http::Body,
    B::Error: HasH2Reason,
{
    fn drop(self: Pin<&mut Self>) {
        let BodyProj { inner: _, taps } = self.project();
        taps.drain(..).for_each(|t| t.eos(None));
    }
}
