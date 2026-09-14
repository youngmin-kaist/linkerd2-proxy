#![deny(rust_2018_idioms, clippy::disallowed_methods, clippy::disallowed_types)]
#![forbid(unsafe_code)]

use futures::prelude::*;
use linkerd_error::Error;
use linkerd_stack::{layer, ExtractParam, NewService, Oneshot, Service, ServiceExt};
use std::{
    fmt::Debug,
    marker::PhantomData,
    task::{Context, Poll},
};
use tracing::debug;

mod cache;

pub use self::cache::{Cache, NewCache};

pub trait SelectRoute<Req> {
    type Key;
    type Error: Into<Error>;

    /// Given a request, returns the key matching this request.
    ///
    /// If no route matches the request, this method returns an error.
    fn select(&self, req: &Req) -> Result<Self::Key, Self::Error>;
}

/// A [`NewService`] that builds `Route` services for targets that provide a
/// [`SelectRoute`].
///
/// The selector is built with an `X`-typed [`ExtractParam`] implementation.
#[derive(Debug)]
pub struct NewOneshotRoute<Sel, X, N> {
    extract: X,
    inner: N,
    _marker: PhantomData<fn() -> Sel>,
}

/// Dispatches requests to a new `S`-typed inner service.
///
/// Each request is matched against the route table and routed to a new inner
/// service via a [`Oneshot`].
#[derive(Clone, Debug)]
pub struct OneshotRoute<Sel, N> {
    select: Sel,
    new_route: N,
}

// === impl NewOneshotRoute ===

impl<Sel, X, N> NewOneshotRoute<Sel, X, N> {
    pub fn new(extract: X, inner: N) -> Self {
        Self {
            extract,
            inner,
            _marker: PhantomData,
        }
    }
}

impl<Sel, X: Clone, N> NewOneshotRoute<Sel, X, N> {
    /// Builds a [`layer::Layer`] that produces `NewOneshotRoute`s.
    ///
    /// Targets must produce a `Sel` via the provided `X`-typed
    /// [`ExtractParam`].
    pub fn layer_via(extract: X) -> impl layer::Layer<N, Service = Self> + Clone {
        layer::mk(move |inner| Self::new(extract.clone(), inner))
    }
}

impl<Sel, N> NewOneshotRoute<Sel, (), N> {
    /// Builds a [`layer::Layer`] that produces `NewOneshotRoute`s.
    ///
    /// Target types must implement `Param<Sel>`.
    pub fn layer() -> impl layer::Layer<N, Service = Self> + Clone {
        Self::layer_via(())
    }
}

impl<Sel, K, N> NewOneshotRoute<Sel, (), NewCache<K, N>> {
    /// Builds a [`layer::Layer`] that produces `NewOneshotRoute`s using a cache
    /// of inner services.
    ///
    /// Target types must implement `Param<Sel>`.
    pub fn layer_cached() -> impl layer::Layer<N, Service = Self> + Clone {
        layer::mk(move |inner: N| Self::new((), NewCache::new(inner)))
    }
}

impl<T, Sel, X, N> NewService<T> for NewOneshotRoute<Sel, X, N>
where
    X: ExtractParam<Sel, T>,
    N: NewService<T>,
{
    type Service = OneshotRoute<Sel, N::Service>;

    fn new_service(&self, target: T) -> Self::Service {
        let select = self.extract.extract_param(&target);
        let new_route = self.inner.new_service(target);
        OneshotRoute { select, new_route }
    }
}

impl<Sel, X: Clone, N: Clone> Clone for NewOneshotRoute<Sel, X, N> {
    fn clone(&self) -> Self {
        Self {
            extract: self.extract.clone(),
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

// === impl OneshotRoute ===

impl<Sel, N, S, Req> Service<Req> for OneshotRoute<Sel, N>
where
    Sel: SelectRoute<Req>,
    N: NewService<Sel::Key, Service = S>,
    S: Service<Req>,
    S::Error: Into<Error>,
{
    type Response = S::Response;
    type Error = Error;
    type Future = future::Either<
        future::MapErr<Oneshot<S, Req>, fn(S::Error) -> Error>,
        future::Ready<Result<S::Response, Error>>,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        match self.select.select(&req) {
            Ok(key) => future::Either::Left({
                let route = self.new_route.new_service(key);
                route.oneshot(req).map_err(Into::into)
            }),
            Err(e) => future::Either::Right({
                let error = e.into();
                debug!(%error, "Failed to route request");
                future::err(error)
            }),
        }
    }
}


// === Memoized variant ===

/// Like [`NewOneshotRoute`], but the produced service memoizes the most
/// recently selected route's inner service: consecutive requests selecting the
/// same key call the stored service directly instead of cloning a fresh copy
/// of the whole route stack per request (the `new_service -> box_clone_sync ->
/// LoadShed/Distribute deep-clone` chain). On a key change, or whenever the
/// stored service is not immediately ready, it falls back to the standard
/// clone + [`Oneshot`] path, so tower semantics are preserved.
///
/// Route/policy updates are safe: updated params produce a different key
/// (cache miss), and a rebuilt router starts with an empty memo.
#[derive(Debug)]
pub struct NewMemoOneshotRoute<Req, Sel, X, N> {
    extract: X,
    inner: N,
    _marker: PhantomData<fn(Req) -> Sel>,
}

pub struct MemoOneshotRoute<Req, Sel: SelectRoute<Req>, N: NewService<Sel::Key>> {
    select: Sel,
    new_route: N,
    cached: Option<(Sel::Key, N::Service)>,
    _marker: PhantomData<fn(Req)>,
}

impl<Req, Sel, X, N> NewMemoOneshotRoute<Req, Sel, X, N> {
    pub fn new(extract: X, inner: N) -> Self {
        Self {
            extract,
            inner,
            _marker: PhantomData,
        }
    }
}

impl<Req, Sel, K, N> NewMemoOneshotRoute<Req, Sel, (), NewCache<K, N>> {
    /// Builds a [`layer::Layer`] that produces `NewMemoOneshotRoute`s using a
    /// cache of inner services.
    pub fn layer_cached() -> impl layer::Layer<N, Service = Self> + Clone {
        layer::mk(move |inner: N| Self::new((), NewCache::new(inner)))
    }
}

impl<Req, T, Sel, X, N> NewService<T> for NewMemoOneshotRoute<Req, Sel, X, N>
where
    Sel: SelectRoute<Req>,
    X: ExtractParam<Sel, T>,
    N: NewService<T>,
    N::Service: NewService<Sel::Key>,
{
    type Service = MemoOneshotRoute<Req, Sel, N::Service>;

    fn new_service(&self, target: T) -> Self::Service {
        let select = self.extract.extract_param(&target);
        let new_route = self.inner.new_service(target);
        MemoOneshotRoute {
            select,
            new_route,
            cached: None,
            _marker: PhantomData,
        }
    }
}

impl<Req, Sel, X: Clone, N: Clone> Clone for NewMemoOneshotRoute<Req, Sel, X, N> {
    fn clone(&self) -> Self {
        Self {
            extract: self.extract.clone(),
            inner: self.inner.clone(),
            _marker: PhantomData,
        }
    }
}

/// Clones start with an empty memo (each clone — typically one per
/// connection — warms its own cache on first use).
impl<Req, Sel, N> Clone for MemoOneshotRoute<Req, Sel, N>
where
    Sel: SelectRoute<Req> + Clone,
    N: NewService<Sel::Key> + Clone,
{
    fn clone(&self) -> Self {
        Self {
            select: self.select.clone(),
            new_route: self.new_route.clone(),
            cached: None,
            _marker: PhantomData,
        }
    }
}

impl<Req, Sel, N, S> Service<Req> for MemoOneshotRoute<Req, Sel, N>
where
    Sel: SelectRoute<Req>,
    Sel::Key: PartialEq + Clone,
    N: NewService<Sel::Key, Service = S>,
    S: Service<Req>,
    S::Error: Into<Error>,
{
    type Response = S::Response;
    type Error = Error;
    type Future = future::Either<
        future::MapErr<S::Future, fn(S::Error) -> Error>,
        future::Either<
            future::MapErr<Oneshot<S, Req>, fn(S::Error) -> Error>,
            future::Ready<Result<S::Response, Error>>,
        >,
    >;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let key = match self.select.select(&req) {
            Ok(key) => key,
            Err(e) => {
                let error = e.into();
                debug!(%error, "Failed to route request");
                return future::Either::Right(future::Either::Right(future::err(error)));
            }
        };

        // Fast path: the memoized service matches the key and is immediately
        // ready — call it directly, no clone. A noop waker is fine here: if
        // the service is pending we discard this poll and the fallback path
        // re-registers with the real waker.
        if let Some((k, svc)) = self.cached.as_mut() {
            if *k == key {
                let mut cx = Context::from_waker(futures::task::noop_waker_ref());
                if let Poll::Ready(Ok(())) = svc.poll_ready(&mut cx) {
                    return future::Either::Left(
                        svc.call(req).map_err(Into::into as fn(S::Error) -> Error),
                    );
                }
                self.cached = None;
            } else {
                self.cached = None;
            }
        }

        // Slow path (first request / key change): build the route service
        // once. If it is immediately ready, serve from it and memoize it;
        // otherwise let a Oneshot drive it (and skip memoizing this copy).
        let mut svc = self.new_route.new_service(key.clone());
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        if let Poll::Ready(Ok(())) = svc.poll_ready(&mut cx) {
            let fut = svc.call(req).map_err(Into::into as fn(S::Error) -> Error);
            self.cached = Some((key, svc));
            return future::Either::Left(fut);
        }
        future::Either::Right(future::Either::Left(
            svc.oneshot(req).map_err(Into::into),
        ))
    }
}

// === impl SelectRoute ===

impl<T, K, E, F> SelectRoute<T> for F
where
    K: Clone,
    E: std::error::Error + Send + Sync + 'static,
    F: Fn(&T) -> Result<K, E>,
{
    type Key = K;
    type Error = E;

    fn select(&self, t: &T) -> Result<Self::Key, E> {
        (self)(t)
    }
}

#[cfg(test)]
mod memo_tests {
    use super::*;
    use std::{
        pin::Pin,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    };

    #[derive(Debug)]
    struct NoRoute;
    impl std::fmt::Display for NoRoute {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("no route")
        }
    }
    impl std::error::Error for NoRoute {}

    /// Routes `req` to key `req / 10`; `0` has no route.
    #[derive(Clone)]
    struct Sel;
    impl SelectRoute<u8> for Sel {
        type Key = u8;
        type Error = NoRoute;
        fn select(&self, req: &u8) -> Result<u8, NoRoute> {
            if *req == 0 {
                return Err(NoRoute);
            }
            Ok(*req / 10)
        }
    }

    /// Builds a `Svc` per key, counting how many were built.
    #[derive(Clone)]
    struct NewSvc {
        built: Arc<AtomicUsize>,
        ready: Arc<AtomicBool>,
    }
    impl NewService<u8> for NewSvc {
        type Service = Svc;
        fn new_service(&self, key: u8) -> Svc {
            self.built.fetch_add(1, Ordering::SeqCst);
            Svc {
                key,
                ready: self.ready.clone(),
                calls: 0,
            }
        }
    }

    /// Responds with `(key, nth call on this instance)`.
    struct Svc {
        key: u8,
        ready: Arc<AtomicBool>,
        calls: usize,
    }
    impl Service<u8> for Svc {
        type Response = (u8, usize);
        type Error = Error;
        type Future = future::Ready<Result<(u8, usize), Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Error>> {
            if self.ready.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }
        fn call(&mut self, _: u8) -> Self::Future {
            self.calls += 1;
            future::ok((self.key, self.calls))
        }
    }

    fn mk() -> (MemoOneshotRoute<u8, Sel, NewSvc>, Arc<AtomicUsize>, Arc<AtomicBool>) {
        let built = Arc::new(AtomicUsize::new(0));
        let ready = Arc::new(AtomicBool::new(true));
        let route = MemoOneshotRoute {
            select: Sel,
            new_route: NewSvc {
                built: built.clone(),
                ready: ready.clone(),
            },
            cached: None,
            _marker: PhantomData,
        };
        (route, built, ready)
    }

    fn poll<F: Future>(fut: F) -> Poll<F::Output> {
        let mut fut = Box::pin(fut);
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        Pin::new(&mut fut).poll(&mut cx)
    }

    fn call(route: &mut MemoOneshotRoute<u8, Sel, NewSvc>, req: u8) -> Poll<Result<(u8, usize), Error>> {
        let mut cx = Context::from_waker(futures::task::noop_waker_ref());
        assert!(route.poll_ready(&mut cx).is_ready());
        poll(route.call(req))
    }

    #[test]
    fn memo_hits_for_same_key() {
        let (mut route, built, _) = mk();
        // First request builds the route service and memoizes it.
        assert!(matches!(call(&mut route, 11), Poll::Ready(Ok((1, 1)))));
        assert_eq!(built.load(Ordering::SeqCst), 1);
        // Same key: the memoized instance is called directly (call count grows,
        // nothing rebuilt).
        assert!(matches!(call(&mut route, 12), Poll::Ready(Ok((1, 2)))));
        assert!(matches!(call(&mut route, 19), Poll::Ready(Ok((1, 3)))));
        assert_eq!(built.load(Ordering::SeqCst), 1);
        assert!(route.cached.as_ref().map(|(k, _)| *k) == Some(1));
    }

    #[test]
    fn memo_invalidates_on_key_change() {
        let (mut route, built, _) = mk();
        assert!(matches!(call(&mut route, 11), Poll::Ready(Ok((1, 1)))));
        assert_eq!(built.load(Ordering::SeqCst), 1);
        // A different key must never be served by the memoized instance.
        assert!(matches!(call(&mut route, 25), Poll::Ready(Ok((2, 1)))));
        assert_eq!(built.load(Ordering::SeqCst), 2);
        assert!(route.cached.as_ref().map(|(k, _)| *k) == Some(2));
        // And switching back rebuilds again (only the last key is memoized).
        assert!(matches!(call(&mut route, 15), Poll::Ready(Ok((1, 1)))));
        assert_eq!(built.load(Ordering::SeqCst), 3);
        assert!(route.cached.as_ref().map(|(k, _)| *k) == Some(1));
    }

    #[test]
    fn memo_skipped_when_not_ready() {
        let (mut route, built, ready) = mk();
        assert!(matches!(call(&mut route, 11), Poll::Ready(Ok((1, 1)))));
        assert_eq!(built.load(Ordering::SeqCst), 1);

        // The memoized service becomes unready: the memo is dropped and the
        // request falls back to a fresh clone driven by a Oneshot (pending).
        ready.store(false, Ordering::SeqCst);
        assert!(call(&mut route, 12).is_pending());
        assert_eq!(built.load(Ordering::SeqCst), 2);
        assert!(route.cached.is_none());

        // Once ready again, the next request memoizes a new instance.
        ready.store(true, Ordering::SeqCst);
        assert!(matches!(call(&mut route, 13), Poll::Ready(Ok((1, 1)))));
        assert_eq!(built.load(Ordering::SeqCst), 3);
        assert!(matches!(call(&mut route, 14), Poll::Ready(Ok((1, 2)))));
        assert_eq!(built.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn no_route_is_an_error() {
        let (mut route, built, _) = mk();
        assert!(matches!(call(&mut route, 0), Poll::Ready(Err(_))));
        assert_eq!(built.load(Ordering::SeqCst), 0);
        assert!(route.cached.is_none());
    }

    #[test]
    fn clone_starts_with_empty_memo() {
        let (mut route, built, _) = mk();
        assert!(matches!(call(&mut route, 11), Poll::Ready(Ok((1, 1)))));
        let mut clone = route.clone();
        assert!(clone.cached.is_none());
        assert!(matches!(call(&mut clone, 11), Poll::Ready(Ok((1, 1)))));
        assert_eq!(built.load(Ordering::SeqCst), 2);
    }
}
