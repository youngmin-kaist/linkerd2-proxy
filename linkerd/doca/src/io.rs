//! `DmeshIo`: an `AsyncRead + AsyncWrite` endpoint backed by DMesh DMA
//! buffers instead of a TCP socket.
//!
//! Received bytes are read directly out of the per-connection DMA
//! staging buffer: the driver pushes `(pos, len)` segments (offsets into the
//! mapped staging region) via [`DmeshIoHandle::push_segment`], and
//! [`DmeshIo::poll_read`] copies straight from `staging_base + pos` into the
//! caller's `ReadBuf` (the single copy every `AsyncRead` performs; no extra
//! intermediate buffer on the DPU). Writes copy into the endpoint's arena
//! batch through the bound writer, under the endpoint lock; Pending never
//! retains caller bytes.
//!
//! Every endpoint belongs to one worker thread. The stack raises the worker's
//! [`DriverSignal`] when the driver has something to publish and marks the
//! endpoint dirty; the driver pumps only dirty endpoints.
//!
//! Because linkerd stacks are generic over `I: AsyncRead + AsyncWrite + Peek +
//! PeerAddr`, implementing those traits here is what lets a DMA-backed
//! connection flow through the real outbound stack (detect / discovery / LB /
//! mTLS) unchanged.

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::thread::ThreadId;

use linkerd_io::{DmeshSession, DmeshSessionId, Peek, PeerAddr};
use parking_lot::{Mutex, MutexGuard};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::api::SessionToken;

/// Exact destination selected by Linkerd discovery for backend output.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum BackendRoute {
    #[default]
    Any,
    Origin,
    Local(i32),
    Remote(String),
}

/// Write result. Retry accepts no caller bytes; an earlier batch may remain owned.
pub enum TxAttempt {
    /// Copied once into the endpoint's unpublished arena batch.
    Buffered(usize),
    Retry,
    Failed(io::Error),
}

/// Publishes one endpoint's output. Every method runs on the owning worker
/// thread with the endpoint lock held, so an implementation must not touch
/// the endpoint it serves.
pub trait TxWriter: Send {
    /// Copy an ordered prefix of `bufs`.
    fn write(&mut self, route: &BackendRoute, bufs: &[io::IoSlice<'_>]) -> TxAttempt;
    /// Some(n): published bytes, None: retry later with ownership intact.
    fn flush(&mut self) -> io::Result<Option<usize>> {
        Ok(Some(0))
    }
    fn cancel(&mut self) {}
}

/// One per worker: endpoints raise it, the driver parks on it.
#[derive(Default)]
pub struct DriverSignal {
    raised: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl DriverSignal {
    pub fn raise(&self) {
        if self.raised.swap(true, Ordering::AcqRel) {
            return;
        }
        let wake = self.waker.lock().take();
        if let Some(w) = wake {
            w.wake();
        }
    }

    /// Ready once per raise. Registering and raising may interleave freely.
    pub fn poll(&self, cx: &mut Context<'_>) -> Poll<()> {
        if self.raised.swap(false, Ordering::AcqRel) {
            return Poll::Ready(());
        }
        park(&mut self.waker.lock(), cx);
        if self.raised.swap(false, Ordering::AcqRel) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

#[derive(Default, Clone, Copy)]
pub struct TxStats {
    /// Bytes the transport took custody of.
    pub accepted: u64,
    pub publications: u64,
    /// Bytes copied into the arena, whether or not published yet.
    pub copied: u64,
    pub retries: u64,
    pub errors: u64,
}

/// What one driver pump observed and did.
pub struct Pumped {
    pub stats: TxStats,
    pub state: DrainState,
    /// The transport refused the batch; pump again on a later pass.
    pub blocked: bool,
}

thread_local! {
    static THREAD_ID: ThreadId = std::thread::current().id();
}

fn this_thread() -> ThreadId {
    THREAD_ID.with(|id| *id)
}

/// Register `cx` without cloning a waker that is already registered.
fn park(slot: &mut Option<Waker>, cx: &Context<'_>) {
    if !slot.as_ref().is_some_and(|w| w.will_wake(cx.waker())) {
        *slot = Some(cx.waker().clone());
    }
}

/// Every `DmeshIo`/`DmeshIoHandle` operation takes the same per-connection lock.
fn lock(shared: &Shared) -> MutexGuard<'_, Inner> {
    shared.inner.lock()
}

struct Shared {
    inner: Mutex<Inner>,
    /// The driver should pump this endpoint on its next pass.
    dirty: AtomicBool,
    /// A terminal transport error is latched.
    failed: AtomicBool,
}

impl Shared {
    fn mark(&self) {
        self.dirty.store(true, Ordering::Release);
    }
}

#[derive(Default)]
struct Inner {
    backend_route: BackendRoute,
    /// Owned received bytes (copy path; used by tests and any non-DMA push).
    rx: Vec<u8>,

    /// Zero-copy staging: base address (as usize for Send) + length of the
    /// mapped DMA staging region, and a FIFO of completed `(pos, len)`
    /// segments within it. Read directly by `poll_read`.
    staging_base: usize,
    staging_len: usize,
    segs: VecDeque<(u32, u32)>,
    seg_read_off: usize, // partial-consume cursor into segs.front()

    /// Peer half closed: reads drain rx/segs then return EOF.
    rx_closed: bool,
    rx_waker: Option<Waker>,

    writer: Option<Box<dyn TxWriter>>,
    owner: Option<ThreadId>,
    signal: Option<Arc<DriverSignal>>,
    tx_error: Option<io::ErrorKind>,
    tx_buffered: bool,
    tx_stats: TxStats,
    /// Local half shut down: further writes fail.
    tx_closed: bool,
    /// The stack dropped the endpoint without an orderly poll_shutdown.
    tx_aborted: bool,
    tx_waker: Option<Waker>,
}

impl Inner {
    fn rx_has_data(&self) -> bool {
        !self.rx.is_empty() || !self.segs.is_empty()
    }

    fn drain_state(&self) -> DrainState {
        DrainState {
            has_rx: self.rx_has_data(),
            tx_finished: self.tx_closed && !self.tx_buffered,
            tx_aborted: self.tx_aborted || self.tx_error.is_some(),
        }
    }

    fn fail(&mut self, shared: &Shared, error: &io::Error) {
        self.tx_error = Some(error.kind());
        self.tx_stats.errors += 1;
        shared.failed.store(true, Ordering::Release);
    }
}

/// Stack-facing endpoint (the `TcpStream` analogue).
pub struct DmeshIo {
    shared: Arc<Shared>,
    peer: SocketAddr,
    /// The DMA session this endpoint belongs to, carried so the outbound
    /// connector can resolve the session's backend channel from the
    /// connection itself rather than from stack configuration.
    session: Option<SessionToken>,
}

/// Driver-facing endpoint bridging the DMA staging buffer to the stack.
pub struct DmeshIoHandle {
    shared: Arc<Shared>,
}

/// What one drain pass observes about an endpoint after publishing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrainState {
    /// The reader still holds undelivered or undrained input.
    pub has_rx: bool,
    /// The stack shut its write half and every queued byte has been published.
    pub tx_finished: bool,
    /// The endpoint disappeared without publishing an orderly output FIN.
    pub tx_aborted: bool,
}

// SAFETY: `staging_base` is a raw address into a DMA region that outlives the
// connection; it is only ever read (never freed) through `DmeshIo`, whose
// access is serialized by the `Mutex`. A single logical reader consumes it.
unsafe impl Send for DmeshIo {}
unsafe impl Sync for DmeshIo {}
unsafe impl Send for DmeshIoHandle {}
unsafe impl Sync for DmeshIoHandle {}

/// Create a connected pair. The driver keeps the handle, the stack gets the IO.
/// `peer` is the flow's source address, reported via `PeerAddr`; `session`
/// names the DMA session the flow belongs to, reported via `DmeshSession`.
pub fn dmesh_io_pair(peer: SocketAddr, session: Option<SessionToken>) -> (DmeshIo, DmeshIoHandle) {
    let shared = Arc::new(Shared {
        inner: Mutex::new(Inner::default()),
        dirty: AtomicBool::new(false),
        failed: AtomicBool::new(false),
    });
    (
        DmeshIo {
            shared: shared.clone(),
            peer,
            session,
        },
        DmeshIoHandle { shared },
    )
}

impl fmt::Debug for DmeshIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DmeshIo").field("peer", &self.peer).finish()
    }
}

impl DmeshIo {
    pub(crate) fn set_backend_route(&self, route: BackendRoute) {
        lock(&self.shared).backend_route = route;
    }
}

impl AsyncRead for DmeshIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let mut inner = lock(&self.shared);

        // Owned bytes first (copy path).
        if !inner.rx.is_empty() {
            let n = buf.remaining().min(inner.rx.len());
            buf.put_slice(&inner.rx[..n]);
            inner.rx.drain(..n);
            if !inner.rx_has_data() {
                self.shared.mark();
            }
            return Poll::Ready(Ok(()));
        }

        // Zero-copy staging segment.
        if let Some(&(pos, len)) = inner.segs.front() {
            let seg_len = len as usize;
            let off = inner.seg_read_off;
            let avail = seg_len - off;
            let n = buf.remaining().min(avail);
            let base = inner.staging_base as *const u8;
            debug_assert!(pos as usize + off + n <= inner.staging_len);
            // SAFETY: [pos, pos+len) is a completed DMA segment inside the
            // staging region [base, base+staging_len) reported by the C side;
            // the region lives for the connection's lifetime and is only read.
            let src = unsafe { std::slice::from_raw_parts(base.add(pos as usize + off), n) };
            buf.put_slice(src);
            inner.seg_read_off += n;
            if inner.seg_read_off >= seg_len {
                inner.segs.pop_front();
                inner.seg_read_off = 0;
                // Fully consumed input is custody the driver can return.
                if inner.segs.is_empty() {
                    self.shared.mark();
                }
            }
            return Poll::Ready(Ok(()));
        }

        if inner.rx_closed {
            return Poll::Ready(Ok(())); // EOF
        }
        park(&mut inner.rx_waker, cx);
        Poll::Pending
    }
}

impl DmeshIo {
    fn poll_transmit(
        &self,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let offered = bufs.iter().fold(0usize, |n, b| n.saturating_add(b.len()));
        if offered == 0 {
            return Poll::Ready(Ok(0));
        }
        let shared = &*self.shared;
        let mut inner = lock(shared);
        if inner.tx_closed || inner.tx_error.is_some() {
            return Poll::Ready(Err(io::Error::new(
                inner.tx_error.unwrap_or(io::ErrorKind::BrokenPipe),
                "dmesh writer closed",
            )));
        }
        let Some(owner) = inner.owner else {
            // Not bound yet; bind_writer wakes the writer.
            park(&mut inner.tx_waker, cx);
            return Poll::Pending;
        };
        let mut raise = false;
        let result = if owner != this_thread() {
            let error = io::Error::other("dmesh writer polled on wrong thread");
            inner.fail(shared, &error);
            raise = true;
            Poll::Ready(Err(error))
        } else {
            let attempt = {
                let Inner {
                    writer,
                    backend_route,
                    ..
                } = &mut *inner;
                writer
                    .as_mut()
                    .expect("a bound endpoint has a writer")
                    .write(backend_route, bufs)
            };
            match attempt {
                TxAttempt::Buffered(got) if got > 0 && got <= offered => {
                    inner.tx_stats.copied += got as u64;
                    inner.tx_buffered = true;
                    raise = true;
                    Poll::Ready(Ok(got))
                }
                TxAttempt::Retry => {
                    // Nothing was accepted: the batch is full or the arena is
                    // dry. The driver's next pass publishes or wakes us; it is
                    // not summoned for this, so a dry arena cannot spin.
                    inner.tx_stats.retries += 1;
                    park(&mut inner.tx_waker, cx);
                    Poll::Pending
                }
                other => {
                    let error = match other {
                        TxAttempt::Failed(error) => error,
                        _ => io::Error::other("invalid arena batch length"),
                    };
                    inner.fail(shared, &error);
                    raise = true;
                    Poll::Ready(Err(error))
                }
            }
        };
        shared.mark();
        let signal = if raise { inner.signal.clone() } else { None };
        drop(inner);
        if let Some(signal) = signal {
            signal.raise();
        }
        result
    }
}

impl AsyncWrite for DmeshIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_transmit(cx, &[io::IoSlice::new(data)])
    }
    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.poll_transmit(cx, bufs)
    }
    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Accepted bytes already belong to the transport, like bytes queued by
        // a socket send, and the write that accepted them summoned the driver.
        if !self.shared.failed.load(Ordering::Acquire) {
            return Poll::Ready(Ok(()));
        }
        let kind = lock(&self.shared)
            .tx_error
            .unwrap_or(io::ErrorKind::BrokenPipe);
        Poll::Ready(Err(io::Error::new(kind, "dmesh transport failed")))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Close admission now. The driver publishes the buffered DATA and only
        // then the FIN; neither waits for destination delivery.
        let mut inner = lock(&self.shared);
        if let Some(kind) = inner.tx_error {
            return Poll::Ready(Err(io::Error::new(kind, "dmesh transport failed")));
        }
        let signal = if inner.tx_closed {
            None
        } else {
            inner.tx_closed = true;
            self.shared.mark();
            inner.signal.clone()
        };
        drop(inner);
        if let Some(signal) = signal {
            signal.raise();
        }
        Poll::Ready(Ok(()))
    }
}

#[async_trait::async_trait]
impl Peek for DmeshIo {
    /// The DMA endpoint is not peekable; return 0 so protocol detection falls
    /// back to `read_buf` + `PrefixedIo` replay (see linkerd-tls / http-detect).
    async fn peek(&self, _buf: &mut [u8]) -> io::Result<usize> {
        Ok(0)
    }
}

impl DmeshSession for DmeshIo {
    fn dmesh_session(&self) -> Option<DmeshSessionId> {
        self.session.map(Into::into)
    }
}

impl PeerAddr for DmeshIo {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer)
    }
}

impl Drop for DmeshIo {
    fn drop(&mut self) {
        let mut inner = lock(&self.shared);
        let signal = if inner.tx_closed {
            None
        } else {
            inner.tx_aborted = true;
            inner.tx_closed = true;
            self.shared.mark();
            inner.signal.clone()
        };
        drop(inner);
        if let Some(signal) = signal {
            signal.raise();
        }
    }
}

impl DmeshIoHandle {
    /// Linkerd's backend choice, installed before the connector hands the IO
    /// to the stack.
    pub fn backend_route(&self) -> BackendRoute {
        lock(&self.shared).backend_route.clone()
    }

    /// Point the reader at the connection's mapped DMA staging region. Must be
    /// set before any `push_segment`.
    pub fn set_staging(&self, base_addr: usize, len: usize) {
        let mut inner = lock(&self.shared);
        inner.staging_base = base_addr;
        inner.staging_len = len;
    }

    /// Deliver a completed recv segment `[pos, pos+len)` in the staging region
    /// to the reading stack (zero-copy: no bytes are moved here).
    pub fn push_segment(&self, pos: u32, len: u32) {
        let mut inner = lock(&self.shared);
        inner.segs.push_back((pos, len));
        let wake = inner.rx_waker.take();
        drop(inner);
        if let Some(w) = wake {
            w.wake();
        }
    }

    /// Deliver owned bytes to the reading stack (copy path; tests / non-DMA).
    pub fn push_rx(&self, bytes: &[u8]) {
        let mut inner = lock(&self.shared);
        inner.rx.extend_from_slice(bytes);
        let wake = inner.rx_waker.take();
        drop(inner);
        if let Some(w) = wake {
            w.wake();
        }
    }

    /// Signal peer half-close: pending data drains, then reads return EOF.
    pub fn close_rx(&self) {
        let mut inner = lock(&self.shared);
        inner.rx_closed = true;
        let wake = inner.rx_waker.take();
        drop(inner);
        if let Some(w) = wake {
            w.wake();
        }
    }

    /// Abort both halves and discard all buffered data.
    ///
    /// The driver must call this before returning custody for unread DMA
    /// segments. Once this returns, no stack task can retain a reference to a
    /// queued staging segment or enqueue more output. An unpublished batch is
    /// cancelled here on the owning thread; elsewhere the connection close
    /// reclaims it.
    pub fn abort(&self) {
        let mut inner = lock(&self.shared);
        inner.rx.clear();
        inner.segs.clear();
        inner.seg_read_off = 0;
        inner.staging_base = 0;
        inner.staging_len = 0;
        inner.rx_closed = true;
        let writer = inner.writer.take();
        let owner = inner.owner.take();
        inner.signal = None;
        inner.tx_buffered = false;
        inner.tx_closed = true;
        inner.tx_aborted = true;
        inner.tx_error = Some(io::ErrorKind::BrokenPipe);
        self.shared.failed.store(true, Ordering::Release);
        if let Some(mut writer) = writer {
            if owner == Some(this_thread()) {
                writer.cancel();
            }
        }
        let wakes = [inner.rx_waker.take(), inner.tx_waker.take()];
        drop(inner);
        for w in wakes.into_iter().flatten() {
            w.wake();
        }
    }

    /// Install once on the owning worker; late orphan registrations are aborted.
    pub fn bind_writer(&self, writer: Box<dyn TxWriter>, signal: Arc<DriverSignal>, origin: bool) {
        let wake = {
            let mut inner = lock(&self.shared);
            if inner.tx_closed || inner.writer.is_some() {
                return;
            }
            if origin {
                inner.backend_route = BackendRoute::Origin;
            }
            inner.writer = Some(writer);
            inner.owner = Some(this_thread());
            inner.signal = Some(signal);
            inner.tx_waker.take()
        };
        if let Some(w) = wake {
            w.wake();
        }
    }

    /// Whether the stack touched this endpoint since the driver last asked.
    pub fn take_dirty(&self) -> bool {
        self.shared.dirty.swap(false, Ordering::AcqRel)
    }

    /// One driver pass over the endpoint: publish the owned batch if any, wake
    /// a writer that can make progress again, and report what the pass saw.
    pub fn pump(&self) -> Pumped {
        let shared = &*self.shared;
        let mut inner = lock(shared);
        let mut blocked = false;
        if inner.tx_buffered && inner.tx_error.is_none() && !inner.tx_aborted {
            if inner.owner != Some(this_thread()) {
                inner.fail(shared, &io::Error::other("dmesh pump on wrong thread"));
            } else {
                let flushed = inner
                    .writer
                    .as_mut()
                    .expect("a buffered endpoint has a writer")
                    .flush();
                match flushed {
                    Ok(Some(n)) => {
                        inner.tx_stats.accepted += n as u64;
                        inner.tx_stats.publications += u64::from(n > 0);
                        inner.tx_buffered = false;
                    }
                    Ok(None) => {
                        inner.tx_stats.retries += 1;
                        blocked = true;
                    }
                    Err(error) => inner.fail(shared, &error),
                }
            }
        }
        // A writer parked on a full batch or a dry arena may try again once
        // nothing is buffered; a latched error is reported to it.
        let wake = if !inner.tx_buffered || inner.tx_error.is_some() {
            inner.tx_waker.take()
        } else {
            None
        };
        let stats = std::mem::take(&mut inner.tx_stats);
        let state = inner.drain_state();
        drop(inner);
        if let Some(w) = wake {
            w.wake();
        }
        Pumped {
            stats,
            state,
            blocked,
        }
    }

    pub fn take_tx_stats(&self) -> TxStats {
        std::mem::take(&mut lock(&self.shared).tx_stats)
    }

    /// True once the stack shut down its write half and tx is fully drained.
    pub fn tx_finished(&self) -> bool {
        let inner = lock(&self.shared);
        inner.tx_closed && !inner.tx_buffered
    }

    /// True while the reader still has undelivered/undrained data.
    pub fn has_rx(&self) -> bool {
        lock(&self.shared).rx_has_data()
    }

    pub fn drain_state(&self) -> DrainState {
        lock(&self.shared).drain_state()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn pair() -> (DmeshIo, DmeshIoHandle) {
        dmesh_io_pair("127.0.0.1:40000".parse().unwrap(), None)
    }

    #[tokio::test]
    async fn read_waits_then_delivers() {
        let (mut io, handle) = pair();

        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            let n = io.read(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });

        tokio::task::yield_now().await;
        handle.push_rx(b"hello dma");

        assert_eq!(reader.await.unwrap(), b"hello dma");
    }

    #[tokio::test]
    async fn eof_after_close() {
        let (mut io, handle) = pair();
        handle.push_rx(b"tail");
        handle.close_rx();

        let mut out = Vec::new();
        io.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"tail");
    }

    #[tokio::test]
    async fn abort_discards_buffers_and_closes_both_halves() {
        let (mut io, handle) = pair();
        handle.push_rx(b"unread");
        assert!(
            poll_fn(|cx| Poll::Ready(Pin::new(&mut io).poll_write(cx, b"unsent")))
                .await
                .is_pending()
        );

        handle.abort();

        let mut buf = [0u8; 8];
        assert_eq!(io.read(&mut buf).await.unwrap(), 0);
        assert_eq!(
            io.write_all(b"late").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(handle.take_tx_stats().accepted, 0);
        assert!(handle.tx_finished());
    }

    #[tokio::test]
    async fn zero_copy_staging_read_marks_consumed_input() {
        // Simulate a staging region: a leaked buffer the driver "DMA'd" into.
        let staging: &'static [u8] = Box::leak(
            b"....GET / HTTP/1.1\r\n\r\nXXXX"
                .to_vec()
                .into_boxed_slice(),
        );
        let (mut io, handle) = pair();
        handle.set_staging(staging.as_ptr() as usize, staging.len());
        // Two segments referencing offsets within the staging region.
        handle.push_segment(4, 14); // "GET / HTTP/1.1"
        handle.push_segment(18, 4); // "\r\n\r\n"
        handle.close_rx();
        assert!(!handle.take_dirty(), "delivery alone is not stack activity");

        let mut out = Vec::new();
        io.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"GET / HTTP/1.1\r\n\r\n");
        assert!(handle.take_dirty(), "consumed input is custody to return");
        assert!(!handle.has_rx());
    }

    #[tokio::test]
    async fn peer_addr_reports_flow_src() {
        let (io, _h) = pair();
        assert_eq!(io.peer_addr().unwrap(), "127.0.0.1:40000".parse().unwrap());
    }

    /// A writer that copies into one bounded batch the driver publishes.
    #[derive(Default)]
    struct BatchState {
        batch: Mutex<Vec<u8>>,
        published: Mutex<Vec<Vec<u8>>>,
        cap: usize,
        refuse_flush: AtomicBool,
        dry: AtomicBool,
        calls: AtomicUsize,
    }
    struct Batching(Arc<BatchState>);
    impl TxWriter for Batching {
        fn write(&mut self, _: &BackendRoute, bufs: &[io::IoSlice<'_>]) -> TxAttempt {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            if self.0.dry.load(Ordering::SeqCst) {
                return TxAttempt::Retry;
            }
            let mut batch = self.0.batch.lock();
            let mut room = self.0.cap - batch.len();
            let mut copied = 0;
            for b in bufs {
                let n = room.min(b.len());
                batch.extend_from_slice(&b[..n]);
                copied += n;
                room -= n;
            }
            if copied == 0 {
                TxAttempt::Retry
            } else {
                TxAttempt::Buffered(copied)
            }
        }
        fn flush(&mut self) -> io::Result<Option<usize>> {
            if self.0.refuse_flush.load(Ordering::SeqCst) {
                return Ok(None);
            }
            let batch = std::mem::take(&mut *self.0.batch.lock());
            let n = batch.len();
            self.0.published.lock().push(batch);
            Ok(Some(n))
        }
    }
    async fn write_once(io: &mut DmeshIo, data: &[u8]) -> Poll<io::Result<usize>> {
        poll_fn(|cx| Poll::Ready(Pin::new(&mut *io).poll_write(cx, data))).await
    }
    fn noop_cx() -> Context<'static> {
        Context::from_waker(Waker::noop())
    }
    fn bind_batching(handle: &DmeshIoHandle, cap: usize) -> (Arc<BatchState>, Arc<DriverSignal>) {
        let state = Arc::new(BatchState {
            cap,
            ..BatchState::default()
        });
        let signal = Arc::new(DriverSignal::default());
        handle.bind_writer(Box::new(Batching(state.clone())), signal.clone(), false);
        (state, signal)
    }

    #[tokio::test]
    async fn writes_summon_the_driver_once_and_the_pump_publishes() {
        let (mut io, handle) = pair();
        let (writer, signal) = bind_batching(&handle, 64);
        let mut cx = noop_cx();
        assert!(signal.poll(&mut cx).is_pending());
        io.write_all(b"one").await.unwrap();
        io.write_all(b"two").await.unwrap();
        io.flush().await.unwrap();
        assert!(signal.poll(&mut cx).is_ready(), "the first write raised it");
        assert!(
            signal.poll(&mut cx).is_pending(),
            "later writes did not re-raise"
        );
        assert!(handle.take_dirty());
        assert!(!handle.take_dirty());
        assert!(writer.published.lock().is_empty());
        let pumped = handle.pump();
        assert_eq!(&*writer.published.lock(), &[b"onetwo".to_vec()]);
        assert_eq!(pumped.stats.accepted, 6);
        assert_eq!(pumped.stats.copied, 6);
        assert_eq!(pumped.stats.publications, 1);
        assert!(!pumped.blocked);
        assert!(!pumped.state.tx_finished);
    }

    #[tokio::test]
    async fn a_full_batch_parks_the_writer_until_the_pump_publishes() {
        let (mut io, handle) = pair();
        let (writer, signal) = bind_batching(&handle, 4);
        assert!(matches!(
            write_once(&mut io, b"012345").await,
            Poll::Ready(Ok(4))
        ));
        let mut cx = noop_cx();
        assert!(signal.poll(&mut cx).is_ready());
        assert!(write_once(&mut io, b"45").await.is_pending());
        assert!(
            signal.poll(&mut cx).is_pending(),
            "a refused write does not summon the driver"
        );
        assert!(handle.take_dirty());
        let pumped = handle.pump();
        assert_eq!(pumped.stats.retries, 1);
        io.write_all(b"45").await.unwrap();
        handle.pump();
        assert_eq!(
            &*writer.published.lock(),
            &[b"0123".to_vec(), b"45".to_vec()]
        );
    }

    #[tokio::test]
    async fn a_dry_arena_is_retried_on_a_later_pass_without_spinning() {
        let (mut io, handle) = pair();
        let (writer, signal) = bind_batching(&handle, 64);
        writer.dry.store(true, Ordering::SeqCst);
        let mut cx = noop_cx();
        assert!(write_once(&mut io, b"later").await.is_pending());
        assert!(signal.poll(&mut cx).is_pending());
        assert!(handle.take_dirty());
        // A pass with nothing to publish wakes the writer to try again.
        assert_eq!(handle.pump().stats.retries, 1);
        writer.dry.store(false, Ordering::SeqCst);
        io.write_all(b"later").await.unwrap();
        handle.pump();
        assert_eq!(&*writer.published.lock(), &[b"later".to_vec()]);
    }

    #[tokio::test]
    async fn a_refused_flush_keeps_the_batch_and_reports_blocked() {
        let (mut io, handle) = pair();
        let (writer, _) = bind_batching(&handle, 64);
        io.write_all(b"held").await.unwrap();
        writer.refuse_flush.store(true, Ordering::SeqCst);
        let pumped = handle.pump();
        assert!(pumped.blocked);
        assert_eq!(pumped.stats.retries, 1);
        assert_eq!(pumped.stats.accepted, 0);
        assert!(writer.published.lock().is_empty());
        writer.refuse_flush.store(false, Ordering::SeqCst);
        let pumped = handle.pump();
        assert!(!pumped.blocked);
        assert_eq!(pumped.stats.accepted, 4);
        assert_eq!(&*writer.published.lock(), &[b"held".to_vec()]);
    }

    #[tokio::test]
    async fn shutdown_finishes_only_after_the_batch_is_published() {
        let (mut io, handle) = pair();
        let (writer, signal) = bind_batching(&handle, 64);
        io.write_all(b"last").await.unwrap();
        let mut cx = noop_cx();
        assert!(signal.poll(&mut cx).is_ready());
        io.shutdown().await.unwrap();
        assert!(
            signal.poll(&mut cx).is_ready(),
            "shutdown summons the driver"
        );
        assert!(!handle.tx_finished(), "buffered DATA precedes the FIN");
        let pumped = handle.pump();
        assert!(pumped.state.tx_finished);
        assert!(!pumped.state.tx_aborted);
        assert_eq!(&*writer.published.lock(), &[b"last".to_vec()]);
        assert_eq!(
            io.write_all(b"late").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
    }

    #[tokio::test]
    async fn dropping_the_endpoint_reports_an_abort() {
        let (io, handle) = pair();
        let (_, signal) = bind_batching(&handle, 64);
        drop(io);
        let mut cx = noop_cx();
        assert!(signal.poll(&mut cx).is_ready());
        assert!(handle.take_dirty());
        assert!(handle.pump().state.tx_aborted);
    }

    #[tokio::test]
    async fn a_partial_write_owns_its_prefix_and_the_rest_follows() {
        let (mut io, handle) = pair();
        let (writer, _) = bind_batching(&handle, 4);
        let mut source = b"0123456789".to_vec();
        assert!(matches!(
            write_once(&mut io, &source).await,
            Poll::Ready(Ok(4))
        ));
        source[..4].fill(99);
        handle.pump();
        let mut at = 4;
        while at < source.len() {
            match write_once(&mut io, &source[at..]).await {
                Poll::Ready(Ok(n)) => at += n,
                Poll::Pending => {
                    handle.pump();
                }
                Poll::Ready(Err(e)) => panic!("{e}"),
            }
        }
        handle.pump();
        assert_eq!(
            writer.published.lock().concat(),
            b"0123456789",
            "the accepted prefix survives the caller's overwrite"
        );
    }

    #[tokio::test]
    async fn a_refused_write_keeps_no_caller_bytes() {
        let (mut io, handle) = pair();
        assert!(write_once(&mut io, b"unbound").await.is_pending());
        let (writer, _) = bind_batching(&handle, 64);
        writer.dry.store(true, Ordering::SeqCst);
        assert!(write_once(&mut io, b"refused").await.is_pending());
        writer.dry.store(false, Ordering::SeqCst);
        handle.pump();
        io.write_all(b"replacement").await.unwrap();
        io.shutdown().await.unwrap();
        let pumped = handle.pump();
        assert!(pumped.state.tx_finished && !pumped.state.tx_aborted);
        assert_eq!(&*writer.published.lock(), &[b"replacement".to_vec()]);
    }

    #[tokio::test]
    async fn empty_write_never_reaches_the_writer() {
        let (mut io, handle) = pair();
        let (writer, signal) = bind_batching(&handle, 64);
        io.write_all(b"").await.unwrap();
        assert!(matches!(write_once(&mut io, b"").await, Poll::Ready(Ok(0))));
        assert_eq!(writer.calls.load(Ordering::SeqCst), 0);
        assert!(!handle.take_dirty());
        assert!(signal.poll(&mut noop_cx()).is_pending());
    }

    #[test]
    fn wrong_thread_never_enters_writer() {
        let (mut io, handle) = pair();
        let (writer, _) = bind_batching(&handle, 64);
        std::thread::spawn(move || {
            let mut cx = noop_cx();
            assert!(matches!(
                Pin::new(&mut io).poll_write(&mut cx, b"wrong"),
                Poll::Ready(Err(_))
            ));
        })
        .join()
        .unwrap();
        assert_eq!(writer.calls.load(Ordering::SeqCst), 0);
        assert!(handle.drain_state().tx_aborted);
        assert!(handle.take_dirty());
    }

    #[tokio::test]
    async fn writer_runs_under_the_endpoint_lock() {
        struct Locked(Arc<Shared>);
        impl TxWriter for Locked {
            fn write(&mut self, _: &BackendRoute, bufs: &[io::IoSlice<'_>]) -> TxAttempt {
                assert!(
                    self.0.inner.try_lock().is_none(),
                    "publication runs with the endpoint lock held"
                );
                TxAttempt::Buffered(bufs[0].len())
            }
        }
        let (mut io, handle) = pair();
        handle.bind_writer(
            Box::new(Locked(handle.shared.clone())),
            Arc::new(DriverSignal::default()),
            false,
        );
        io.write_all(b"lock-held").await.unwrap();
    }
}
