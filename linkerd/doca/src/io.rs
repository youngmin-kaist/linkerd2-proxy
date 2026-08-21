//! `DmeshIo`: an `AsyncRead + AsyncWrite` endpoint backed by DMesh DMA
//! buffers instead of a TCP socket.
//!
//! Received bytes are read **zero-copy** directly out of the per-connection DMA
//! staging buffer: the driver pushes `(pos, len)` segments (offsets into the
//! mapped staging region) via [`DmeshIoHandle::push_segment`], and
//! [`DmeshIo::poll_read`] copies straight from `staging_base + pos` into the
//! caller's `ReadBuf` (the single copy every `AsyncRead` performs; no extra
//! intermediate buffer on the DPU). The stack's writes are collected in `tx`
//! and picked up by the driver via [`DmeshIoHandle::take_tx`].
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
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

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

/// Default cap on buffered-but-unsent bytes before writers see backpressure.
const DEFAULT_TX_CAPACITY: usize = 256 * 1024;

/// How large the consumed prefix of `tx` may grow before it is reclaimed. One
/// publication consumes at most an egress chunk, so a smaller bound would move
/// the queue tail on nearly every publication, which is the copy the cursor
/// exists to avoid.
const TX_COMPACT_THRESHOLD: usize = 64 * 1024;

/// Every `DmeshIo`/`DmeshIoHandle` operation takes the same per-connection lock.
fn lock(inner: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    inner.lock()
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

    /// Bytes written by the stack, awaiting pickup by the driver. The queue is
    /// `tx[tx_read..]`; everything before the cursor has been published, and
    /// that prefix is reclaimed in one move once it reaches
    /// `TX_COMPACT_THRESHOLD`.
    tx: Vec<u8>,
    tx_read: usize,
    /// Local half shut down: further writes fail.
    tx_closed: bool,
    /// The stack dropped the endpoint without an orderly poll_shutdown.
    tx_aborted: bool,
    tx_waker: Option<Waker>,
    tx_capacity: usize,

    /// Driver-side waker: signalled when new tx bytes (or shutdown) appear.
    driver_waker: Option<Waker>,
}

impl Inner {
    fn wake_reader(&mut self) {
        if let Some(w) = self.rx_waker.take() {
            w.wake();
        }
    }
    fn wake_writer(&mut self) {
        if let Some(w) = self.tx_waker.take() {
            w.wake();
        }
    }
    fn wake_driver(&mut self) {
        if let Some(w) = self.driver_waker.take() {
            w.wake();
        }
    }
    fn rx_has_data(&self) -> bool {
        !self.rx.is_empty() || !self.segs.is_empty()
    }

    /// Output the driver has not published yet.
    fn tx_pending(&self) -> &[u8] {
        &self.tx[self.tx_read..]
    }

    fn tx_queued(&self) -> usize {
        self.tx.len() - self.tx_read
    }

    /// Advance the cursor over `n` published bytes and reclaim the prefix once
    /// it is worth moving what is left.
    fn tx_consume(&mut self, n: usize) {
        self.tx_read += n;
        if self.tx_read == self.tx.len() {
            self.tx.clear();
            self.tx_read = 0;
        } else if self.tx_read >= TX_COMPACT_THRESHOLD {
            self.tx.drain(..self.tx_read);
            self.tx_read = 0;
        }
    }

    fn tx_clear(&mut self) {
        self.tx.clear();
        self.tx_read = 0;
    }
}

/// Stack-facing endpoint (the `TcpStream` analogue).
pub struct DmeshIo {
    inner: Arc<Mutex<Inner>>,
    peer: SocketAddr,
    /// The DMA session this endpoint belongs to, carried so the outbound
    /// connector can resolve the session's backend channel from the
    /// connection itself rather than from stack configuration.
    session: Option<SessionToken>,
}

/// Driver-facing endpoint bridging the DMA staging buffer to the stack.
pub struct DmeshIoHandle {
    inner: Arc<Mutex<Inner>>,
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
    let inner = Arc::new(Mutex::new(Inner {
        tx_capacity: DEFAULT_TX_CAPACITY,
        ..Inner::default()
    }));
    (
        DmeshIo {
            inner: inner.clone(),
            peer,
            session,
        },
        DmeshIoHandle { inner },
    )
}

impl fmt::Debug for DmeshIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DmeshIo").field("peer", &self.peer).finish()
    }
}

impl DmeshIo {
    pub(crate) fn set_backend_route(&self, route: BackendRoute) {
        lock(&self.inner).backend_route = route;
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
        let mut inner = lock(&self.inner);

        // Owned bytes first (copy path).
        if !inner.rx.is_empty() {
            let n = buf.remaining().min(inner.rx.len());
            buf.put_slice(&inner.rx[..n]);
            inner.rx.drain(..n);
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
            }
            return Poll::Ready(Ok(()));
        }

        if inner.rx_closed {
            return Poll::Ready(Ok(())); // EOF
        }
        inner.rx_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl AsyncWrite for DmeshIo {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let mut inner = lock(&self.inner);
        if inner.tx_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "dmesh connection is shut down",
            )));
        }
        let room = inner.tx_capacity.saturating_sub(inner.tx_queued());
        if room == 0 {
            inner.tx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n = data.len().min(room);
        inner.tx.extend_from_slice(&data[..n]);
        inner.wake_driver();
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut inner = lock(&self.inner);
        inner.tx_closed = true;
        inner.wake_driver();
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
        let mut inner = lock(&self.inner);
        if !inner.tx_closed {
            inner.tx_aborted = true;
        }
        inner.tx_closed = true;
        inner.wake_driver();
    }
}

impl DmeshIoHandle {
    /// Linkerd's backend choice, installed before the connector hands the IO
    /// to the stack.
    pub fn backend_route(&self) -> BackendRoute {
        lock(&self.inner).backend_route.clone()
    }

    /// Point the reader at the connection's mapped DMA staging region. Must be
    /// set before any `push_segment`.
    pub fn set_staging(&self, base_addr: usize, len: usize) {
        let mut inner = lock(&self.inner);
        inner.staging_base = base_addr;
        inner.staging_len = len;
    }

    /// Deliver a completed recv segment `[pos, pos+len)` in the staging region
    /// to the reading stack (zero-copy: no bytes are moved here).
    pub fn push_segment(&self, pos: u32, len: u32) {
        let mut inner = lock(&self.inner);
        inner.segs.push_back((pos, len));
        inner.wake_reader();
    }

    /// Deliver owned bytes to the reading stack (copy path; tests / non-DMA).
    pub fn push_rx(&self, bytes: &[u8]) {
        let mut inner = lock(&self.inner);
        inner.rx.extend_from_slice(bytes);
        inner.wake_reader();
    }

    /// Signal peer half-close: pending data drains, then reads return EOF.
    pub fn close_rx(&self) {
        let mut inner = lock(&self.inner);
        inner.rx_closed = true;
        inner.wake_reader();
    }

    /// Abort both halves and discard all buffered data.
    ///
    /// The driver must call this before returning custody for unread DMA
    /// segments. Once this returns, no stack task can retain a reference to a
    /// queued staging segment or enqueue more output.
    pub fn abort(&self) {
        let mut inner = lock(&self.inner);
        inner.rx.clear();
        inner.segs.clear();
        inner.seg_read_off = 0;
        inner.staging_base = 0;
        inner.staging_len = 0;
        inner.rx_closed = true;
        inner.tx_clear();
        inner.tx_closed = true;
        inner.tx_aborted = true;
        inner.wake_reader();
        inner.wake_writer();
        inner.wake_driver();
    }

    /// Copy up to `out.len()` queued output bytes into `out` without consuming
    /// them, and answer how many were copied.
    ///
    /// This is the reservation path's read: the caller copies straight into the
    /// egress arena and consumes only the prefix the datapath accepted, so a
    /// refused reservation leaves the queue exactly as it was.
    pub fn copy_tx_into(&self, out: &mut [u8]) -> usize {
        let inner = lock(&self.inner);
        let pending = inner.tx_pending();
        let n = pending.len().min(out.len());
        out[..n].copy_from_slice(&pending[..n]);
        n
    }

    /// Drop the first `n` output bytes, which the datapath has accepted.
    pub fn consume_tx(&self, n: usize) {
        if n == 0 {
            return;
        }
        let mut inner = lock(&self.inner);
        let n = n.min(inner.tx_queued());
        inner.tx_consume(n);
        inner.wake_writer();
    }

    /// Queued output bytes.
    pub fn tx_len(&self) -> usize {
        lock(&self.inner).tx_queued()
    }

    /// Take up to `max` bytes written by the stack (to post as DMA sends).
    pub fn take_tx(&self, max: usize) -> Vec<u8> {
        let mut inner = lock(&self.inner);
        let n = inner.tx_queued().min(max);
        let out = inner.tx_pending()[..n].to_vec();
        inner.tx_consume(n);
        if n > 0 {
            inner.wake_writer();
        }
        out
    }

    /// Return unsent bytes to the front of the tx queue (the DMA ring was full,
    /// or the reverse path was not ready). They are re-sent on a later tick.
    pub fn untake_tx(&self, data: &[u8]) {
        if data.is_empty() {
            return;
        }
        let mut inner = lock(&self.inner);
        // The bytes came off the front of this queue, so the room they left is
        // still in front of the cursor unless it was reclaimed meanwhile.
        if data.len() <= inner.tx_read {
            let at = inner.tx_read - data.len();
            let end = inner.tx_read;
            inner.tx[at..end].copy_from_slice(data);
            inner.tx_read = at;
            return;
        }
        let mut merged = Vec::with_capacity(data.len() + inner.tx_queued());
        merged.extend_from_slice(data);
        merged.extend_from_slice(inner.tx_pending());
        inner.tx = merged;
        inner.tx_read = 0;
    }

    /// True once the stack shut down its write half and tx is fully drained.
    pub fn tx_finished(&self) -> bool {
        let inner = lock(&self.inner);
        inner.tx_closed && inner.tx_queued() == 0
    }

    /// True while the reader still has undelivered/undrained data.
    pub fn has_rx(&self) -> bool {
        lock(&self.inner).rx_has_data()
    }

    /// Both answers a drain pass needs about an endpoint once it has published,
    /// under one lock: a pass asks them together and they are read together.
    pub fn drain_state(&self) -> DrainState {
        let inner = lock(&self.inner);
        DrainState {
            has_rx: inner.rx_has_data(),
            tx_finished: inner.tx_closed && inner.tx_queued() == 0,
            tx_aborted: inner.tx_aborted,
        }
    }

    /// Poll for bytes waiting in the tx queue.
    pub fn poll_tx_ready(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = lock(&self.inner);
        if inner.tx_queued() > 0 {
            return Poll::Ready(());
        }
        inner.driver_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::poll_fn;
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
        io.write_all(b"unsent").await.unwrap();

        handle.abort();

        let mut buf = [0u8; 8];
        assert_eq!(io.read(&mut buf).await.unwrap(), 0);
        assert_eq!(
            io.write_all(b"late").await.unwrap_err().kind(),
            io::ErrorKind::BrokenPipe
        );
        assert!(handle.take_tx(usize::MAX).is_empty());
        assert!(handle.tx_finished());
    }

    #[tokio::test]
    async fn zero_copy_staging_read() {
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

        let mut out = Vec::new();
        io.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"GET / HTTP/1.1\r\n\r\n");
    }

    #[tokio::test]
    async fn peer_addr_reports_flow_src() {
        let (io, _h) = pair();
        assert_eq!(io.peer_addr().unwrap(), "127.0.0.1:40000".parse().unwrap());
    }

    #[tokio::test]
    async fn copy_tx_leaves_the_queue_until_it_is_consumed() {
        let (mut io, handle) = pair();
        io.write_all(b"0123456789").await.unwrap();

        let mut out = [0u8; 4];
        assert_eq!(handle.copy_tx_into(&mut out), 4);
        assert_eq!(&out, b"0123");
        // A refused reservation consumes nothing.
        handle.consume_tx(0);
        assert_eq!(handle.tx_len(), 10);

        handle.consume_tx(4);
        assert_eq!(handle.tx_len(), 6);
        let mut rest = [0u8; 16];
        assert_eq!(handle.copy_tx_into(&mut rest), 6);
        assert_eq!(&rest[..6], b"456789");

        handle.consume_tx(usize::MAX);
        assert_eq!(handle.tx_len(), 0);
        assert_eq!(handle.copy_tx_into(&mut rest), 0);
    }

    #[tokio::test]
    async fn a_requeued_suffix_returns_to_the_front_in_order() {
        let (mut io, handle) = pair();
        io.write_all(b"0123456789").await.unwrap();

        let taken = handle.take_tx(6);
        assert_eq!(taken, b"012345");
        // The datapath accepted only "012"; the rest goes back ahead of "6789".
        handle.untake_tx(&taken[3..]);
        assert_eq!(handle.tx_len(), 7);
        assert_eq!(handle.take_tx(usize::MAX), b"3456789");
        assert_eq!(handle.tx_len(), 0);
    }

    #[tokio::test]
    async fn the_consumed_prefix_does_not_count_against_capacity() {
        let (mut io, handle) = pair();
        // Fill the queue, then publish more than the compaction threshold so
        // the cursor has run well ahead of the buffer's start.
        let big = vec![7u8; super::DEFAULT_TX_CAPACITY];
        io.write_all(&big).await.unwrap();
        let published = super::TX_COMPACT_THRESHOLD / 2;
        let mut sink = vec![0u8; published];
        assert_eq!(handle.copy_tx_into(&mut sink), published);
        assert!(sink.iter().all(|&b| b == 7));
        handle.consume_tx(published);
        assert_eq!(handle.tx_len(), super::DEFAULT_TX_CAPACITY - published);

        // Room freed by publication is room a writer may use again.
        io.write_all(&vec![9u8; published]).await.unwrap();
        assert_eq!(handle.tx_len(), super::DEFAULT_TX_CAPACITY);

        let drained = handle.take_tx(usize::MAX);
        assert_eq!(drained.len(), super::DEFAULT_TX_CAPACITY);
        assert_eq!(
            &drained[drained.len() - published..],
            &vec![9u8; published][..]
        );
        assert_eq!(handle.tx_len(), 0);
    }

    #[tokio::test]
    async fn write_reaches_driver_and_backpressure_releases() {
        let (mut io, handle) = pair();

        io.write_all(b"ping").await.unwrap();
        poll_fn(|cx| handle.poll_tx_ready(cx)).await;
        assert_eq!(handle.take_tx(usize::MAX), b"ping");

        let big = vec![0u8; super::DEFAULT_TX_CAPACITY];
        io.write_all(&big).await.unwrap();

        let mut blocked = tokio::spawn(async move {
            io.write_all(b"x").await.unwrap();
            io
        });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished());

        let drained = handle.take_tx(usize::MAX);
        assert_eq!(drained.len(), super::DEFAULT_TX_CAPACITY);

        let mut io = (&mut blocked).await.unwrap();
        assert_eq!(handle.take_tx(usize::MAX), b"x");

        io.shutdown().await.unwrap();
        assert!(handle.tx_finished());
    }
}
