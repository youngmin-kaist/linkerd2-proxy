//! `DmeshIo`: an `AsyncRead + AsyncWrite` endpoint backed by DMesh DMA
//! buffers instead of a TCP socket.
//!
//! Received bytes are read **zero-copy** directly out of the per-connection DMA
//! staging buffer: the driver pushes `(pos, len)` segments (offsets into the
//! mapped staging region) via [`DmeshIoHandle::push_segment`], and
//! [`DmeshIo::poll_read`] copies straight from `staging_base + pos` into the
//! caller's `ReadBuf` (the single copy every `AsyncRead` performs; no extra
//! intermediate buffer on the DPU).
//!
//! Written bytes are likewise **staged in place**: `poll_write` copies the
//! stack's bytes once, directly into the connection's mapped `tx_staging`
//! region at a write cursor, and the driver publishes the accumulated
//! `[publish, write)` range as DMA descriptors ([`DmeshIoHandle::take_staged`]
//! / [`advance_publish`]). This replaces the old three-copy pipeline
//! (stack -> tx Vec -> take Vec -> memcpy into staging) with a single copy and
//! no per-tick heap allocation. Small writes (h2 frame headers etc.) coalesce
//! in staging and are published in batches, so descriptor efficiency matches
//! the old Vec-batching path.
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
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use linkerd_io::{Peek, PeerAddr};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

#[derive(Default)]
struct Inner {
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

    /// TX staging (write-side zero-copy): the connection's mapped tx_staging
    /// region. The stack writes at `tx_write`, the driver publishes
    /// `[tx_publish, tx_write)` as DMA work. Cursors are cumulative (u64), the
    /// physical offset is `cursor % tx_len`; unpublished bytes are never
    /// overwritten (`room = tx_len - (tx_write - tx_publish)`).
    tx_base: usize,
    tx_len: usize,
    tx_write: u64,
    tx_publish: u64,
    /// Connection torn down: the staging pointer is no longer valid.
    tx_dead: bool,

    /// Local half shut down: further writes fail.
    tx_closed: bool,
    tx_waker: Option<Waker>,

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
    fn tx_unpublished(&self) -> u64 {
        self.tx_write - self.tx_publish
    }
}

/// Stack-facing endpoint (the `TcpStream` analogue).
pub struct DmeshIo {
    inner: Arc<Mutex<Inner>>,
    peer: SocketAddr,
}

/// Driver-facing endpoint bridging the DMA staging buffers to the stack.
pub struct DmeshIoHandle {
    inner: Arc<Mutex<Inner>>,
}

// SAFETY: `staging_base` / `tx_base` are raw addresses into DMA regions that
// outlive the connection (`clear_tx_staging` marks tx dead before the C side
// frees it); access is serialized by the `Mutex`, with a single logical reader
// and a single logical writer.
unsafe impl Send for DmeshIo {}
unsafe impl Sync for DmeshIo {}
unsafe impl Send for DmeshIoHandle {}
unsafe impl Sync for DmeshIoHandle {}

/// Create a connected pair. The driver keeps the handle, the stack gets the IO.
/// `peer` is the flow's source address, reported via `PeerAddr`.
pub fn dmesh_io_pair(peer: SocketAddr) -> (DmeshIo, DmeshIoHandle) {
    let inner = Arc::new(Mutex::new(Inner::default()));
    (
        DmeshIo {
            inner: inner.clone(),
            peer,
        },
        DmeshIoHandle { inner },
    )
}

impl fmt::Debug for DmeshIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DmeshIo").field("peer", &self.peer).finish()
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
        let mut inner = self.inner.lock().unwrap();

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
        let mut inner = self.inner.lock().unwrap();
        if inner.tx_closed || inner.tx_dead {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "dmesh connection is shut down",
            )));
        }
        // The tx staging region appears once the reverse path is set up; until
        // then writers wait (for client channels that is the same driver tick
        // that made the connection ready).
        if inner.tx_len == 0 {
            inner.tx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let room = inner.tx_len as u64 - inner.tx_unpublished();
        if room == 0 {
            inner.tx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n = (data.len() as u64).min(room) as usize;
        let off = (inner.tx_write % inner.tx_len as u64) as usize;
        let contiguous = inner.tx_len - off;
        let first = n.min(contiguous);
        let base = inner.tx_base as *mut u8;
        // SAFETY: [off, off+first) and (on wrap) [0, n-first) lie inside the
        // mapped tx_staging region of `tx_len` bytes; the room check above
        // guarantees these bytes are not part of the unpublished window, and
        // the mutex serializes all access. The region stays alive until
        // `clear_tx_staging` sets `tx_dead` (checked above under this lock).
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), base.add(off), first);
            if first < n {
                std::ptr::copy_nonoverlapping(data.as_ptr().add(first), base, n - first);
            }
        }
        inner.tx_write += n as u64;
        inner.wake_driver();
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut inner = self.inner.lock().unwrap();
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

impl PeerAddr for DmeshIo {
    fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.peer)
    }
}

impl Drop for DmeshIo {
    fn drop(&mut self) {
        let mut inner = self.inner.lock().unwrap();
        inner.tx_closed = true;
        inner.wake_driver();
    }
}

impl DmeshIoHandle {
    /// Point the reader at the connection's mapped DMA staging region. Must be
    /// set before any `push_segment`.
    pub fn set_staging(&self, base_addr: usize, len: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.staging_base = base_addr;
        inner.staging_len = len;
    }

    /// Point the writer at the connection's mapped tx_staging region (usable
    /// length, i.e. minus any C-side reserved tail). Wakes writers that were
    /// waiting for the reverse path to come up.
    pub fn set_tx_staging(&self, base_addr: usize, len: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.tx_base = base_addr;
        inner.tx_len = len;
        inner.wake_writer();
    }

    /// The connection is being torn down: the tx staging pointer is about to
    /// be freed. Further writes fail; the driver stops publishing.
    pub fn clear_tx_staging(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.tx_dead = true;
        inner.tx_len = 0;
        inner.wake_writer();
        inner.wake_driver();
    }

    /// Deliver a completed recv segment `[pos, pos+len)` in the staging region
    /// to the reading stack (zero-copy: no bytes are moved here).
    pub fn push_segment(&self, pos: u32, len: u32) {
        let mut inner = self.inner.lock().unwrap();
        inner.segs.push_back((pos, len));
        inner.wake_reader();
    }

    /// Deliver owned bytes to the reading stack (copy path; tests / non-DMA).
    pub fn push_rx(&self, bytes: &[u8]) {
        let mut inner = self.inner.lock().unwrap();
        inner.rx.extend_from_slice(bytes);
        inner.wake_reader();
    }

    /// Signal peer half-close: pending data drains, then reads return EOF.
    pub fn close_rx(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.rx_closed = true;
        inner.wake_reader();
    }

    /// Next contiguous unpublished run `[pos, pos+len)` in tx staging, if any.
    /// Runs never cross the buffer wrap; call again after `advance_publish` to
    /// pick up the wrapped remainder. Does not consume - the driver reports
    /// what the DMA layer actually accepted via [`advance_publish`].
    pub fn take_staged(&self) -> Option<(u32, u32)> {
        let inner = self.inner.lock().unwrap();
        if inner.tx_dead || inner.tx_len == 0 {
            return None;
        }
        let unpublished = inner.tx_unpublished();
        if unpublished == 0 {
            return None;
        }
        let off = (inner.tx_publish % inner.tx_len as u64) as usize;
        let run = (unpublished as usize).min(inner.tx_len - off);
        Some((off as u32, run as u32))
    }

    /// Mark `n` staged bytes as published (queued for / covered by DMA); frees
    /// writer room.
    pub fn advance_publish(&self, n: u32) {
        if n == 0 {
            return;
        }
        let mut inner = self.inner.lock().unwrap();
        debug_assert!(n as u64 <= inner.tx_unpublished());
        inner.tx_publish += n as u64;
        inner.wake_writer();
    }

    /// True once the stack shut down its write half and staging fully drained.
    pub fn tx_finished(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.tx_closed && inner.tx_unpublished() == 0
    }

    /// True while the reader still has undelivered/undrained data.
    pub fn has_rx(&self) -> bool {
        self.inner.lock().unwrap().rx_has_data()
    }

    /// Poll-style wait for unpublished tx bytes (or shutdown); used by the
    /// driver's tx-wake select arm.
    pub fn poll_tx_ready(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.inner.lock().unwrap();
        if inner.tx_unpublished() > 0 || inner.tx_closed {
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
        dmesh_io_pair("127.0.0.1:40000".parse().unwrap())
    }

    /// A fake tx staging region (stands in for the mapped DMA buffer).
    fn tx_staging(handle: &DmeshIoHandle, len: usize) -> &'static mut [u8] {
        let buf: &'static mut [u8] = Box::leak(vec![0u8; len].into_boxed_slice());
        handle.set_tx_staging(buf.as_ptr() as usize, len);
        buf
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
    async fn zero_copy_staging_read() {
        // Simulate a staging region: a leaked buffer the driver "DMA'd" into.
        let staging: &'static [u8] =
            Box::leak(b"....GET / HTTP/1.1\r\n\r\nXXXX".to_vec().into_boxed_slice());
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
    async fn write_pending_until_tx_staging_set() {
        let (mut io, handle) = pair();

        let mut w = tokio::spawn(async move {
            io.write_all(b"early").await.unwrap();
            io
        });
        tokio::task::yield_now().await;
        assert!(!w.is_finished(), "write must wait for tx staging");

        let buf = tx_staging(&handle, 64);
        let _io = (&mut w).await.unwrap();
        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (0, 5));
        assert_eq!(&buf[..5], b"early");
    }

    #[tokio::test]
    async fn staged_write_take_advance() {
        let (mut io, handle) = pair();
        let buf = tx_staging(&handle, 64);

        io.write_all(b"ping").await.unwrap();
        poll_fn(|cx| handle.poll_tx_ready(cx)).await;

        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (0, 4));
        assert_eq!(&buf[..4], b"ping");
        handle.advance_publish(4);
        assert!(handle.take_staged().is_none());

        // Small writes coalesce into one contiguous run.
        io.write_all(b"ab").await.unwrap();
        io.write_all(b"cd").await.unwrap();
        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (4, 4));
        assert_eq!(&buf[4..8], b"abcd");
        handle.advance_publish(4);
    }

    #[tokio::test]
    async fn wrap_splits_published_runs() {
        let (mut io, handle) = pair();
        let buf = tx_staging(&handle, 8);

        io.write_all(b"abcdef").await.unwrap(); // [0..6)
        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (0, 6));
        handle.advance_publish(6);

        // 6 more bytes: 2 fit before the end, 4 wrap to the front.
        io.write_all(b"ghijkl").await.unwrap();
        assert_eq!(&buf[6..8], b"gh");
        assert_eq!(&buf[..4], b"ijkl");

        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (6, 2)); // run stops at the wrap
        handle.advance_publish(2);
        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (0, 4)); // wrapped remainder
        handle.advance_publish(4);
        assert!(handle.take_staged().is_none());
    }

    #[tokio::test]
    async fn backpressure_until_publish_frees_room() {
        let (mut io, handle) = pair();
        let _buf = tx_staging(&handle, 8);

        io.write_all(b"12345678").await.unwrap(); // fills the region

        let mut blocked = tokio::spawn(async move {
            io.write_all(b"x").await.unwrap();
            io
        });
        tokio::task::yield_now().await;
        assert!(!blocked.is_finished(), "full staging must backpressure");

        let (_, len) = handle.take_staged().unwrap();
        handle.advance_publish(len); // frees the whole region

        let mut io = (&mut blocked).await.unwrap();
        let (pos, len) = handle.take_staged().unwrap();
        assert_eq!((pos, len), (0, 1)); // wrapped to the front

        handle.advance_publish(1);
        io.shutdown().await.unwrap();
        assert!(handle.tx_finished());
    }

    #[tokio::test]
    async fn cleared_staging_fails_writes() {
        let (mut io, handle) = pair();
        let _buf = tx_staging(&handle, 16);
        io.write_all(b"ok").await.unwrap();
        handle.advance_publish(2);

        handle.clear_tx_staging();
        assert!(io.write_all(b"nope").await.is_err());
        assert!(handle.take_staged().is_none());
    }
}
