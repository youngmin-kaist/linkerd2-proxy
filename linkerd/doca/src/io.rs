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
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use linkerd_io::{Peek, PeerAddr};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Default cap on buffered-but-unsent bytes before writers see backpressure.
const DEFAULT_TX_CAPACITY: usize = 256 * 1024;

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

    /// Bytes written by the stack, awaiting pickup by the driver.
    tx: Vec<u8>,
    /// Local half shut down: further writes fail.
    tx_closed: bool,
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
}

/// Stack-facing endpoint (the `TcpStream` analogue).
pub struct DmeshIo {
    inner: Arc<Mutex<Inner>>,
    peer: SocketAddr,
}

/// Driver-facing endpoint bridging the DMA staging buffer to the stack.
pub struct DmeshIoHandle {
    inner: Arc<Mutex<Inner>>,
}

// SAFETY: `staging_base` is a raw address into a DMA region that outlives the
// connection; it is only ever read (never freed) through `DmeshIo`, whose
// access is serialized by the `Mutex`. A single logical reader consumes it.
unsafe impl Send for DmeshIo {}
unsafe impl Sync for DmeshIo {}
unsafe impl Send for DmeshIoHandle {}
unsafe impl Sync for DmeshIoHandle {}

/// Create a connected pair. The driver keeps the handle, the stack gets the IO.
/// `peer` is the flow's source address, reported via `PeerAddr`.
pub fn dmesh_io_pair(peer: SocketAddr) -> (DmeshIo, DmeshIoHandle) {
    let inner = Arc::new(Mutex::new(Inner {
        tx_capacity: DEFAULT_TX_CAPACITY,
        ..Inner::default()
    }));
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
        if inner.tx_closed {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "dmesh connection is shut down",
            )));
        }
        let room = inner.tx_capacity.saturating_sub(inner.tx.len());
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

    /// Take up to `max` bytes written by the stack (to post as DMA sends).
    pub fn take_tx(&self, max: usize) -> Vec<u8> {
        let mut inner = self.inner.lock().unwrap();
        let n = inner.tx.len().min(max);
        let out: Vec<u8> = inner.tx.drain(..n).collect();
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
        let mut inner = self.inner.lock().unwrap();
        let mut merged = Vec::with_capacity(data.len() + inner.tx.len());
        merged.extend_from_slice(data);
        merged.append(&mut inner.tx);
        inner.tx = merged;
    }

    /// True once the stack shut down its write half and tx is fully drained.
    pub fn tx_finished(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.tx_closed && inner.tx.is_empty()
    }

    /// True while the reader still has undelivered/undrained data.
    pub fn has_rx(&self) -> bool {
        self.inner.lock().unwrap().rx_has_data()
    }

    /// Poll-style wait for new tx bytes (or shutdown); used by the driver.
    pub fn poll_tx_ready(&self, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.inner.lock().unwrap();
        if !inner.tx.is_empty() || inner.tx_closed {
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
        let staging: &'static [u8] = Box::leak(
            b"....GET / HTTP/1.1\r\n\r\nXXXX".to_vec().into_boxed_slice(),
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
