//! `DmeshIo`: an `AsyncRead + AsyncWrite` endpoint backed by DMesh DMA
//! buffers instead of a TCP socket.
//!
//! P1 scaffolding: the byte transport is a pair of in-memory queues with
//! correct waker semantics and write backpressure. The driver side holds a
//! [`DmeshIoHandle`] and bridges it to the per-connection DMA rings (P2):
//! frames received from the host are pushed with `push_rx`, and bytes the
//! stack wrote are drained with `take_tx` and posted as DMA descriptors.
//! Because linkerd stacks are generic over `I: AsyncRead + AsyncWrite`, this
//! type is what lets a DMA-backed connection flow through them unchanged.

use std::io;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Default cap on buffered-but-unsent bytes before writers see backpressure.
const DEFAULT_TX_CAPACITY: usize = 256 * 1024;

#[derive(Default)]
struct Inner {
    /// Bytes received from the peer (filled by the driver via push_rx)
    rx: Vec<u8>,
    /// Peer half closed: reads drain rx then return EOF
    rx_closed: bool,
    rx_waker: Option<Waker>,

    /// Bytes written by the stack, awaiting pickup by the driver
    tx: Vec<u8>,
    /// Local half shut down: further writes fail
    tx_closed: bool,
    tx_waker: Option<Waker>,
    tx_capacity: usize,

    /// Driver-side waker: signalled when new tx bytes (or shutdown) appear
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
}

/// Stack-facing endpoint (the `TcpStream` analogue).
pub struct DmeshIo {
    inner: Arc<Mutex<Inner>>,
}

/// Driver-facing endpoint bridging the byte queues to the DMA rings.
pub struct DmeshIoHandle {
    inner: Arc<Mutex<Inner>>,
}

/// Create a connected pair. The driver keeps the handle, the stack gets the IO.
pub fn dmesh_io_pair() -> (DmeshIo, DmeshIoHandle) {
    let inner = Arc::new(Mutex::new(Inner {
        tx_capacity: DEFAULT_TX_CAPACITY,
        ..Inner::default()
    }));
    (
        DmeshIo {
            inner: inner.clone(),
        },
        DmeshIoHandle { inner },
    )
}

impl AsyncRead for DmeshIo {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.rx.is_empty() {
            if inner.rx_closed {
                return Poll::Ready(Ok(())); // EOF
            }
            inner.rx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n = buf.remaining().min(inner.rx.len());
        buf.put_slice(&inner.rx[..n]);
        inner.rx.drain(..n);
        Poll::Ready(Ok(()))
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
            // Backpressure: woken by the driver when it drains tx.
            inner.tx_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }

        let n = data.len().min(room);
        inner.tx.extend_from_slice(&data[..n]);
        inner.wake_driver();
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // Bytes are visible to the driver as soon as poll_write returns; the
        // DMA commit itself is asynchronous and covered by write backpressure.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut inner = self.inner.lock().unwrap();
        inner.tx_closed = true;
        inner.wake_driver();
        Poll::Ready(Ok(()))
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
    /// Deliver bytes received from the peer to the reading stack.
    pub fn push_rx(&self, bytes: &[u8]) {
        let mut inner = self.inner.lock().unwrap();
        inner.rx.extend_from_slice(bytes);
        inner.wake_reader();
    }

    /// Signal peer half-close: pending bytes drain, then reads return EOF.
    pub fn close_rx(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.rx_closed = true;
        inner.wake_reader();
    }

    /// Take up to `max` bytes written by the stack (to post as DMA sends).
    /// Waking the writer here is what releases write backpressure.
    pub fn take_tx(&self, max: usize) -> Vec<u8> {
        let mut inner = self.inner.lock().unwrap();
        let n = inner.tx.len().min(max);
        let out: Vec<u8> = inner.tx.drain(..n).collect();
        if n > 0 {
            inner.wake_writer();
        }
        out
    }

    /// True once the stack shut down its write half and tx is fully drained.
    pub fn tx_finished(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.tx_closed && inner.tx.is_empty()
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

    #[tokio::test]
    async fn read_waits_then_delivers() {
        let (mut io, handle) = dmesh_io_pair();

        let reader = tokio::spawn(async move {
            let mut buf = [0u8; 16];
            let n = io.read(&mut buf).await.unwrap();
            buf[..n].to_vec()
        });

        // Give the reader a chance to park on the rx waker first.
        tokio::task::yield_now().await;
        handle.push_rx(b"hello dma");

        assert_eq!(reader.await.unwrap(), b"hello dma");
    }

    #[tokio::test]
    async fn eof_after_close() {
        let (mut io, handle) = dmesh_io_pair();
        handle.push_rx(b"tail");
        handle.close_rx();

        let mut out = Vec::new();
        io.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, b"tail");
    }

    #[tokio::test]
    async fn write_reaches_driver_and_backpressure_releases() {
        let (mut io, handle) = dmesh_io_pair();

        io.write_all(b"ping").await.unwrap();
        // Driver waits for tx bytes, then picks them up.
        poll_fn(|cx| handle.poll_tx_ready(cx)).await;
        assert_eq!(handle.take_tx(usize::MAX), b"ping");

        // Fill to capacity: the next write must block until the driver drains.
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

        // Shutdown propagates to the driver side.
        io.shutdown().await.unwrap();
        assert!(handle.tx_finished());
    }

    #[tokio::test]
    async fn echo_loopback_through_dmesh_io() {
        // The P1 "echo test": a driver-side loop reflecting tx back into rx,
        // exercising the full waker path a real DMA bridge will use.
        let (mut io, handle) = dmesh_io_pair();

        let echo = tokio::spawn(async move {
            loop {
                poll_fn(|cx| handle.poll_tx_ready(cx)).await;
                let bytes = handle.take_tx(4096);
                if !bytes.is_empty() {
                    handle.push_rx(&bytes);
                }
                if handle.tx_finished() {
                    handle.close_rx();
                    return;
                }
            }
        });

        for i in 0..100u32 {
            let msg = format!("frame-{i}");
            io.write_all(msg.as_bytes()).await.unwrap();
            let mut buf = vec![0u8; msg.len()];
            io.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, msg.as_bytes());
        }

        io.shutdown().await.unwrap();
        let mut rest = Vec::new();
        io.read_to_end(&mut rest).await.unwrap();
        assert!(rest.is_empty());
        drop(io);
        echo.await.unwrap();
    }
}
