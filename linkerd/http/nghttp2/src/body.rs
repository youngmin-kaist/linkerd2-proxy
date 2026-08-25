//! Body plumbing between the nghttp2 session (driven by one connection task)
//! and the proxy stack (which may poll bodies from other tasks).
//!
//! Receive side: `RecvBody` is a shared-state channel body. DATA chunks are
//! copied out of nghttp2's callback buffer (whose lifetime is the callback)
//! into `Bytes` and queued; the consumer pops them from any task. Flow
//! control is explicit: bytes are only `nghttp2_session_consume`d after the
//! consumer actually reads them (credits reported back to the driver), so
//! WINDOW_UPDATEs reflect true end-to-end backpressure.
//!
//! Send side: `SendBody` is single-task state owned by the connection driver:
//! the driver polls the stack's `BoxBody` into a chunk queue, and the nghttp2
//! data-provider read callback drains it (deferring when empty).

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use futures::task::AtomicWaker;
use http::HeaderMap;
use http_body::Body as _;
use linkerd_http_box::BoxBody;

use crate::error::{Error, Reason};

#[derive(Default)]
struct RecvInner {
    chunks: VecDeque<Bytes>,
    buffered: usize,
    trailers: Option<HeaderMap>,
    eof: bool,
    error: Option<Error>,
    /// Consumer dropped the body before EOF.
    dropped: bool,
}

/// Per-connection rendezvous for receive-side flow control.
///
/// The driver used to walk every open stream each poll to collect consume
/// credits, calling `AtomicWaker::register` on each — O(open streams) waker
/// clones + CAS per poll iteration, which collapsed throughput once a client
/// kept a few hundred streams open (13.3k → 0.2k req/s at 300 streams).
/// Instead the consumer pushes its stream id here when it actually reads, and
/// the driver registers ONE waker and drains only those ids.
#[derive(Default)]
pub(crate) struct ConnRecv {
    waker: AtomicWaker,
    dirty: Mutex<Vec<i32>>,
}

impl ConnRecv {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(ConnRecv::default())
    }

    /// Register the driver's waker once and take the ids that consumed bytes.
    pub(crate) fn take_dirty(&self, cx: &mut Context<'_>, out: &mut Vec<i32>) {
        self.waker.register(cx.waker());
        let mut d = self.dirty.lock().unwrap();
        out.append(&mut d);
    }

    fn mark(&self, sid: i32) {
        self.dirty.lock().unwrap().push(sid);
        self.waker.wake();
    }
}

pub(crate) struct RecvShared {
    inner: Mutex<RecvInner>,
    /// Bytes read by the consumer, not yet reported to nghttp2 as consumed.
    /// Atomic (not behind the mutex) because the driver polls this for every
    /// open stream on every loop iteration — the common case is "zero, no
    /// body" and must cost one relaxed load, not a lock round trip.
    consumed_unreported: AtomicUsize,
    read_waker: AtomicWaker,
    conn: Arc<ConnRecv>,
    sid: i32,
}

impl RecvShared {
    pub(crate) fn new(conn: Arc<ConnRecv>, sid: i32) -> Arc<Self> {
        Arc::new(RecvShared {
            inner: Mutex::new(RecvInner::default()),
            consumed_unreported: AtomicUsize::new(0),
            read_waker: AtomicWaker::new(),
            conn,
            sid,
        })
    }

    // --- driver side ---

    pub(crate) fn push_chunk(&self, data: Bytes) {
        let mut i = self.inner.lock().unwrap();
        i.buffered += data.len();
        i.chunks.push_back(data);
        drop(i);
        self.read_waker.wake();
    }

    pub(crate) fn set_trailers(&self, trailers: HeaderMap) {
        self.inner.lock().unwrap().trailers = Some(trailers);
        self.read_waker.wake();
    }

    pub(crate) fn set_eof(&self) {
        self.inner.lock().unwrap().eof = true;
        self.read_waker.wake();
    }

    pub(crate) fn set_error(&self, e: Error) {
        self.inner.lock().unwrap().error = Some(e);
        self.read_waker.wake();
    }

    /// Take accumulated consume credits (bytes the app has read); the driver
    /// forwards them to `nghttp2_session_consume`. Registers the driver waker
    /// so later reads re-wake the connection.
    /// Bytes the consumer read since the last call (no waker work: the driver
    /// only calls this for streams that marked themselves dirty).
    pub(crate) fn take_consumed(&self) -> usize {
        self.consumed_unreported.swap(0, Ordering::Relaxed)
    }

    pub(crate) fn is_dropped(&self) -> bool {
        self.inner.lock().unwrap().dropped
    }

    /// Bytes received but not yet read by the consumer (still owed to the
    /// connection window if the stream dies).
    pub(crate) fn unread(&self) -> usize {
        self.inner.lock().unwrap().buffered
    }
}

/// The `http_body::Body` handed to the stack for requests (server side) and
/// responses (client side).
pub struct RecvBody {
    shared: Arc<RecvShared>,
}

impl RecvBody {
    pub(crate) fn new(shared: Arc<RecvShared>) -> Self {
        RecvBody { shared }
    }
}

impl http_body::Body for RecvBody {
    type Data = Bytes;
    type Error = Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Error>>> {
        let sh = &self.shared;
        let mut i = sh.inner.lock().unwrap();
        if let Some(data) = i.chunks.pop_front() {
            i.buffered -= data.len();
            let n = data.len();
            drop(i);
            sh.consumed_unreported.fetch_add(n, Ordering::Relaxed);
            sh.conn.mark(sh.sid);
            return Poll::Ready(Some(Ok(http_body::Frame::data(data))));
        }
        if let Some(e) = &i.error {
            return Poll::Ready(Some(Err(e.clone())));
        }
        if let Some(trailers) = i.trailers.take() {
            return Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))));
        }
        if i.eof {
            return Poll::Ready(None);
        }
        drop(i);
        sh.read_waker.register(cx.waker());
        // Re-check under the race between the check above and registration.
        let i = sh.inner.lock().unwrap();
        if !i.chunks.is_empty() || i.eof || i.error.is_some() || i.trailers.is_some() {
            drop(i);
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }

    fn is_end_stream(&self) -> bool {
        let i = self.shared.inner.lock().unwrap();
        i.eof && i.chunks.is_empty() && i.trailers.is_none()
    }
}

impl Drop for RecvBody {
    fn drop(&mut self) {
        let mut i = self.shared.inner.lock().unwrap();
        if !(i.eof && i.chunks.is_empty() && i.trailers.is_none()) {
            i.dropped = true;
        }
        drop(i);
        self.shared.conn.mark(self.shared.sid);
    }
}

/// Cap on bytes buffered from the stack's response/request body before we
/// stop polling it (nghttp2's flow-control window bounds the drain side).
pub(crate) const SEND_BUFFER_CAP: usize = 64 * 1024;

/// Driver-owned per-stream send state; `nghttp2_data_source.ptr` points here.
/// Only ever touched from the connection task (callbacks run inside
/// `mem_send`/`mem_recv` called by that same task).
pub(crate) struct SendBody {
    pub(crate) chunks: VecDeque<Bytes>,
    pub(crate) buffered: usize,
    pub(crate) body: Option<BoxBody>,
    pub(crate) trailers: Option<HeaderMap>,
    /// The stack body finished (once chunks drain, emit EOF/trailers).
    pub(crate) done: bool,
    /// The provider returned DEFERRED and needs `resume_data` on new data.
    pub(crate) deferred: bool,
    /// The stack body failed: reset the stream with this reason.
    pub(crate) failed: Option<Reason>,
}

impl SendBody {
    pub(crate) fn new(body: BoxBody) -> Box<Self> {
        Box::new(SendBody {
            chunks: VecDeque::new(),
            buffered: 0,
            body: Some(body),
            trailers: None,
            done: false,
            deferred: false,
            failed: None,
        })
    }

    /// Re-arm a recycled box for a new stream, keeping the `VecDeque`'s
    /// capacity (the point of the pool).
    pub(crate) fn reset(&mut self, body: BoxBody) {
        self.chunks.clear();
        self.buffered = 0;
        self.body = Some(body);
        self.trailers = None;
        self.done = false;
        self.deferred = false;
        self.failed = None;
    }
}

/// Per-connection free list of `SendBody` boxes. Streams are short-lived and
/// their addresses must stay stable while nghttp2 holds a provider pointer, so
/// recycling the boxes removes a heap allocation per request without changing
/// that invariant.
#[derive(Default)]
pub(crate) struct SendBodyPool {
    free: Vec<Box<SendBody>>,
}

impl SendBodyPool {
    pub(crate) fn take(&mut self, body: BoxBody) -> Box<SendBody> {
        match self.free.pop() {
            Some(mut b) => {
                b.reset(body);
                b
            }
            None => SendBody::new(body),
        }
    }

    pub(crate) fn put(&mut self, mut b: Box<SendBody>) {
        if self.free.len() < 64 {
            b.body = None;
            b.chunks.clear();
            b.trailers = None;
            self.free.push(b);
        }
    }
}

impl SendBody {

    /// Poll the stack body toward the chunk queue. Returns `true` if new data
    /// or a terminal state appeared (caller resumes a deferred stream).
    pub(crate) fn pump(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while self.buffered < SEND_BUFFER_CAP {
            let Some(body) = self.body.as_mut() else { break };
            match Pin::new(body).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    let frame = match frame.into_data() {
                        Ok(mut data) => {
                            // BoxBody yields an opaque `Buf`; materialize it as
                            // Bytes for the nghttp2 provider queue.
                            let data = data.copy_to_bytes(data.remaining());
                            if !data.is_empty() {
                                self.buffered += data.len();
                                self.chunks.push_back(data);
                                progressed = true;
                            }
                            continue;
                        }
                        Err(frame) => frame,
                    };
                    if let Ok(trailers) = frame.into_trailers() {
                        self.trailers = Some(trailers);
                        progressed = true;
                    }
                }
                Poll::Ready(Some(Err(_e))) => {
                    self.failed = Some(Reason::INTERNAL_ERROR);
                    self.body = None;
                    self.done = true;
                    progressed = true;
                    break;
                }
                Poll::Ready(None) => {
                    self.body = None;
                    self.done = true;
                    progressed = true;
                    break;
                }
                Poll::Pending => break,
            }
        }
        progressed
    }
}
