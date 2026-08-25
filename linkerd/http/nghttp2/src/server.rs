//! Server connection driver: one future per connection, streams dispatched to
//! the tower service and polled inline (no per-stream task spawn).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::BytesMut;
use futures::stream::{FuturesUnordered, StreamExt};
use http_body::Body as _;
use linkerd_error::Error as BoxError;
use linkerd_http_box::BoxBody;
use linkerd_http_h2 as h2c;
use linkerd_io::{AsyncRead, AsyncWrite};
use tower::{Service, ServiceExt};

use crate::body::{ConnRecv, RecvBody, RecvShared, SendBody, SendBodyPool};
use crate::callbacks::send_read_callback;
use crate::error::{Error, Reason};
use crate::idmap::{self, IdMap};
use crate::ffi::*;
use crate::keepalive::{self, KeepAlive, Tick};
use crate::pump::{self, ReadStep};
use crate::session::{self as sess, Action, NvScratch, Session};

/// Diagnostic counters (NG_STATS=1 prints on connection end). Loop-scan cost
/// is the design risk of the single-task driver, so make it observable.
pub(crate) mod stats {
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    pub static POLLS: AtomicU64 = AtomicU64::new(0);
    pub static ITERS: AtomicU64 = AtomicU64::new(0);
    pub static READS: AtomicU64 = AtomicU64::new(0);
    pub static REQS: AtomicU64 = AtomicU64::new(0);
    pub static BODY_POLLS: AtomicU64 = AtomicU64::new(0);
    /// Inbound frames by type (index = nghttp2 frame type, 0..=9).
    pub static FRAMES: [AtomicU64; 10] = [
        AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
        AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
        AtomicU64::new(0), AtomicU64::new(0),
    ];
    pub fn frame(t: u8) {
        if let Some(c) = FRAMES.get(t as usize) { c.fetch_add(1, Relaxed); }
    }
    pub fn bump(c: &AtomicU64) { c.fetch_add(1, Relaxed); }
    pub fn dump() {
        let (p, i, r, q, b) = (
            POLLS.load(Relaxed), ITERS.load(Relaxed), READS.load(Relaxed),
            REQS.load(Relaxed).max(1), BODY_POLLS.load(Relaxed),
        );
        eprintln!(
            "NG_STATS reqs={q} polls={p} ({:.1}/req) iters={i} ({:.1}/req) \
             mem_recv={r} ({:.1}/req) body_polls={b} ({:.1}/req)",
            p as f64 / q as f64, i as f64 / q as f64,
            r as f64 / q as f64, b as f64 / q as f64
        );
        let names = ["DATA","HEADERS","PRIORITY","RST","SETTINGS","PUSH","PING","GOAWAY","WINUPD","CONT"];
        let f: Vec<String> = FRAMES.iter().enumerate()
            .filter(|(_, c)| c.load(Relaxed) > 0)
            .map(|(i, c)| format!("{}={} ({:.2}/req)", names[i], c.load(Relaxed),
                                  c.load(Relaxed) as f64 / q as f64))
            .collect();
        eprintln!("NG_FRAMES in: {}", f.join(" "));
    }
}

type RespFuture = Pin<Box<dyn Future<Output = (i32, Result<http::Response<BoxBody>, BoxError>)> + Send>>;

/// Per-stream send state. nghttp2 *copies* the `nghttp2_data_provider` at
/// submit time (documented), so we only need to keep the `SendBody` alive at
/// a stable address for `data_source.ptr` to stay valid.
struct StreamOut {
    send: Box<SendBody>,
}

/// Serve `io` with `service` until the connection ends. `service` is cloned
/// per request (mirroring TowerToHyperService).
/// `shutdown` resolving starts a graceful close: a GOAWAY is sent, in-flight
/// streams are served to completion, then the connection ends. Pass
/// `std::future::pending()` for "never".
pub async fn serve<I, S, F>(
    io: I,
    service: S,
    params: h2c::ServerParams,
    shutdown: F,
) -> Result<(), Error>
where
    I: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    S: Service<http::Request<BoxBody>, Response = http::Response<BoxBody>, Error = BoxError>
        + Clone
        + Send
        + Unpin
        + 'static,
    S::Future: Send + 'static,
    F: Future<Output = ()> + Send + 'static,
{
    let keepalive = params
        .keep_alive
        .and_then(|ka| KeepAlive::new(ka.interval, ka.timeout, true));
    ServerConn {
        session: Session::new_server(&params),
        io,
        service,
        responding: FuturesUnordered::new(),
        outs: idmap::new_map(),
        out: BytesMut::with_capacity(pump::READ_CHUNK),
        read_buf: vec![0u8; pump::READ_CHUNK].into_boxed_slice(),
        keepalive,
        closing: false,
        shutdown: Some(Box::pin(shutdown)),
        going_away: false,
        actions_buf: Vec::new(),
        nv_buf: NvScratch::with_capacity(16),
        send_pool: SendBodyPool::default(),
        conn_recv: ConnRecv::new(),
        dirty_buf: Vec::new(),
    }
    .await
}

struct ServerConn<I, S: Service<http::Request<BoxBody>>> {
    session: Session,
    io: I,
    service: S,
    responding: FuturesUnordered<RespFuture>,
    outs: IdMap<StreamOut>,
    out: BytesMut,
    read_buf: Box<[u8]>,
    keepalive: Option<KeepAlive>,
    closing: bool,
    /// Resolves when the process/stack wants this connection drained.
    shutdown: Option<Pin<Box<dyn Future<Output = ()> + Send>>>,
    going_away: bool,
    /// Reusable scratch for draining callback actions.
    actions_buf: Vec<Action>,
    /// Reusable scratch for header submission (no per-request allocation).
    nv_buf: NvScratch,
    send_pool: SendBodyPool,
    conn_recv: Arc<ConnRecv>,
    dirty_buf: Vec<i32>,
}

impl<I, S> ServerConn<I, S>
where
    I: AsyncRead + AsyncWrite + Send + Unpin,
    S: Service<http::Request<BoxBody>, Response = http::Response<BoxBody>, Error = BoxError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    /// Drain queued callback actions: build requests, wire trailers/close.
    fn handle_actions(&mut self, _cx: &mut Context<'_>) {
        // Reuse one buffer: swapping keeps the callback-side Vec's capacity
        // instead of allocating a fresh one per batch.
        let mut actions = std::mem::take(&mut self.actions_buf);
        std::mem::swap(&mut actions, &mut self.session.state().actions);
        for a in actions.drain(..) {
            match a {
                // Dispatch immediately. There is no admission valve: the
                // service is cloned per request and driven via `oneshot`
                // (mirroring TowerToHyperService), and linkerd's LoadShed makes
                // `poll_ready` always-Ready anyway, so a pre-admission gate
                // gave no backpressure — only latency. Measured: disabling the
                // former cap changed nothing at 300/600 streams (0×5xx, no
                // collapse), because the real fix for high concurrency is
                // `unconstrained` in `dispatch`, not a dispatch cap.
                Action::Dispatch(sid, end_stream) => self.dispatch(sid, end_stream),
                Action::Eof(sid) => {
                    if let Some(rs) = self.session.state().recv.get(&sid) {
                        if let Some(shared) = &rs.shared {
                            shared.set_eof();
                        }
                    }
                }
                Action::Trailers(sid, trailers) => {
                    if let Some(rs) = self.session.state().recv.get(&sid) {
                        if let Some(shared) = &rs.shared {
                            shared.set_trailers(trailers);
                        }
                    }
                }
                Action::Closed(sid, code) => {
                    if let Some(rs) = self.session.state().recv.remove(&sid) {
                        if let Some(shared) = rs.shared {
                            if code != NGHTTP2_NO_ERROR {
                                shared.set_error(Error::reset(Reason::from_u32(code)));
                            } else {
                                shared.set_eof();
                            }
                        }
                    }
                    if let Some(o) = self.outs.remove(&sid) {
                        self.send_pool.put(o.send);
                    }
                }
            }
        }
        self.actions_buf = actions;
    }

    fn dispatch(&mut self, sid: i32, end_stream: bool) {
        let st = self.session.state();
        let Some(rs) = st.recv.get_mut(&sid) else { return };
        let Some(builder) = rs.builder.take() else { return };
        let count_only = st.no_header_materialize;
        let stub = if count_only {
            Some(sess::HeaderBuilder::into_request_stub())
        } else {
            None
        };
        let Some(req_builder) = stub.or_else(|| builder.into_request()) else {
            // Malformed request head: reset.
            unsafe {
                nghttp2_submit_rst_stream(self.session.raw(), NGHTTP2_FLAG_NONE, sid, NGHTTP2_PROTOCOL_ERROR);
            }
            return;
        };
        let shared = RecvShared::new(self.conn_recv.clone(), sid);
        // Flush any DATA chunks that arrived in the same `mem_recv` as the head
        // (buffered by `on_data_chunk_recv` before `shared` existed). Must run
        // before `set_eof` so the body order is chunks-then-EOF.
        for chunk in rs.pending.drain(..) {
            shared.push_chunk(chunk);
        }
        if end_stream {
            // No request body: the stack must see EOF immediately.
            shared.set_eof();
        }
        rs.shared = Some(shared.clone());
        let body = BoxBody::new(RecvBody::new(shared));
        let req = match req_builder.body(body) {
            Ok(r) => r,
            Err(_) => return,
        };
        stats::bump(&stats::REQS);
        // Clone per request. linkerd's stack is built to be cloned per request
        // (that is why every layer is Clone); issuing overlapping `call`s on a
        // single instance violates tower's one-poll_ready-per-call contract.
        let svc = self.service.clone();
        // MUST be `unconstrained`. tokio's cooperative budget is per TASK, and
        // this driver polls every in-flight service future from the single
        // connection task. Past ~100 concurrent requests the shared budget is
        // exhausted, after which polls return Pending *artificially* — long
        // enough for the stack's failfast timers to fire. The symptom was
        // brutal and misleading: at 300 streams throughput fell from ~13k to
        // 37 req/s with only a handful of 503s, while the engine's own counters
        // looked perfectly healthy. hyper does not hit this because it spawns a
        // task (and therefore a budget) per stream.
        //
        // Opting out is safe here because these futures yield at their own
        // await points; we are only declining tokio's artificial preemption.
        self.responding.push(Box::pin(async move {
            (sid, tokio::task::unconstrained(svc.oneshot(req)).await)
        }));
    }

    /// A response future completed: submit the response head + data provider.
    fn submit_response(&mut self, sid: i32, res: Result<http::Response<BoxBody>, BoxError>) {
        let rsp = match res {
            Ok(r) => r,
            Err(_e) => {
                unsafe {
                    nghttp2_submit_rst_stream(
                        self.session.raw(),
                        NGHTTP2_FLAG_NONE,
                        sid,
                        NGHTTP2_INTERNAL_ERROR,
                    );
                }
                return;
            }
        };
        let (parts, body) = rsp.into_parts();
        let mut nv = std::mem::take(&mut self.nv_buf);
        sess::fill_response_nv(&mut nv.0, parts.status.as_str(), &parts.headers);
        let empty = body.is_end_stream();
        if empty {
            unsafe {
                nghttp2_submit_response(
                    self.session.raw(),
                    sid,
                    nv.0.as_ptr(),
                    nv.0.len(),
                    std::ptr::null(),
                );
            }
            self.nv_buf = nv;
            return;
        }
        let send = self.send_pool.take(body);
        self.outs.insert(sid, StreamOut { send });
        // The boxed SendBody now lives in the map at a stable address; build
        // the provider on the stack (nghttp2 copies it).
        if let Some(o) = self.outs.get_mut(&sid) {
            let provider = nghttp2_data_provider {
                source: nghttp2_data_source {
                    ptr: &mut *o.send as *mut SendBody as *mut std::os::raw::c_void,
                },
                read_callback: send_read_callback,
            };
            unsafe {
                nghttp2_submit_response(
                    self.session.raw(),
                    sid,
                    nv.0.as_ptr(),
                    nv.0.len(),
                    &provider,
                );
            }
        }
        self.nv_buf = nv;
    }

    /// Poll all in-flight send bodies; resume any deferred streams with data.
    /// Returns true if any body produced data or finished — the caller must
    /// then iterate again so the new bytes reach `fill_out` (and so a body
    /// paused at the buffer cap gets re-polled once the provider drains it).
    fn pump_send_bodies(&mut self, cx: &mut Context<'_>) -> bool {
        let sess = self.session.raw();
        let mut any = false;
        for (sid, o) in self.outs.iter_mut() {
            stats::bump(&stats::BODY_POLLS);
            let progressed = o.send.pump(cx);
            any |= progressed;
            if progressed && o.send.deferred && (!o.send.chunks.is_empty() || o.send.done) {
                o.send.deferred = false;
                unsafe {
                    nghttp2_session_resume_data(sess, *sid);
                }
            }
        }
        any
    }

    /// Forward consume credits. Only streams that actually read bytes are
    /// visited (see `ConnRecv`); this used to scan every open stream.
    fn forward_consume(&mut self, cx: &mut Context<'_>) {
        let sess = self.session.raw();
        let mut dirty = std::mem::take(&mut self.dirty_buf);
        dirty.clear();
        self.conn_recv.take_dirty(cx, &mut dirty);
        for sid in dirty.drain(..) {
            let Some(rs) = self.session.state().recv.get(&sid) else { continue };
            let Some(shared) = rs.shared.as_ref() else { continue };
            let n = shared.take_consumed();
            if n > 0 {
                unsafe {
                    nghttp2_session_consume(sess, sid, n);
                }
            }
        }
        self.dirty_buf = dirty;
    }
}

impl<I, S> Future for ServerConn<I, S>
where
    Self: Unpin,
    I: AsyncRead + AsyncWrite + Send + Unpin,
    S: Service<http::Request<BoxBody>, Response = http::Response<BoxBody>, Error = BoxError>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let this = self.get_mut();
        stats::bump(&stats::POLLS);
        loop {
            stats::bump(&stats::ITERS);
            let mut progressed = false;

            // 1a. Drain completed response futures.
            while let Poll::Ready(Some((sid, res))) = this.responding.poll_next_unpin(cx) {
                this.submit_response(sid, res);
                progressed = true;
            }

            // 1b. Callback actions (dispatch/trailers/close). Dispatch is
            // immediate — no admission queue (see `handle_actions`).
            if !this.session.state().actions.is_empty() {
                this.handle_actions(cx);
                progressed = true;
            }

            // 1c. Pump send bodies; forward consume credits.
            progressed |= this.pump_send_bodies(cx);
            this.forward_consume(cx);

            // 1d. Keepalive.
            if let Some(ka) = this.keepalive.as_mut() {
                let active = this.session.state().recv.len();
                match ka.poll(cx, active) {
                    Tick::SendPing(opaque) => {
                        keepalive::submit_ping(this.session.raw(), &opaque);
                        progressed = true;
                    }
                    Tick::TimedOut => return Poll::Ready(Err(Error::keepalive_timeout())),
                    Tick::Idle => {}
                }
            }

            // 1e. Graceful shutdown: GOAWAY once, then let in-flight streams
            // finish (nghttp2 stops accepting new ones and clears want_read
            // when they are all closed).
            if !this.going_away {
                if let Some(f) = this.shutdown.as_mut() {
                    if f.as_mut().poll(cx).is_ready() {
                        this.shutdown = None;
                        this.going_away = true;
                        unsafe {
                            nghttp2_submit_shutdown_notice(this.session.raw());
                            nghttp2_submit_goaway(
                                this.session.raw(),
                                NGHTTP2_FLAG_NONE,
                                i32::MAX,
                                NGHTTP2_NO_ERROR,
                                std::ptr::null(),
                                0,
                            );
                        }
                        progressed = true;
                    }
                }
            }

            // 2. Serialize outbound. Producing bytes counts as progress: the
            // provider just drained per-stream queues, so the send bodies can
            // be pumped again on the next iteration.
            match pump::fill_out(this.session.raw(), &mut this.out) {
                Ok(n) => progressed |= n > 0,
                Err(e) => return Poll::Ready(Err(e)),
            }

            // 3. Flush. Draining bytes counts as progress: it makes room in
            // `out`, so the next iteration must re-run `fill_out` (otherwise
            // frames queued inside nghttp2 sit undelivered until unrelated IO
            // happens to wake us).
            let before = this.out.len();
            match pump::flush_out(&mut this.io, &mut this.out, cx) {
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(_)) => return Poll::Ready(Err(Error::session(Reason::INTERNAL_ERROR))),
                Poll::Pending => { /* keep going: reads can still progress */ }
            }
            progressed |= this.out.len() < before;

            // 4. Read + feed (bounded by DoS cap on our own egress backlog).
            if pump::want_read(this.session.raw()) && this.out.len() < pump::IN_DOS_CAP {
                let buf = &mut this.read_buf[..];
                match pump::read_feed(&mut this.io, this.session.raw(), buf, cx) {
                    Ok(ReadStep::Fed(_)) => {
                        stats::bump(&stats::READS);
                        if let Some(ka) = this.keepalive.as_mut() {
                            ka.on_activity();
                        }
                        progressed = true;
                    }
                    Ok(ReadStep::Eof) => {
                        this.closing = true;
                    }
                    Ok(ReadStep::Pending) => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }

            // 5. Termination.
            let done = (!pump::want_read(this.session.raw()) || this.going_away)
                && !pump::want_write(this.session.raw())
                && this.out.is_empty()
                && this.responding.is_empty();
            if done || (this.closing && this.responding.is_empty() && this.out.is_empty()) {
                if std::env::var_os("NG_STATS").is_some() {
                    stats::dump();
                }
                return Poll::Ready(Ok(()));
            }

            if !progressed {
                return Poll::Pending;
            }
        }
    }
}
