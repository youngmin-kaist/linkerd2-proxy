//! Client connection driver: mirrors `server.rs` (one task, inline polling of
//! request bodies, no per-stream spawn). Requests arrive over an mpsc channel
//! from `Connection` handles; responses are delivered through oneshots.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::BytesMut;
use http_body::Body as _;
use linkerd_http_box::BoxBody;
use linkerd_http_h2 as h2c;
use linkerd_io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot};

use crate::body::{ConnRecv, RecvBody, RecvShared, SendBody, SendBodyPool};
use crate::callbacks::send_read_callback;
use crate::error::{Error, Reason};
use crate::idmap::{self, IdMap};
use crate::ffi::*;
use crate::pump::{self, ReadStep};
use crate::session::{self as sess, Action, NvScratch, Session};

type Rsp = Result<http::Response<RecvBody>, Error>;

struct Submit {
    parts: http::request::Parts,
    body: BoxBody,
    rsp: oneshot::Sender<Rsp>,
}

/// Clonable request handle (the `SendRequest` analogue).
#[derive(Clone, Debug)]
pub struct Connection {
    tx: mpsc::Sender<Submit>,
}

impl Connection {
    pub async fn send_request(
        &self,
        req: http::Request<BoxBody>,
    ) -> Result<http::Response<RecvBody>, Error> {
        let (parts, body) = req.into_parts();
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Submit { parts, body, rsp: tx })
            .await
            .map_err(|_| Error::connection_closed())?;
        rx.await.map_err(|_| Error::connection_closed())?
    }
}

/// Establish a client session over `io`. Returns the handle plus the
/// connection driver future, which the caller spawns.
pub fn handshake<I>(
    io: I,
    params: h2c::ClientParams,
) -> (Connection, impl Future<Output = Result<(), Error>> + Send)
where
    I: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let (tx, rx) = mpsc::channel(64);
    let conn = ClientConn {
        session: Session::new_client(&params),
        io,
        rx,
        pending: idmap::new_map(),
        outs: idmap::new_map(),
        out: BytesMut::with_capacity(pump::READ_CHUNK),
        read_buf: vec![0u8; pump::READ_CHUNK].into_boxed_slice(),
        actions_buf: Vec::new(),
        nv_buf: NvScratch::with_capacity(16),
        closing: false,
        send_pool: SendBodyPool::default(),
        conn_recv: ConnRecv::new(),
        dirty_buf: Vec::new(),
    };
    (Connection { tx }, conn)
}

struct StreamOut {
    send: Box<SendBody>,
}

struct ClientConn<I> {
    session: Session,
    io: I,
    rx: mpsc::Receiver<Submit>,
    pending: IdMap<oneshot::Sender<Rsp>>,
    outs: IdMap<StreamOut>,
    out: BytesMut,
    read_buf: Box<[u8]>,
    actions_buf: Vec<Action>,
    nv_buf: NvScratch,
    closing: bool,
    send_pool: SendBodyPool,
    conn_recv: Arc<ConnRecv>,
    dirty_buf: Vec<i32>,
}

impl<I: AsyncRead + AsyncWrite + Send + Unpin> ClientConn<I> {
    /// Accept new requests from the handle and submit them to the session.
    fn drain_submits(&mut self, cx: &mut Context<'_>) -> bool {
        let mut any = false;
        loop {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(sub)) => {
                    self.submit(sub);
                    any = true;
                }
                Poll::Ready(None) => {
                    self.closing = true;
                    break;
                }
                Poll::Pending => break,
            }
        }
        any
    }

    fn submit(&mut self, sub: Submit) {
        let Submit { parts, body, rsp } = sub;
        let scheme = parts.uri.scheme_str().unwrap_or("http").to_owned();
        let authority = parts
            .uri
            .authority()
            .map(|a| a.as_str().to_owned())
            .or_else(|| {
                parts
                    .headers
                    .get(http::header::HOST)
                    .and_then(|h| h.to_str().ok())
                    .map(|s| s.to_owned())
            })
            .unwrap_or_default();
        let path = parts
            .uri
            .path_and_query()
            .map(|p| p.as_str().to_owned())
            .unwrap_or_else(|| "/".to_owned());

        let mut nv = std::mem::take(&mut self.nv_buf);
        sess::fill_request_nv(&mut nv.0, &parts, &scheme, &authority, &path);

        let empty = body.is_end_stream();
        let sid = if empty {
            unsafe {
                nghttp2_submit_request(
                    self.session.raw(),
                    std::ptr::null(),
                    nv.0.as_ptr(),
                    nv.0.len(),
                    std::ptr::null(),
                    std::ptr::null_mut(),
                )
            }
        } else {
            let send = self.send_pool.take(body);
            // Reserve the slot first so the boxed SendBody has a stable address
            // before nghttp2 records the provider (which it copies).
            self.outs.insert(i32::MIN, StreamOut { send });
            let o = self.outs.get_mut(&i32::MIN).expect("just inserted");
            let provider = nghttp2_data_provider {
                source: nghttp2_data_source {
                    ptr: &mut *o.send as *mut SendBody as *mut std::os::raw::c_void,
                },
                read_callback: send_read_callback,
            };
            let sid = unsafe {
                nghttp2_submit_request(
                    self.session.raw(),
                    std::ptr::null(),
                    nv.0.as_ptr(),
                    nv.0.len(),
                    &provider,
                    std::ptr::null_mut(),
                )
            };
            if let Some(slot) = self.outs.remove(&i32::MIN) {
                if sid > 0 {
                    self.outs.insert(sid, slot);
                }
            }
            sid
        };
        self.nv_buf = nv;

        if sid > 0 {
            self.pending.insert(sid, rsp);
        } else {
            let _ = rsp.send(Err(Error::session(Reason::REFUSED_STREAM)));
        }
    }

    fn handle_actions(&mut self, _cx: &mut Context<'_>) {
        let mut actions = std::mem::take(&mut self.actions_buf);
        std::mem::swap(&mut actions, &mut self.session.state().actions);
        for a in actions.drain(..) {
            match a {
                Action::Dispatch(sid, end_stream) => self.deliver_response(sid, end_stream),
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
                    // Response never arrived: fail the caller.
                    if let Some(tx) = self.pending.remove(&sid) {
                        let _ = tx.send(Err(Error::reset(Reason::from_u32(code))));
                    }
                }
            }
        }
        self.actions_buf = actions;
    }

    fn deliver_response(&mut self, sid: i32, end_stream: bool) {
        let st = self.session.state();
        let Some(rs) = st.recv.get_mut(&sid) else { return };
        let Some(builder) = rs.builder.take() else { return };
        let Some(rsp_builder) = builder.into_response() else { return };
        let shared = RecvShared::new(self.conn_recv.clone(), sid);
        // Flush response DATA that arrived in the same `mem_recv` as the head
        // (buffered before `shared` existed); before `set_eof` for correct order.
        for chunk in rs.pending.drain(..) {
            shared.push_chunk(chunk);
        }
        if end_stream {
            shared.set_eof();
        }
        rs.shared = Some(shared.clone());
        let body = RecvBody::new(shared);
        if let (Some(tx), Ok(rsp)) = (self.pending.remove(&sid), rsp_builder.body(body)) {
            let _ = tx.send(Ok(rsp));
        }
    }

    fn pump_send_bodies(&mut self, cx: &mut Context<'_>) -> bool {
        let sess = self.session.raw();
        let mut any = false;
        for (sid, o) in self.outs.iter_mut() {
            any |= o.send.pump(cx);
            if o.send.deferred && (!o.send.chunks.is_empty() || o.send.done) {
                o.send.deferred = false;
                unsafe { nghttp2_session_resume_data(sess, *sid) };
            }
        }
        any
    }

    /// Forward consume credits. Only streams that actually read bytes are
    /// visited (see `ConnRecv`); this used to scan every open stream, which
    /// cost an `AtomicWaker::register` per stream per poll.
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
                unsafe { nghttp2_session_consume(sess, sid, n) };
            }
        }
        self.dirty_buf = dirty;
    }
}

impl<I: AsyncRead + AsyncWrite + Send + Unpin> Future for ClientConn<I> {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let this = self.get_mut();
        loop {
            let mut progressed = false;

            progressed |= this.drain_submits(cx);
            if !this.session.state().actions.is_empty() {
                this.handle_actions(cx);
                progressed = true;
            }
            progressed |= this.pump_send_bodies(cx);
            this.forward_consume(cx);

            match pump::fill_out(this.session.raw(), &mut this.out) {
                Ok(n) => progressed |= n > 0,
                Err(e) => return Poll::Ready(Err(e)),
            }

            let before = this.out.len();
            match pump::flush_out(&mut this.io, &mut this.out, cx) {
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(_)) => {
                    return Poll::Ready(Err(Error::session(Reason::INTERNAL_ERROR)))
                }
                Poll::Pending => {}
            }
            progressed |= this.out.len() < before;

            if pump::want_read(this.session.raw()) && this.out.len() < pump::IN_DOS_CAP {
                let buf = &mut this.read_buf[..];
                match pump::read_feed(&mut this.io, this.session.raw(), buf, cx) {
                    Ok(ReadStep::Fed(_)) => progressed = true,
                    Ok(ReadStep::Eof) => this.closing = true,
                    Ok(ReadStep::Pending) => {}
                    Err(e) => return Poll::Ready(Err(e)),
                }
            }

            let idle = !pump::want_read(this.session.raw())
                && !pump::want_write(this.session.raw())
                && this.out.is_empty()
                && this.pending.is_empty();
            if this.closing && idle {
                return Poll::Ready(Ok(()));
            }

            if !progressed {
                return Poll::Pending;
            }
        }
    }
}
