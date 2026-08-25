//! `Session`: a `Send` newtype over `*mut nghttp2_session` plus the shared
//! connection state that C callbacks mutate.
//!
//! SAFETY MODEL: a `Session` is owned by exactly one connection future and is
//! never shared. nghttp2 has no internal threading. `user_data` points at the
//! `ConnState` boxed alongside; callbacks run synchronously inside
//! `mem_send`/`mem_recv` calls made by the owning task, so they never alias
//! Rust borrows held across those calls (the driver never holds a `&mut
//! ConnState` across a session call). `!Sync` is preserved.

use std::os::raw::c_void;
use std::ptr::NonNull;
use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue};
use linkerd_http_h2 as h2c;

use crate::body::{RecvShared, SendBody};
use crate::idmap::{self, IdMap};
use crate::ffi::{self, *};

/// Per-stream state on the receive side (request on server, response on client).
pub(crate) struct RecvStream {
    /// Header builder accumulated across `on_header` callbacks.
    pub(crate) builder: Option<HeaderBuilder>,
    /// Shared body channel once the head is delivered.
    pub(crate) shared: Option<Arc<RecvShared>>,
    /// Head was already dispatched (subsequent HEADERS are trailers).
    pub(crate) head_done: bool,
    /// DATA chunks that arrived before the head was dispatched — i.e. HEADERS
    /// and body landed in the same `mem_recv`, so the shared channel does not
    /// exist yet. Buffered here and flushed into `shared` when it is created
    /// (server `dispatch` / client `deliver_response`). Without this the first
    /// body chunk of a coalesced HEADERS+DATA write was silently lost.
    pub(crate) pending: Vec<Bytes>,
}

/// Accumulates pseudo-headers + regular headers as they stream in.
#[derive(Default)]
pub(crate) struct HeaderBuilder {
    pub(crate) method: Option<Bytes>,
    pub(crate) path: Option<Bytes>,
    pub(crate) authority: Option<Bytes>,
    pub(crate) scheme: Option<Bytes>,
    pub(crate) status: Option<u16>,
    pub(crate) headers: HeaderMap,
    pub(crate) header_list_size: usize,
}

/// An action the driver must take after a `mem_recv`/`mem_send` returns, when
/// a callback cannot safely call the session API inline.
pub(crate) enum Action {
    /// A new receive stream head is ready to dispatch; the flag carries
    /// END_STREAM (a request/response with no body).
    Dispatch(i32, bool),
    /// Trailing HEADERS finished; set trailers on the recv body.
    Trailers(i32, HeaderMap),
    /// END_STREAM seen on DATA/trailers: the recv body is complete. This must
    /// be signalled here — NOT at stream close, which only happens after our
    /// own response completes (that ordering deadlocks any service that reads
    /// the request body before responding).
    Eof(i32),
    /// The stream closed with this reason (0 = NO_ERROR).
    Closed(i32, u32),
}

/// Shared connection state pointed to by nghttp2 `user_data`.
pub(crate) struct ConnState {
    pub(crate) is_server: bool,
    pub(crate) recv: IdMap<RecvStream>,
    /// Actions queued from callbacks, drained by the driver each loop.
    pub(crate) actions: Vec<Action>,
    /// EXPERIMENT: skip owned-header materialization (NG_NOHDR=1).
    pub(crate) no_header_materialize: bool,
    /// Rapid-reset leaky bucket (server only).
    pub(crate) reset_budget: i32,
    pub(crate) reset_budget_max: i32,
}

impl ConnState {
    fn new(is_server: bool, reset_budget_max: i32) -> Box<Self> {
        Box::new(ConnState {
            is_server,
            recv: idmap::new_map(),
            actions: Vec::new(),
            no_header_materialize: std::env::var_os("NG_NOHDR").is_some(),
            reset_budget: reset_budget_max,
            reset_budget_max,
        })
    }
}

pub(crate) struct Session {
    ptr: NonNull<nghttp2_session>,
    /// Kept alive for `user_data`; boxed so its address is stable.
    state: Box<ConnState>,
}

// SAFETY: see module docs — single-task ownership, no internal threading.
unsafe impl Send for Session {}

impl Session {
    pub(crate) fn state(&mut self) -> &mut ConnState {
        &mut self.state
    }

    pub(crate) fn raw(&self) -> *mut nghttp2_session {
        self.ptr.as_ptr()
    }

    pub(crate) fn new_server(params: &h2c::ServerParams) -> Self {
        let reset_max = params.max_pending_accept_reset_streams.unwrap_or(200) as i32;
        Self::new(true, reset_max, |cb, ud, opt, out| unsafe {
            nghttp2_session_server_new2(out, cb, ud, opt)
        })
        .apply_server_settings(params)
    }

    pub(crate) fn new_client(params: &h2c::ClientParams) -> Self {
        Self::new(false, 0, |cb, ud, opt, out| unsafe {
            nghttp2_session_client_new2(out, cb, ud, opt)
        })
        .apply_client_settings(params)
    }

    fn new(
        is_server: bool,
        reset_max: i32,
        ctor: impl FnOnce(
            *const nghttp2_session_callbacks,
            *mut c_void,
            *const nghttp2_option,
            *mut *mut nghttp2_session,
        ) -> i32,
    ) -> Self {
        ffi::runtime_version_guard();
        let mut state = ConnState::new(is_server, reset_max);
        let ud = &mut *state as *mut ConnState as *mut c_void;

        let callbacks = install_callbacks();
        let option = unsafe {
            let mut o: *mut nghttp2_option = std::ptr::null_mut();
            assert_eq!(nghttp2_option_new(&mut o), 0);
            // App-driven window updates: only ACK what the consumer read.
            nghttp2_option_set_no_auto_window_update(o, 1);
            o
        };

        let mut sess: *mut nghttp2_session = std::ptr::null_mut();
        let rc = ctor(callbacks, ud, option, &mut sess);
        unsafe {
            nghttp2_option_del(option);
            nghttp2_session_callbacks_del(callbacks);
        }
        assert_eq!(rc, 0, "nghttp2 session ctor failed");

        Session {
            ptr: NonNull::new(sess).expect("null session"),
            state,
        }
    }

    fn apply_server_settings(self, p: &h2c::ServerParams) -> Self {
        let (stream_win, conn_win) = window_sizes(&p.flow_control);
        let mut iv = vec![
            se(NGHTTP2_SETTINGS_ENABLE_PUSH, 0),
            se(NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, stream_win as u32),
        ];
        // Advertise a limit only when the operator configured one — same as
        // hyper. An unconditional default of 100 was added while chasing the
        // >100-stream collapse; that turned out to be tokio's per-task coop
        // budget (see `server::dispatch`), and with the real fix in place the
        // cap only costs pipelining (13.3k vs 17.2k req/s at 300 streams).
        if let Some(v) = p.max_concurrent_streams {
            iv.push(se(NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, v));
        }
        if let Some(v) = p.max_frame_size {
            iv.push(se(NGHTTP2_SETTINGS_MAX_FRAME_SIZE, v));
        }
        if let Some(v) = p.max_header_list_size {
            iv.push(se(NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE, v));
        }
        self.submit_settings(&iv, conn_win)
    }

    fn apply_client_settings(self, p: &h2c::ClientParams) -> Self {
        let (stream_win, conn_win) = window_sizes(&p.flow_control);
        let iv = vec![
            se(NGHTTP2_SETTINGS_ENABLE_PUSH, 0),
            se(NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, stream_win as u32),
        ];
        self.submit_settings(&iv, conn_win)
    }

    fn submit_settings(self, iv: &[nghttp2_settings_entry], conn_win: i32) -> Self {
        unsafe {
            let rc = nghttp2_submit_settings(self.raw(), NGHTTP2_FLAG_NONE, iv.as_ptr(), iv.len());
            assert_eq!(rc, 0, "submit_settings failed");
            // Connection-level window (stream 0).
            nghttp2_session_set_local_window_size(self.raw(), NGHTTP2_FLAG_NONE, 0, conn_win);
        }
        self
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        unsafe { nghttp2_session_del(self.ptr.as_ptr()) };
    }
}

fn se(id: i32, value: u32) -> nghttp2_settings_entry {
    nghttp2_settings_entry { settings_id: id, value }
}

/// Returns (stream_window, connection_window). Adaptive is unmappable → fixed.
fn window_sizes(fc: &Option<h2c::FlowControl>) -> (i32, i32) {
    const MIB: i32 = 1024 * 1024;
    match fc {
        Some(h2c::FlowControl::Fixed {
            initial_stream_window_size,
            initial_connection_window_size,
        }) => (
            *initial_stream_window_size as i32,
            *initial_connection_window_size as i32,
        ),
        Some(h2c::FlowControl::Adaptive) | None => {
            static WARNED: std::sync::Once = std::sync::Once::new();
            WARNED.call_once(|| {
                tracing::warn!(
                    "nghttp2 engine has no adaptive-window support; using fixed 1MiB/4MiB"
                );
            });
            (MIB, 4 * MIB)
        }
    }
}

impl HeaderBuilder {
    /// EXPERIMENT (NG_NOHDR=1): count fields only — no copy, no validation,
    /// no owned HeaderName/HeaderValue, no map insert. This is exactly what
    /// the pure-C nghttp2 benchmark's on_header does, so the delta against the
    /// normal path is the price of materializing owned http types.
    pub(crate) fn push_count_only(&mut self, name: &[u8], value: &[u8]) -> bool {
        self.header_list_size += name.len() + value.len() + 32;
        true
    }

    /// Feed one header; returns false on a hard protocol error.
    pub(crate) fn push(&mut self, name: &[u8], value: &[u8]) -> bool {
        self.header_list_size += name.len() + value.len() + 32;
        if name.first() == Some(&b':') {
            match name {
                b":method" => self.method = Some(Bytes::copy_from_slice(value)),
                b":path" => self.path = Some(Bytes::copy_from_slice(value)),
                b":authority" => self.authority = Some(Bytes::copy_from_slice(value)),
                b":scheme" => self.scheme = Some(Bytes::copy_from_slice(value)),
                b":status" => {
                    self.status = std::str::from_utf8(value).ok().and_then(|s| s.parse().ok());
                }
                _ => return false,
            }
            return true;
        }
        match (HeaderName::from_bytes(name), HeaderValue::from_bytes(value)) {
            (Ok(n), Ok(v)) => {
                self.headers.append(n, v);
                true
            }
            _ => false,
        }
    }

    /// Synthesize a fixed head for the count-only experiment (the real one is
    /// unavailable because nothing was materialized).
    pub(crate) fn into_request_stub() -> http::request::Builder {
        http::Request::builder()
            .method(http::Method::POST)
            .uri("http://srv-search/search.Search/Nearby")
    }

    pub(crate) fn into_request(self) -> Option<http::request::Builder> {
        let method = http::Method::from_bytes(self.method.as_deref()?).ok()?;
        let mut uri = http::uri::Builder::new();
        if let Some(scheme) = &self.scheme {
            uri = uri.scheme(http::uri::Scheme::try_from(scheme.as_ref()).ok()?);
        }
        if let Some(authority) = &self.authority {
            uri = uri.authority(authority.as_ref());
        }
        uri = uri.path_and_query(self.path.as_deref().unwrap_or(b"/"));
        let uri = uri.build().ok()?;
        let mut b = http::Request::builder()
            .method(method)
            .uri(uri)
            .version(http::Version::HTTP_2);
        if let Some(hs) = b.headers_mut() {
            *hs = self.headers;
        }
        Some(b)
    }

    pub(crate) fn into_response(self) -> Option<http::response::Builder> {
        let mut b = http::Response::builder()
            .status(self.status?)
            .version(http::Version::HTTP_2);
        if let Some(hs) = b.headers_mut() {
            *hs = self.headers;
        }
        Some(b)
    }
}

/// Fill a reusable `nghttp2_nv` scratch buffer, pointing directly at the
/// caller's `HeaderMap` bytes. nghttp2 documents that submit_* "creates copies
/// of all name/value pairs", so no owned storage is needed here — this is what
/// keeps the header path allocation-free per request.
///
/// SAFETY CONTRACT: the borrowed head/headers must outlive the submit call.
pub(crate) fn fill_response_nv(
    nv: &mut Vec<nghttp2_nv>,
    status: &str,
    headers: &HeaderMap,
) {
    nv.clear();
    nv.push(nv_pair(b":status", status.as_bytes()));
    push_headers(nv, headers);
}

pub(crate) fn fill_request_nv(
    nv: &mut Vec<nghttp2_nv>,
    parts: &http::request::Parts,
    scheme: &str,
    authority: &str,
    path: &str,
) {
    nv.clear();
    nv.push(nv_pair(b":method", parts.method.as_str().as_bytes()));
    nv.push(nv_pair(b":scheme", scheme.as_bytes()));
    if !authority.is_empty() {
        nv.push(nv_pair(b":authority", authority.as_bytes()));
    }
    nv.push(nv_pair(b":path", path.as_bytes()));
    push_headers(nv, &parts.headers);
}

pub(crate) fn fill_trailer_nv(nv: &mut Vec<nghttp2_nv>, headers: &HeaderMap) {
    nv.clear();
    push_headers(nv, headers);
}

/// Reusable `nghttp2_nv` scratch owned by a connection driver.
///
/// SAFETY: holds raw pointers only *transiently* — it is cleared at the start
/// of every fill and the pointers are consumed by the immediately following
/// submit call (which copies). It never escapes the connection task, so the
/// `Send` impl carries the same single-owner argument as `Session`.
#[derive(Default)]
pub(crate) struct NvScratch(pub(crate) Vec<nghttp2_nv>);

unsafe impl Send for NvScratch {}

impl NvScratch {
    pub(crate) fn with_capacity(n: usize) -> Self {
        NvScratch(Vec::with_capacity(n))
    }
}

#[inline]
fn nv_pair(name: &[u8], value: &[u8]) -> nghttp2_nv {
    nghttp2_nv {
        name: name.as_ptr() as *mut u8,
        value: value.as_ptr() as *mut u8,
        namelen: name.len(),
        valuelen: value.len(),
        flags: NGHTTP2_NV_FLAG_NONE,
    }
}

fn push_headers(nv: &mut Vec<nghttp2_nv>, headers: &HeaderMap) {
    for (name, value) in headers {
        // HeaderName is already lowercase. Connection-specific headers are
        // illegal in h2 and must be dropped.
        if is_connection_specific(name) {
            continue;
        }
        nv.push(nv_pair(name.as_str().as_bytes(), value.as_bytes()));
    }
}

fn is_connection_specific(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection" | "keep-alive" | "proxy-connection" | "transfer-encoding" | "upgrade"
    )
}

/// A `Send` pointer into a per-stream `SendBody`, used as `data_source.ptr`.
pub(crate) fn send_body_ptr(b: &mut SendBody) -> *mut c_void {
    b as *mut SendBody as *mut c_void
}

// Callback installation lives in `callbacks.rs`.
use crate::callbacks::install_callbacks;
