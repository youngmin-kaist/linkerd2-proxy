//! C callback trampolines. Each runs synchronously inside a `mem_recv`/
//! `mem_send` call on the connection task. They mutate `ConnState` (via the
//! `user_data` pointer) and the per-stream `RecvShared`/`SendBody`, and defer
//! anything requiring a session-API call to the driver via `ConnState.actions`
//! — except the two operations the nghttp2 docs explicitly permit inline
//! (`submit_trailer` / `submit_rst_stream` in the data read callback).

use std::os::raw::c_void;

use bytes::Bytes;

use crate::body::SendBody;
use crate::ffi::*;
use crate::session::{Action, ConnState, HeaderBuilder, RecvStream};

unsafe fn conn<'a>(user_data: *mut c_void) -> &'a mut ConnState {
    unsafe { &mut *(user_data as *mut ConnState) }
}

extern "C" fn on_begin_headers(
    _session: *mut nghttp2_session,
    frame: *const nghttp2_frame,
    user_data: *mut c_void,
) -> i32 {
    let st = unsafe { conn(user_data) };
    let hd = unsafe { (*frame).hd };
    if hd.type_ != NGHTTP2_HEADERS {
        return 0;
    }
    // Read the HEADERS variant through a plain struct pointer (it shares
    // offset 0 with the union) to avoid autoref through the union field.
    let cat = unsafe { (*(frame as *const nghttp2_headers)).cat };
    // Server: only REQUEST opens a recv stream. Client: RESPONSE.
    let opens = if st.is_server {
        cat == NGHTTP2_HCAT_REQUEST
    } else {
        cat == NGHTTP2_HCAT_RESPONSE
    };
    if opens {
        st.recv.entry(hd.stream_id).or_insert_with(|| RecvStream {
            builder: Some(HeaderBuilder::default()),
            shared: None,
            head_done: false,
            pending: Vec::new(),
        });
    }
    0
}

extern "C" fn on_header(
    _session: *mut nghttp2_session,
    frame: *const nghttp2_frame,
    name: *const u8,
    namelen: usize,
    value: *const u8,
    valuelen: usize,
    _flags: u8,
    user_data: *mut c_void,
) -> i32 {
    let st = unsafe { conn(user_data) };
    let sid = unsafe { (*frame).hd.stream_id };
    let name = unsafe { std::slice::from_raw_parts(name, namelen) };
    let value = unsafe { std::slice::from_raw_parts(value, valuelen) };
    let count_only = st.no_header_materialize;
    if let Some(rs) = st.recv.get_mut(&sid) {
        if let Some(b) = rs.builder.as_mut() {
            let ok = if count_only {
                b.push_count_only(name, value)
            } else {
                b.push(name, value)
            };
            if !ok {
                return NGHTTP2_ERR_CALLBACK_FAILURE as i32;
            }
        }
    }
    0
}

extern "C" fn on_frame_recv(
    _session: *mut nghttp2_session,
    frame: *const nghttp2_frame,
    user_data: *mut c_void,
) -> i32 {
    let st = unsafe { conn(user_data) };
    let hd = unsafe { (*frame).hd };
    let end_stream = hd.flags & NGHTTP2_FLAG_END_STREAM != 0;
    crate::server::stats::frame(hd.type_);

    match hd.type_ {
        NGHTTP2_HEADERS => {
            let Some(rs) = st.recv.get_mut(&hd.stream_id) else {
                return 0;
            };
            if !rs.head_done {
                rs.head_done = true;
                st.actions.push(Action::Dispatch(hd.stream_id, end_stream));
            } else {
                // Trailing HEADERS (always END_STREAM).
                if let Some(b) = rs.builder.take() {
                    st.actions.push(Action::Trailers(hd.stream_id, b.headers));
                }
                if end_stream {
                    st.actions.push(Action::Eof(hd.stream_id));
                }
            }
        }
        NGHTTP2_DATA => {
            if end_stream {
                st.actions.push(Action::Eof(hd.stream_id));
            }
        }
        _ => {}
    }
    0
}

extern "C" fn on_data_chunk_recv(
    session: *mut nghttp2_session,
    _flags: u8,
    stream_id: i32,
    data: *const u8,
    len: usize,
    user_data: *mut c_void,
) -> i32 {
    let st = unsafe { conn(user_data) };
    let Some(rs) = st.recv.get_mut(&stream_id) else {
        return 0;
    };
    if let Some(shared) = rs.shared.as_ref() {
        if shared.is_dropped() {
            // Consumer gone: keep the connection window healthy, drop the bytes.
            unsafe { nghttp2_session_consume_connection(session, len) };
            return 0;
        }
        let bytes = Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len) });
        shared.push_chunk(bytes);
        return 0;
    }
    // Body not yet wired: HEADERS and this DATA arrived in the same `mem_recv`,
    // so the head is queued for dispatch but `shared` does not exist yet. Buffer
    // the chunk — the driver flushes `pending` into `shared` the moment it is
    // created (this same poll iteration). Do NOT consume here: like the normal
    // path, these bytes stay charged to the flow-control window until the
    // consumer reads them. (The old code consumed and discarded them, silently
    // losing the first body chunk of any client that coalesced HEADERS+DATA.)
    let bytes = Bytes::copy_from_slice(unsafe { std::slice::from_raw_parts(data, len) });
    rs.pending.push(bytes);
    0
}

extern "C" fn on_stream_close(
    _session: *mut nghttp2_session,
    stream_id: i32,
    error_code: u32,
    user_data: *mut c_void,
) -> i32 {
    let st = unsafe { conn(user_data) };
    st.actions.push(Action::Closed(stream_id, error_code));
    0
}

/// The nghttp2 data-source read callback: drains a per-stream `SendBody`.
/// `source.ptr` points at the `SendBody`. Trailers/reset are submitted inline
/// as the docs permit.
pub(crate) extern "C" fn send_read_callback(
    session: *mut nghttp2_session,
    stream_id: i32,
    buf: *mut u8,
    length: usize,
    data_flags: *mut u32,
    source: *mut nghttp2_data_source,
    _user_data: *mut c_void,
) -> isize {
    let sb = unsafe { &mut *((*source).ptr as *mut SendBody) };

    if let Some(reason) = sb.failed {
        unsafe {
            nghttp2_submit_rst_stream(session, NGHTTP2_FLAG_NONE, stream_id, reason_code(reason));
        }
        return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;
    }

    // Copy as much queued data as fits.
    let mut written = 0usize;
    while written < length {
        let Some(front) = sb.chunks.front_mut() else { break };
        let n = (length - written).min(front.len());
        unsafe {
            std::ptr::copy_nonoverlapping(front.as_ptr(), buf.add(written), n);
        }
        written += n;
        sb.buffered -= n;
        if n == front.len() {
            sb.chunks.pop_front();
        } else {
            let _ = front.split_to(n);
        }
    }
    if written > 0 {
        return written as isize;
    }

    // Queue empty.
    if !sb.done {
        sb.deferred = true;
        return NGHTTP2_ERR_DEFERRED;
    }

    // Done: emit EOF (+ trailers if present).
    unsafe {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    }
    if let Some(trailers) = sb.trailers.take() {
        let mut nv = Vec::with_capacity(trailers.len());
        crate::session::fill_trailer_nv(&mut nv, &trailers);
        if !nv.is_empty() {
            unsafe {
                *data_flags |= NGHTTP2_DATA_FLAG_NO_END_STREAM;
                nghttp2_submit_trailer(session, stream_id, nv.as_ptr(), nv.len());
            }
        }
    }
    0
}

fn reason_code(r: crate::error::Reason) -> u32 {
    r.description(); // keep Reason import used
    match r {
        x if x == crate::error::Reason::CANCEL => NGHTTP2_CANCEL,
        x if x == crate::error::Reason::REFUSED_STREAM => NGHTTP2_REFUSED_STREAM,
        x if x == crate::error::Reason::ENHANCE_YOUR_CALM => NGHTTP2_ENHANCE_YOUR_CALM,
        _ => NGHTTP2_INTERNAL_ERROR,
    }
}

/// Build a callbacks object with all trampolines installed. Caller owns it and
/// must `nghttp2_session_callbacks_del` after session creation.
pub(crate) fn install_callbacks() -> *mut nghttp2_session_callbacks {
    unsafe {
        let mut cb: *mut nghttp2_session_callbacks = std::ptr::null_mut();
        assert_eq!(nghttp2_session_callbacks_new(&mut cb), 0);
        nghttp2_session_callbacks_set_on_begin_headers_callback(cb, on_begin_headers);
        nghttp2_session_callbacks_set_on_header_callback(cb, on_header);
        nghttp2_session_callbacks_set_on_frame_recv_callback(cb, on_frame_recv);
        nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cb, on_data_chunk_recv);
        nghttp2_session_callbacks_set_on_stream_close_callback(cb, on_stream_close);
        cb
    }
}
