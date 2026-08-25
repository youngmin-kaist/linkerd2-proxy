//! Hand-written FFI surface for the system libnghttp2 (>= 1.43).
//!
//! Declarations are transcribed from the nghttp2 1.43.0 public header; the
//! ABI has been stable for years. Only the fields/functions this crate uses
//! are declared. `nghttp2_frame` is a large C union — we declare only the
//! members we read (`hd`, `headers`) and only ever access it through a
//! pointer handed to us by nghttp2 (never constructed or copied by value).

#![allow(non_camel_case_types, dead_code)]

use std::os::raw::{c_char, c_int, c_void};

pub enum nghttp2_session {}
pub enum nghttp2_session_callbacks {}
pub enum nghttp2_option {}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct nghttp2_nv {
    pub name: *mut u8,
    pub value: *mut u8,
    pub namelen: usize,
    pub valuelen: usize,
    pub flags: u8,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct nghttp2_frame_hd {
    pub length: usize,
    pub stream_id: i32,
    pub type_: u8,
    pub flags: u8,
    pub reserved: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct nghttp2_priority_spec {
    pub stream_id: i32,
    pub weight: i32,
    pub exclusive: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct nghttp2_headers {
    pub hd: nghttp2_frame_hd,
    pub padlen: usize,
    pub pri_spec: nghttp2_priority_spec,
    pub nva: *mut nghttp2_nv,
    pub nvlen: usize,
    pub cat: u32,
}

/// Partial view of the C `nghttp2_frame` union: only members we read.
/// SAFETY: never construct or copy by value — read through `*const` only.
#[repr(C)]
pub union nghttp2_frame {
    pub hd: nghttp2_frame_hd,
    pub headers: nghttp2_headers,
}

#[repr(C)]
pub union nghttp2_data_source {
    pub fd: c_int,
    pub ptr: *mut c_void,
}

pub type nghttp2_data_source_read_callback = unsafe extern "C" fn(
    session: *mut nghttp2_session,
    stream_id: i32,
    buf: *mut u8,
    length: usize,
    data_flags: *mut u32,
    source: *mut nghttp2_data_source,
    user_data: *mut c_void,
) -> isize;

#[repr(C)]
pub struct nghttp2_data_provider {
    pub source: nghttp2_data_source,
    pub read_callback: nghttp2_data_source_read_callback,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct nghttp2_settings_entry {
    pub settings_id: i32,
    pub value: u32,
}

#[repr(C)]
pub struct nghttp2_info {
    pub age: c_int,
    pub version_num: c_int,
    pub version_str: *const c_char,
    pub proto_str: *const c_char,
}

// --- frame types ---
pub const NGHTTP2_DATA: u8 = 0;
pub const NGHTTP2_HEADERS: u8 = 1;
pub const NGHTTP2_RST_STREAM: u8 = 3;
pub const NGHTTP2_SETTINGS: u8 = 4;
pub const NGHTTP2_PING: u8 = 6;
pub const NGHTTP2_GOAWAY: u8 = 7;
pub const NGHTTP2_WINDOW_UPDATE: u8 = 8;

// --- headers categories ---
pub const NGHTTP2_HCAT_REQUEST: u32 = 0;
pub const NGHTTP2_HCAT_RESPONSE: u32 = 1;
pub const NGHTTP2_HCAT_PUSH_RESPONSE: u32 = 2;
pub const NGHTTP2_HCAT_HEADERS: u32 = 3;

// --- flags ---
pub const NGHTTP2_FLAG_NONE: u8 = 0;
pub const NGHTTP2_FLAG_END_STREAM: u8 = 0x01;
pub const NGHTTP2_FLAG_END_HEADERS: u8 = 0x04;
pub const NGHTTP2_FLAG_ACK: u8 = 0x01;

pub const NGHTTP2_NV_FLAG_NONE: u8 = 0;
pub const NGHTTP2_NV_FLAG_NO_INDEX: u8 = 0x01;

pub const NGHTTP2_DATA_FLAG_NONE: u32 = 0;
pub const NGHTTP2_DATA_FLAG_EOF: u32 = 0x01;
pub const NGHTTP2_DATA_FLAG_NO_END_STREAM: u32 = 0x02;

// --- settings ids ---
pub const NGHTTP2_SETTINGS_HEADER_TABLE_SIZE: i32 = 1;
pub const NGHTTP2_SETTINGS_ENABLE_PUSH: i32 = 2;
pub const NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS: i32 = 3;
pub const NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE: i32 = 4;
pub const NGHTTP2_SETTINGS_MAX_FRAME_SIZE: i32 = 5;
pub const NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE: i32 = 6;

// --- library error codes (negative returns) ---
pub const NGHTTP2_ERR_DEFERRED: isize = -508;
pub const NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE: isize = -521;
pub const NGHTTP2_ERR_CALLBACK_FAILURE: isize = -902;

// --- RFC 7540 error codes (stream/connection error_code values) ---
pub const NGHTTP2_NO_ERROR: u32 = 0;
pub const NGHTTP2_PROTOCOL_ERROR: u32 = 1;
pub const NGHTTP2_INTERNAL_ERROR: u32 = 2;
pub const NGHTTP2_FLOW_CONTROL_ERROR: u32 = 3;
pub const NGHTTP2_REFUSED_STREAM: u32 = 7;
pub const NGHTTP2_CANCEL: u32 = 8;
pub const NGHTTP2_ENHANCE_YOUR_CALM: u32 = 11;

pub type on_frame_recv_cb = unsafe extern "C" fn(
    *mut nghttp2_session,
    *const nghttp2_frame,
    *mut c_void,
) -> c_int;
pub type on_data_chunk_recv_cb = unsafe extern "C" fn(
    *mut nghttp2_session,
    u8,
    i32,
    *const u8,
    usize,
    *mut c_void,
) -> c_int;
pub type on_stream_close_cb =
    unsafe extern "C" fn(*mut nghttp2_session, i32, u32, *mut c_void) -> c_int;
pub type on_begin_headers_cb = unsafe extern "C" fn(
    *mut nghttp2_session,
    *const nghttp2_frame,
    *mut c_void,
) -> c_int;
pub type on_header_cb = unsafe extern "C" fn(
    *mut nghttp2_session,
    *const nghttp2_frame,
    *const u8,
    usize,
    *const u8,
    usize,
    u8,
    *mut c_void,
) -> c_int;

extern "C" {
    pub fn nghttp2_session_callbacks_new(ptr: *mut *mut nghttp2_session_callbacks) -> c_int;
    pub fn nghttp2_session_callbacks_del(cb: *mut nghttp2_session_callbacks);
    pub fn nghttp2_session_callbacks_set_on_frame_recv_callback(
        cb: *mut nghttp2_session_callbacks,
        f: on_frame_recv_cb,
    );
    pub fn nghttp2_session_callbacks_set_on_data_chunk_recv_callback(
        cb: *mut nghttp2_session_callbacks,
        f: on_data_chunk_recv_cb,
    );
    pub fn nghttp2_session_callbacks_set_on_stream_close_callback(
        cb: *mut nghttp2_session_callbacks,
        f: on_stream_close_cb,
    );
    pub fn nghttp2_session_callbacks_set_on_begin_headers_callback(
        cb: *mut nghttp2_session_callbacks,
        f: on_begin_headers_cb,
    );
    pub fn nghttp2_session_callbacks_set_on_header_callback(
        cb: *mut nghttp2_session_callbacks,
        f: on_header_cb,
    );

    pub fn nghttp2_option_new(ptr: *mut *mut nghttp2_option) -> c_int;
    pub fn nghttp2_option_del(opt: *mut nghttp2_option);
    pub fn nghttp2_option_set_no_auto_window_update(opt: *mut nghttp2_option, val: c_int);

    pub fn nghttp2_session_client_new2(
        ptr: *mut *mut nghttp2_session,
        cb: *const nghttp2_session_callbacks,
        user_data: *mut c_void,
        option: *const nghttp2_option,
    ) -> c_int;
    pub fn nghttp2_session_server_new2(
        ptr: *mut *mut nghttp2_session,
        cb: *const nghttp2_session_callbacks,
        user_data: *mut c_void,
        option: *const nghttp2_option,
    ) -> c_int;
    pub fn nghttp2_session_del(session: *mut nghttp2_session);

    pub fn nghttp2_session_mem_send(
        session: *mut nghttp2_session,
        data_ptr: *mut *const u8,
    ) -> isize;
    pub fn nghttp2_session_mem_recv(
        session: *mut nghttp2_session,
        input: *const u8,
        len: usize,
    ) -> isize;
    pub fn nghttp2_session_want_read(session: *mut nghttp2_session) -> c_int;
    pub fn nghttp2_session_want_write(session: *mut nghttp2_session) -> c_int;

    pub fn nghttp2_session_resume_data(session: *mut nghttp2_session, stream_id: i32) -> c_int;
    pub fn nghttp2_session_consume(
        session: *mut nghttp2_session,
        stream_id: i32,
        size: usize,
    ) -> c_int;
    pub fn nghttp2_session_consume_connection(
        session: *mut nghttp2_session,
        size: usize,
    ) -> c_int;
    pub fn nghttp2_session_set_local_window_size(
        session: *mut nghttp2_session,
        flags: u8,
        stream_id: i32,
        window_size: i32,
    ) -> c_int;
    pub fn nghttp2_session_terminate_session(
        session: *mut nghttp2_session,
        error_code: u32,
    ) -> c_int;

    pub fn nghttp2_submit_settings(
        session: *mut nghttp2_session,
        flags: u8,
        iv: *const nghttp2_settings_entry,
        niv: usize,
    ) -> c_int;
    pub fn nghttp2_submit_request(
        session: *mut nghttp2_session,
        pri_spec: *const nghttp2_priority_spec,
        nva: *const nghttp2_nv,
        nvlen: usize,
        data_prd: *const nghttp2_data_provider,
        stream_user_data: *mut c_void,
    ) -> i32;
    pub fn nghttp2_submit_response(
        session: *mut nghttp2_session,
        stream_id: i32,
        nva: *const nghttp2_nv,
        nvlen: usize,
        data_prd: *const nghttp2_data_provider,
    ) -> c_int;
    pub fn nghttp2_submit_trailer(
        session: *mut nghttp2_session,
        stream_id: i32,
        nva: *const nghttp2_nv,
        nvlen: usize,
    ) -> c_int;
    pub fn nghttp2_submit_rst_stream(
        session: *mut nghttp2_session,
        flags: u8,
        stream_id: i32,
        error_code: u32,
    ) -> c_int;
    pub fn nghttp2_submit_ping(
        session: *mut nghttp2_session,
        flags: u8,
        opaque_data: *const u8,
    ) -> c_int;
    pub fn nghttp2_submit_goaway(
        session: *mut nghttp2_session,
        flags: u8,
        last_stream_id: i32,
        error_code: u32,
        opaque_data: *const u8,
        opaque_data_len: usize,
    ) -> c_int;
    pub fn nghttp2_submit_shutdown_notice(session: *mut nghttp2_session) -> c_int;

    pub fn nghttp2_version(least_version: c_int) -> *const nghttp2_info;
    pub fn nghttp2_strerror(error_code: c_int) -> *const c_char;
}

/// Minimum runtime library version (1.43.0), matching the transcribed header.
pub const MIN_VERSION_NUM: c_int = 0x012b00;

/// Panics if the runtime libnghttp2 is older than the header we transcribed.
pub fn runtime_version_guard() {
    let info = unsafe { nghttp2_version(MIN_VERSION_NUM) };
    if info.is_null() {
        let actual = unsafe { nghttp2_version(0) };
        let v = if actual.is_null() {
            "<unknown>".to_string()
        } else {
            unsafe {
                std::ffi::CStr::from_ptr((*actual).version_str)
                    .to_string_lossy()
                    .into_owned()
            }
        };
        panic!(
            "libnghttp2 runtime version {v} is older than required 1.43.0; \
             refusing to run against an unvalidated ABI"
        );
    }
}

pub fn strerror(code: isize) -> String {
    unsafe {
        std::ffi::CStr::from_ptr(nghttp2_strerror(code as c_int))
            .to_string_lossy()
            .into_owned()
    }
}

#[cfg(test)]
mod layout_tests {
    use super::*;

    /// Guard the hand-written layouts against the C ABI. Reference values were
    /// produced by compiling sizeof/offsetof against the 1.43.0 header on this
    /// platform (aarch64/x86_64 LP64 agree).
    #[test]
    fn abi_layout() {
        use std::mem::size_of;
        assert_eq!(size_of::<nghttp2_nv>(), 40, "nghttp2_nv");
        assert_eq!(size_of::<nghttp2_frame_hd>(), 16, "nghttp2_frame_hd");
        assert_eq!(size_of::<nghttp2_priority_spec>(), 12, "nghttp2_priority_spec");
        assert_eq!(size_of::<nghttp2_settings_entry>(), 8, "nghttp2_settings_entry");
        assert_eq!(size_of::<nghttp2_data_source>(), 8, "nghttp2_data_source");
        assert_eq!(size_of::<nghttp2_headers>(), 64, "nghttp2_headers");
        assert_eq!(size_of::<nghttp2_data_provider>(), 16, "nghttp2_data_provider");

        // Field offsets the callbacks actually read.
        let h = std::mem::MaybeUninit::<nghttp2_headers>::uninit();
        let base = h.as_ptr() as usize;
        unsafe {
            assert_eq!(std::ptr::addr_of!((*h.as_ptr()).cat) as usize - base, 56, "headers.cat");
            assert_eq!(std::ptr::addr_of!((*h.as_ptr()).nva) as usize - base, 40, "headers.nva");
        }
        let f = std::mem::MaybeUninit::<nghttp2_frame_hd>::uninit();
        let base = f.as_ptr() as usize;
        unsafe {
            assert_eq!(
                std::ptr::addr_of!((*f.as_ptr()).stream_id) as usize - base,
                8,
                "frame_hd.stream_id"
            );
        }
    }

    #[test]
    fn version_guard_passes() {
        runtime_version_guard();
    }
}
