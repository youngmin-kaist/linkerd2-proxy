//! Low-level IO helpers shared by the server and client drivers: serialize
//! nghttp2's outbound frames into a coalescing buffer, flush to the async IO,
//! and feed inbound bytes to `mem_recv`. The connection loops in `server.rs`
//! and `client.rs` orchestrate these around their per-stream state.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, BytesMut};
use linkerd_io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::error::{Error, Reason};
use crate::ffi::*;

pub(crate) const READ_CHUNK: usize = 16 * 1024;
pub(crate) const OUT_HIGH_WATER: usize = 64 * 1024;
pub(crate) const IN_DOS_CAP: usize = 1024 * 1024;

/// Drain nghttp2's outbound queue into `out` until it either empties or hits
/// the high-water mark. `mem_send`'s returned pointer is valid only until the
/// next call, so we copy immediately (this also coalesces tiny frames into one
/// write, as the nghttp2 docs recommend).
pub(crate) fn fill_out(session: *mut nghttp2_session, out: &mut BytesMut) -> Result<usize, Error> {
    let start = out.len();
    while out.len() < OUT_HIGH_WATER {
        let mut ptr: *const u8 = std::ptr::null();
        let n = unsafe { nghttp2_session_mem_send(session, &mut ptr) };
        if n < 0 {
            return Err(Error::session(Reason::INTERNAL_ERROR));
        }
        if n == 0 {
            break;
        }
        let slice = unsafe { std::slice::from_raw_parts(ptr, n as usize) };
        out.extend_from_slice(slice);
    }
    Ok(out.len() - start)
}

/// Flush `out` to the IO. Returns Ready(Ok(true)) if fully drained,
/// Ready(Ok(false)) if the write is pending (partial), Err on IO failure.
pub(crate) fn flush_out<I: AsyncWrite + Unpin>(
    io: &mut I,
    out: &mut BytesMut,
    cx: &mut Context<'_>,
) -> Poll<io::Result<bool>> {
    while !out.is_empty() {
        match Pin::new(&mut *io).poll_write(cx, out) {
            Poll::Ready(Ok(0)) => {
                return Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "h2 write returned 0",
                )))
            }
            Poll::Ready(Ok(n)) => out.advance(n),
            Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            Poll::Pending => return Poll::Pending,
        }
    }
    Poll::Ready(Ok(true))
}

/// Result of one read+feed step.
pub(crate) enum ReadStep {
    /// Fed `n` bytes to the session; more may be available.
    Fed(usize),
    /// Peer closed the read side.
    Eof,
    /// Read is pending.
    Pending,
}

/// Read from the IO into `buf` and feed to `mem_recv` (which fires callbacks).
pub(crate) fn read_feed<I: AsyncRead + Unpin>(
    io: &mut I,
    session: *mut nghttp2_session,
    buf: &mut [u8],
    cx: &mut Context<'_>,
) -> Result<ReadStep, Error> {
    let mut rb = ReadBuf::new(buf);
    match Pin::new(io).poll_read(cx, &mut rb) {
        Poll::Ready(Ok(())) => {
            let filled = rb.filled();
            if filled.is_empty() {
                return Ok(ReadStep::Eof);
            }
            let n = filled.len();
            let consumed = unsafe { nghttp2_session_mem_recv(session, filled.as_ptr(), n) };
            if consumed < 0 {
                return Err(Error::session(Reason::PROTOCOL_ERROR));
            }
            Ok(ReadStep::Fed(n))
        }
        Poll::Ready(Err(_e)) => Err(Error::session(Reason::INTERNAL_ERROR)),
        Poll::Pending => Ok(ReadStep::Pending),
    }
}

pub(crate) fn want_read(session: *mut nghttp2_session) -> bool {
    unsafe { nghttp2_session_want_read(session) != 0 }
}

pub(crate) fn want_write(session: *mut nghttp2_session) -> bool {
    unsafe { nghttp2_session_want_write(session) != 0 }
}
