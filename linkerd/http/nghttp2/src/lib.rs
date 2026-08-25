//! HTTP/2 protocol engine backed by the system libnghttp2.
//!
//! This crate presents the same surface the proxy's hyper-based engine does —
//! `http::Request/Response<BoxBody>` in and out, tower services unchanged —
//! but drives the protocol with nghttp2's memory-to-memory session API on a
//! single task per connection (no per-stream task spawn, no shared stream
//! state behind a mutex), which is where its cost advantage comes from.
//!
//! Scope: the data path (proxy server termination and endpoint client). The
//! control-plane gRPC client and the tap server stay on hyper.

#![deny(rust_2018_idioms, clippy::disallowed_methods, clippy::disallowed_types)]
#![forbid(unsafe_op_in_unsafe_fn)]

mod body;
mod callbacks;
mod error;
mod ffi;
mod idmap;
mod keepalive;
mod pump;
mod session;

pub mod client;
pub mod server;

pub use crate::body::RecvBody;
pub use crate::error::{Error, Reason};

/// Print driver loop counters (diagnostics for the single-task design).
pub fn dump_stats() {
    crate::server::stats::dump();
}
