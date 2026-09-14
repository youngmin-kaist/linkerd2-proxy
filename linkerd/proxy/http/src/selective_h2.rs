//! `DMESH_SELECTIVE_H2=1`: selective HPACK decoding on hyper's HTTP/2 server
//! (the non-nghttp2 `Variant::H2` branch) and on the outbound HTTP/2 client.
//!
//! In selective mode the h2 layer walks received header blocks against a
//! mirror of the peer's HPACK table instead of decoding them: the
//! `http::Request`/`Response` `HeaderMap` holds only the pseudo-headers and
//! the names below, and the raw representations ride in the message
//! `Extensions` (`h2::selective::RawHeaderReps`) to be re-indexed on send.
//! Read once at startup, like `DMESH_NGHTTP2`.
//!
//! * `DMESH_SELECTIVE_H2_HEADERS=a,b,c` appends names to the default set
//!   (`+wide` appends the stage-2 default: `content-length,te,grpc-timeout,
//!   user-agent,l5d-dst-canonical,l5d-orig-proto`).
//! * `DMESH_SELECTIVE_H2_TRUST_REPS=1` emits representations without
//!   checking them against the sparse map (the stack must then not modify
//!   pseudo-headers or materialized names of relayed messages).
//! * `DMESH_SELECTIVE_H2_STATS=1` prints per-connection transcode stats to
//!   stderr when connections close.

use std::sync::OnceLock;

/// The minimum the outbound stack reads on relayed messages: response
/// classification (`content-type`, `grpc-status`, `grpc-message`), meshed
/// proxy error signalling (`l5d-proxy-error`, `l5d-proxy-connection`) and
/// the identity requirement header. Names read only from optional client
/// overrides (`l5d-retry-*`, `l5d-timeout*`, `l5d-require-id` is included)
/// or by access logging (`user-agent`, `content-length`) are not materialized
/// unless added via `DMESH_SELECTIVE_H2_HEADERS`. `content-length`/`te` are
/// only validated by h2/hyper when present and never added for a sparse map.
pub const DEFAULT_NEEDED: &[&str] = &[
    "content-type",
    "grpc-status",
    "grpc-message",
    "l5d-proxy-error",
    "l5d-proxy-connection",
    "l5d-require-id",
];

/// The stage-2 default, selectable with `DMESH_SELECTIVE_H2_HEADERS=+wide`.
pub const WIDE_NEEDED: &[&str] = &[
    "content-length",
    "te",
    "grpc-timeout",
    "user-agent",
    "l5d-dst-canonical",
    "l5d-orig-proto",
];

/// The shared `NeededSet` when `DMESH_SELECTIVE_H2` is set, else `None`.
/// One set for the whole process, so `NeededSet::stats()` aggregates every
/// connection.
pub fn needed_set() -> Option<&'static h2::selective::NeededSet> {
    static SET: OnceLock<Option<h2::selective::NeededSet>> = OnceLock::new();
    SET.get_or_init(|| {
        std::env::var_os("DMESH_SELECTIVE_H2")?;
        let mut names: Vec<http::HeaderName> = DEFAULT_NEEDED
            .iter()
            .map(|s| http::HeaderName::from_static(s))
            .collect();
        if let Ok(extra) = std::env::var("DMESH_SELECTIVE_H2_HEADERS") {
            for s in extra.split(',') {
                let s = s.trim().to_ascii_lowercase();
                if s.is_empty() {
                    continue;
                }
                if s == "+wide" {
                    names.extend(WIDE_NEEDED.iter().map(|s| http::HeaderName::from_static(s)));
                    continue;
                }
                match http::HeaderName::from_bytes(s.as_bytes()) {
                    Ok(n) => names.push(n),
                    Err(_) => tracing::warn!(
                        name = %s,
                        "DMESH_SELECTIVE_H2_HEADERS: invalid header name ignored"
                    ),
                }
            }
        }
        let trust = std::env::var_os("DMESH_SELECTIVE_H2_TRUST_REPS").is_some();
        let stats = std::env::var_os("DMESH_SELECTIVE_H2_STATS").is_some();
        tracing::info!(?names, trust, "HTTP/2 selective header decoding enabled");
        Some(h2::selective::NeededSet::with_options(names, trust, stats))
    })
    .as_ref()
}
