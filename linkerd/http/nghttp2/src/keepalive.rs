//! Hand-rolled HTTP/2 PING keepalive (nghttp2 1.43 has no built-in timer).

use std::future::Future as _;
use std::pin::Pin;
use std::task::Context;
use std::time::Duration;

use tokio::time::{interval, Interval, Sleep};

use crate::ffi::*;

pub(crate) struct KeepAlive {
    ticker: Interval,
    timeout: Duration,
    while_idle: bool,
    pending: Option<Pin<Box<Sleep>>>,
    opaque: [u8; 8],
}

pub(crate) enum Tick {
    /// Nothing to do.
    Idle,
    /// Submit a PING with this opaque payload.
    SendPing([u8; 8]),
    /// The outstanding PING deadline expired.
    TimedOut,
}

impl KeepAlive {
    pub(crate) fn new(interval_dur: Duration, timeout: Duration, while_idle: bool) -> Option<Self> {
        if interval_dur.is_zero() {
            return None;
        }
        let mut ticker = interval(interval_dur);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        Some(KeepAlive {
            ticker,
            timeout,
            while_idle,
            pending: None,
            opaque: *b"lnkd-ka0",
        })
    }

    /// Any inbound frame is liveness; clear an outstanding deadline.
    pub(crate) fn on_activity(&mut self) {
        self.pending = None;
    }

    pub(crate) fn on_ping_ack(&mut self, _payload: &[u8]) {
        self.pending = None;
    }

    pub(crate) fn poll(&mut self, cx: &mut Context<'_>, active_streams: usize) -> Tick {
        // Deadline first.
        if let Some(sleep) = self.pending.as_mut() {
            if sleep.as_mut().poll(cx).is_ready() {
                self.pending = None;
                return Tick::TimedOut;
            }
        }
        if self.pending.is_none()
            && (self.while_idle || active_streams > 0)
            && self.ticker.poll_tick(cx).is_ready()
        {
            // Vary payload so echoes are distinguishable across pings.
            let ctr = u64::from_ne_bytes(self.opaque).wrapping_add(1);
            self.opaque = ctr.to_ne_bytes();
            self.pending = Some(Box::pin(tokio::time::sleep(self.timeout)));
            // Register the fresh deadline.
            let _ = self.pending.as_mut().unwrap().as_mut().poll(cx);
            return Tick::SendPing(self.opaque);
        }
        Tick::Idle
    }
}

/// Submit a PING frame.
pub(crate) fn submit_ping(session: *mut nghttp2_session, opaque: &[u8; 8]) {
    unsafe {
        nghttp2_submit_ping(session, NGHTTP2_FLAG_NONE, opaque.as_ptr());
    }
}
