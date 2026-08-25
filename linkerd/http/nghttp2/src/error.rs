//! h2-crate-shaped error surface so the proxy's error-introspection sites
//! (`HasH2Reason`, orig-proto downgrade, metrics labels, retry-on-refused)
//! compile against this engine with their match arms unchanged.

use std::fmt;

/// RFC 7540 stream/connection error code, mirroring `h2::Reason`'s API.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Reason(u32);

impl Reason {
    pub const NO_ERROR: Reason = Reason(0);
    pub const PROTOCOL_ERROR: Reason = Reason(1);
    pub const INTERNAL_ERROR: Reason = Reason(2);
    pub const FLOW_CONTROL_ERROR: Reason = Reason(3);
    pub const SETTINGS_TIMEOUT: Reason = Reason(4);
    pub const STREAM_CLOSED: Reason = Reason(5);
    pub const FRAME_SIZE_ERROR: Reason = Reason(6);
    pub const REFUSED_STREAM: Reason = Reason(7);
    pub const CANCEL: Reason = Reason(8);
    pub const COMPRESSION_ERROR: Reason = Reason(9);
    pub const CONNECT_ERROR: Reason = Reason(10);
    pub const ENHANCE_YOUR_CALM: Reason = Reason(11);
    pub const INADEQUATE_SECURITY: Reason = Reason(12);
    pub const HTTP_1_1_REQUIRED: Reason = Reason(13);

    pub fn from_u32(v: u32) -> Reason {
        Reason(v)
    }

    pub fn description(&self) -> &'static str {
        match self.0 {
            0 => "not a result of an error",
            1 => "unspecific protocol error detected",
            2 => "unexpected internal error encountered",
            3 => "flow-control protocol violated",
            4 => "settings ACK not received in timely manner",
            5 => "received frame when stream half-closed",
            6 => "frame with invalid size",
            7 => "refused stream before processing any application logic",
            8 => "stream no longer needed",
            9 => "unable to maintain the header compression context",
            10 => "connection established in response to a CONNECT request was reset",
            11 => "detected excessive load generating behavior",
            12 => "security properties do not meet minimum requirements",
            13 => "endpoint requires HTTP/1.1",
            _ => "unknown reason",
        }
    }
}

impl fmt::Debug for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self.0 {
            0 => "NO_ERROR",
            1 => "PROTOCOL_ERROR",
            2 => "INTERNAL_ERROR",
            3 => "FLOW_CONTROL_ERROR",
            4 => "SETTINGS_TIMEOUT",
            5 => "STREAM_CLOSED",
            6 => "FRAME_SIZE_ERROR",
            7 => "REFUSED_STREAM",
            8 => "CANCEL",
            9 => "COMPRESSION_ERROR",
            10 => "CONNECT_ERROR",
            11 => "ENHANCE_YOUR_CALM",
            12 => "INADEQUATE_SECURITY",
            13 => "HTTP_1_1_REQUIRED",
            _ => return write!(f, "Reason({})", self.0),
        };
        f.write_str(name)
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.description())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    /// Stream-level reset (RST_STREAM sent or received).
    Reset(Reason),
    /// Connection-level GOAWAY (received or generated).
    GoAway(Reason),
    /// Keepalive PING timed out.
    KeepAliveTimedOut,
    /// Protocol/session failure from the library.
    Session(Reason),
    /// The connection driver task is gone.
    ConnectionClosed,
}

/// h2-termination error with the same introspection API as `h2::Error`.
#[derive(Clone, Debug)]
pub struct Error {
    kind: Kind,
}

impl Error {
    pub(crate) fn reset(reason: Reason) -> Self {
        Error { kind: Kind::Reset(reason) }
    }
    pub(crate) fn go_away(reason: Reason) -> Self {
        Error { kind: Kind::GoAway(reason) }
    }
    pub(crate) fn keepalive_timeout() -> Self {
        Error { kind: Kind::KeepAliveTimedOut }
    }
    pub(crate) fn session(reason: Reason) -> Self {
        Error { kind: Kind::Session(reason) }
    }
    pub(crate) fn connection_closed() -> Self {
        Error { kind: Kind::ConnectionClosed }
    }

    /// Mirrors `h2::Error::reason`.
    pub fn reason(&self) -> Option<Reason> {
        match self.kind {
            Kind::Reset(r) | Kind::GoAway(r) | Kind::Session(r) => Some(r),
            _ => None,
        }
    }

    /// Mirrors `h2::Error::is_reset`.
    pub fn is_reset(&self) -> bool {
        matches!(self.kind, Kind::Reset(_))
    }

    /// Mirrors `h2::Error::is_go_away`.
    pub fn is_go_away(&self) -> bool {
        matches!(self.kind, Kind::GoAway(_))
    }

    /// Mirrors `h2::Error::is_remote` closely enough for logging call sites.
    pub fn is_remote(&self) -> bool {
        matches!(self.kind, Kind::Reset(_) | Kind::GoAway(_))
    }
}

/// `h2::Error: From<Reason>` is used by orig-proto tests; keep the shape.
impl From<Reason> for Error {
    fn from(r: Reason) -> Self {
        Error::reset(r)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            Kind::Reset(r) => write!(f, "stream reset: {r}"),
            Kind::GoAway(r) => write!(f, "connection gone away: {r}"),
            Kind::KeepAliveTimedOut => f.write_str("keep-alive timed out"),
            Kind::Session(r) => write!(f, "http2 session error: {r}"),
            Kind::ConnectionClosed => f.write_str("http2 connection closed"),
        }
    }
}

impl std::error::Error for Error {}
