//! What a linkerd2-proxy needs when its connections arrive from an external
//! datapath instead of a socket: the [`DmeshIo`] endpoint the outbound stack
//! reads and writes in place of a `TcpStream`, the session and backend registry
//! an acceptor binds them to, and the [`runtime::RuntimeBackend`] contract the
//! datapath implements to drive both.

mod api;
mod backend;
mod io;
mod metrics;
pub mod runtime;

pub use api::{DmeshEvent, FlowId, Registrar, Registration, SessionToken, Slots, MAX_CONNS};
pub use backend::{BackendKey, Backends, PublishError, TakeError};

/// What one ARM worker's DMesh-specific outbound stack is wired to.
///
/// Held by the worker's connector, acceptor and adapter; two workers never
/// share one.
#[derive(Clone, Debug)]
pub struct Dmesh {
    pub backends: std::sync::Arc<Backends>,
    pub metrics: std::sync::Arc<SessionMetrics>,
}

pub use io::{dmesh_io_pair, DmeshIo, DmeshIoHandle, DrainState};
pub use metrics::{record_control_event, SessionMetrics};
