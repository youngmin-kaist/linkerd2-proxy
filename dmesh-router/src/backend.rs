//! Registry of backend channels and the hyper client that speaks over them.
//!
//! A BACKEND-mode dmesh connection (`DMESH_BACKEND_CONNECT` on the host) is a
//! byte stream to a real server on the host; the host bridge splices it into a
//! TCP connection to e.g. nginx. This module completes a hyper client handshake
//! over each such `DmeshIo` and keeps the resulting `SendRequest`, keyed by the
//! flow's destination — the address the client-side flow is routed to.
//!
//! Several channels may register under the same key (start N host backend
//! bridges); requests round-robin over them. That is the only way to get
//! backend concurrency in `Http1` mode, where one channel carries one request
//! at a time.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use hyper::body::Incoming;
use hyper::client::conn::{http1, http2};
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::sync::Notify;
use tracing::{debug, info, warn};

use crate::config::BackendProto;

/// Body type handed to the backend client. Bodies are forwarded, never
/// buffered, so this is the inbound `Incoming` boxed.
pub type ReqBody = BoxBody<Bytes, hyper::Error>;

/// A handle to one backend channel's request sink.
#[derive(Clone)]
pub enum Sender {
    /// h2 multiplexes: the sink is cheap to clone and used concurrently.
    H2(http2::SendRequest<ReqBody>),
    /// HTTP/1.1 has no multiplexing; the mutex serializes request submission
    /// on the channel (held until response headers arrive).
    Http1(Arc<tokio::sync::Mutex<http1::SendRequest<ReqBody>>>),
}

impl Sender {
    pub async fn send(self, req: Request<ReqBody>) -> Result<Response<Incoming>, hyper::Error> {
        match self {
            Self::H2(mut tx) => {
                tx.ready().await?;
                tx.send_request(req).await
            }
            Self::Http1(tx) => {
                let mut tx = tx.lock().await;
                tx.ready().await?;
                tx.send_request(req).await
            }
        }
    }
}

struct Channel {
    id: u64,
    sender: Sender,
}

#[derive(Default)]
struct Group {
    channels: Vec<Channel>,
    next: usize,
}

#[derive(Default)]
struct Registry {
    groups: Mutex<HashMap<SocketAddr, Group>>,
    /// Signalled whenever a channel registers, so requests that arrived before
    /// their backend did can retry instead of failing.
    ready: Notify,
    next_id: Mutex<u64>,
}

fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(Registry::default)
}

/// Complete the client handshake over a backend channel and publish it under
/// `key`. The connection task runs until the channel dies, then unregisters.
///
/// Generic over the transport (rather than taking `DmeshIo`) so the routing
/// path can be exercised over an in-memory pipe in tests.
pub async fn register<I>(key: SocketAddr, io: I, proto: BackendProto)
where
    I: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Unpin + 'static,
{
    let id = {
        let mut n = registry().next_id.lock().unwrap();
        *n += 1;
        *n
    };
    let io = TokioIo::new(io);

    let sender = match proto {
        BackendProto::H2 => match http2::Builder::new(TokioExecutor::new()).handshake(io).await {
            Ok((tx, conn)) => {
                spawn_conn(key, id, conn);
                Sender::H2(tx)
            }
            Err(error) => {
                warn!(%key, %error, "backend h2 handshake failed");
                return;
            }
        },
        BackendProto::Http1 => match http1::Builder::new().handshake(io).await {
            Ok((tx, conn)) => {
                spawn_conn(key, id, conn);
                Sender::Http1(Arc::new(tokio::sync::Mutex::new(tx)))
            }
            Err(error) => {
                warn!(%key, %error, "backend http1 handshake failed");
                return;
            }
        },
    };

    let count = {
        let mut groups = registry().groups.lock().unwrap();
        let group = groups.entry(key).or_default();
        group.channels.push(Channel { id, sender });
        group.channels.len()
    };
    info!(backend = %key, ?proto, channels = count, "backend channel registered");
    registry().ready.notify_waiters();
}

/// Drive a backend connection to completion, then drop its registration.
fn spawn_conn<C>(key: SocketAddr, id: u64, conn: C)
where
    C: std::future::Future<Output = Result<(), hyper::Error>> + Send + 'static,
{
    tokio::spawn(async move {
        match conn.await {
            Ok(()) => debug!(backend = %key, "backend connection closed"),
            Err(error) => debug!(backend = %key, %error, "backend connection failed"),
        }
        unregister(key, id);
    });
}

fn unregister(key: SocketAddr, id: u64) {
    let mut groups = registry().groups.lock().unwrap();
    if let Some(group) = groups.get_mut(&key) {
        group.channels.retain(|c| c.id != id);
        if group.channels.is_empty() {
            groups.remove(&key);
        }
    }
}

/// Next channel for `key`, round-robin, or `None` if none is registered.
fn pick(key: &SocketAddr) -> Option<Sender> {
    let mut groups = registry().groups.lock().unwrap();
    let group = groups.get_mut(key)?;
    if group.channels.is_empty() {
        return None;
    }
    let idx = group.next % group.channels.len();
    group.next = group.next.wrapping_add(1);
    Some(group.channels[idx].sender.clone())
}

/// Wait up to `wait` for a channel serving `key`. Backend bridges may connect
/// after the client does, so a request that arrives first waits rather than
/// failing outright.
pub async fn acquire(key: SocketAddr, wait: Duration) -> Option<Sender> {
    if let Some(sender) = pick(&key) {
        return Some(sender);
    }
    let deadline = Instant::now() + wait;
    loop {
        // Subscribe before re-checking so a registration racing with this loop
        // cannot be missed.
        let ready = registry().ready.notified();
        if let Some(sender) = pick(&key) {
            return Some(sender);
        }
        let remaining = deadline.checked_duration_since(Instant::now())?;
        if tokio::time::timeout(remaining, ready).await.is_err() {
            return None;
        }
    }
}

/// Every registered backend key (for logging / diagnostics).
pub fn keys() -> Vec<SocketAddr> {
    registry().groups.lock().unwrap().keys().copied().collect()
}
