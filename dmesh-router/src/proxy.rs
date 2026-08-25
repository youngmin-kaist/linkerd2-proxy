//! The request handler: route, rewrite, forward, return.
//!
//! This is the whole "proxy" — a plain async fn behind `hyper::service::
//! service_fn`. No tower layers, no discovery, no policy: the routing decision
//! is a lookup of the flow's destination (or an authority route entry) in the
//! backend-channel registry.

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Request, Response, StatusCode, Uri};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use tracing::{debug, warn};

use crate::backend::{self, ReqBody};
use crate::config::{BackendProto, Config};

pub type ResBody = BoxBody<Bytes, hyper::Error>;

/// Per-connection context shared by every stream on a client channel.
pub struct Ctx {
    pub cfg: Arc<Config>,
    /// The flow's original destination — the routing key, analogous to what a
    /// TCP proxy recovers with `SO_ORIGINAL_DST`.
    pub dst: SocketAddr,
    pub slot: usize,
}

/// Headers that are connection-scoped and must not be forwarded (RFC 9110
/// §7.6.1). HTTP/2 forbids most of them outright; strip them so an h2 request
/// can be replayed on an HTTP/1.1 channel and vice versa.
fn hop_by_hop() -> [HeaderName; 9] {
    [
        http::header::CONNECTION,
        http::header::PROXY_AUTHENTICATE,
        http::header::PROXY_AUTHORIZATION,
        http::header::TE,
        http::header::TRAILER,
        http::header::TRANSFER_ENCODING,
        http::header::UPGRADE,
        HeaderName::from_static("keep-alive"),
        HeaderName::from_static("proxy-connection"),
    ]
}

pub async fn handle(req: Request<Incoming>, ctx: Arc<Ctx>) -> Result<Response<ResBody>, Infallible> {
    let authority = authority_of(&req);
    let key = ctx
        .cfg
        .route(authority.as_deref())
        .unwrap_or(ctx.dst);

    let (sender, key) = match backend::acquire(key, ctx.cfg.backend_wait).await {
        Some(sender) => (sender, key),
        None => match ctx.cfg.default_backend {
            Some(fallback) if fallback != key => {
                match backend::acquire(fallback, ctx.cfg.backend_wait).await {
                    Some(sender) => (sender, fallback),
                    None => return Ok(no_backend(key)),
                }
            }
            _ => return Ok(no_backend(key)),
        },
    };

    let authority = authority.unwrap_or_else(|| key.to_string());
    let req = match rewrite(req, &authority, ctx.cfg.backend_proto) {
        Ok(req) => req,
        Err(error) => {
            warn!(slot = ctx.slot, %error, "malformed request");
            return Ok(status(StatusCode::BAD_REQUEST));
        }
    };

    match sender.send(req).await {
        Ok(res) => Ok(res.map(|body| body.boxed())),
        Err(error) => {
            debug!(slot = ctx.slot, backend = %key, %error, "backend request failed");
            Ok(status(StatusCode::BAD_GATEWAY))
        }
    }
}

/// `:authority` (HTTP/2) or `Host` (HTTP/1.1).
fn authority_of<B>(req: &Request<B>) -> Option<String> {
    if let Some(authority) = req.uri().authority() {
        return Some(authority.as_str().to_string());
    }
    req.headers()
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_string())
}

/// Retarget the request at the backend channel: strip hop-by-hop headers and
/// put the URI in the form the outbound protocol requires — absolute-form with
/// scheme and authority for h2, origin-form plus a `Host` header for HTTP/1.1.
fn rewrite(
    req: Request<Incoming>,
    authority: &str,
    proto: BackendProto,
) -> Result<Request<ReqBody>, Box<dyn std::error::Error + Send + Sync>> {
    let (mut parts, body) = req.into_parts();
    strip_hop_by_hop(&mut parts.headers);

    let path = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());

    match proto {
        BackendProto::H2 => {
            parts.headers.remove(http::header::HOST);
            parts.uri = Uri::builder()
                .scheme("http")
                .authority(authority)
                .path_and_query(path)
                .build()?;
        }
        BackendProto::Http1 => {
            parts.uri = path.parse::<Uri>()?;
            parts
                .headers
                .insert(http::header::HOST, HeaderValue::from_str(authority)?);
        }
    }

    Ok(Request::from_parts(parts, body.boxed()))
}

fn strip_hop_by_hop(headers: &mut HeaderMap) {
    // A `Connection: x, y` header nominates further headers as hop-by-hop.
    let nominated = headers
        .get_all(http::header::CONNECTION)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .filter_map(|name| HeaderName::from_bytes(name.trim().as_bytes()).ok())
        .collect::<Vec<_>>();
    for name in nominated {
        headers.remove(name);
    }
    for name in hop_by_hop() {
        headers.remove(name);
    }
}

fn no_backend(key: SocketAddr) -> Response<ResBody> {
    warn!(backend = %key, registered = ?backend::keys(),
          "no dmesh backend channel for destination");
    status(StatusCode::SERVICE_UNAVAILABLE)
}

fn status(status: StatusCode) -> Response<ResBody> {
    let body = Empty::<Bytes>::new().map_err(|never| match never {}).boxed();
    let mut res = Response::new(body);
    *res.status_mut() = status;
    res
}
