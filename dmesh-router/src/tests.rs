//! Routing/rewrite tests over in-memory pipes.
//!
//! These exercise everything except the DMA transport itself: an h2 client
//! drives the same `serve_client` path a dmesh client channel would, and a
//! hyper server stands in for the nginx the host bridge splices to. The
//! transport is a `tokio::io::duplex` pair instead of a `DmeshIo`, which is the
//! only difference from the real datapath.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};

use crate::config::{BackendProto, Config};
use crate::proxy::Ctx;
use crate::{accept, backend};

fn config(proto: BackendProto, routes: HashMap<String, SocketAddr>) -> Config {
    Config {
        dev_pci: String::new(),
        rep_pci: String::new(),
        server_name: String::new(),
        cores: 1,
        backend_proto: proto,
        routes,
        default_backend: None,
        backend_wait: Duration::from_millis(200),
        max_streams: 100,
    }
}

/// Stands in for nginx: reports the authority and path it was asked for, so the
/// test can assert how the request was rewritten for each backend protocol.
async fn echo(req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let authority = req
        .uri()
        .authority()
        .map(|a| a.to_string())
        .or_else(|| {
            req.headers()
                .get(http::header::HOST)
                .and_then(|v| v.to_str().ok())
                .map(|v| v.to_string())
        })
        .unwrap_or_default();
    let had_hop_by_hop = req.headers().contains_key("keep-alive");
    let body = format!(
        "{authority} {} hop={had_hop_by_hop}",
        req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("")
    );
    Ok(Response::new(Full::new(Bytes::from(body))))
}

/// Spawn a backend server on one end of a pipe and register the other end as a
/// dmesh backend channel for `key`.
async fn spawn_backend(key: SocketAddr, proto: BackendProto) {
    let (router_side, server_side) = tokio::io::duplex(64 * 1024);
    let server_side = TokioIo::new(server_side);
    match proto {
        BackendProto::Http1 => {
            tokio::spawn(async move {
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(server_side, service_fn(echo))
                    .await;
            });
        }
        BackendProto::H2 => {
            tokio::spawn(async move {
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(server_side, service_fn(echo))
                    .await;
            });
        }
    }
    backend::register(key, router_side, proto).await;
}

/// Attach an h2 client to the router the way the host client channel does.
async fn client_for(
    cfg: Config,
    dst: SocketAddr,
) -> hyper::client::conn::http2::SendRequest<Empty<Bytes>> {
    let (client_side, router_side) = tokio::io::duplex(64 * 1024);
    accept::serve_client(
        router_side,
        Arc::new(Ctx {
            cfg: Arc::new(cfg),
            dst,
            slot: 0,
        }),
        tracing::Span::none(),
    );

    let (tx, conn) = hyper::client::conn::http2::handshake(
        TokioExecutor::new(),
        TokioIo::new(client_side),
    )
    .await
    .expect("client handshake");
    tokio::spawn(conn);
    tx
}

async fn get(
    tx: &mut hyper::client::conn::http2::SendRequest<Empty<Bytes>>,
    uri: &str,
) -> (StatusCode, String) {
    let req = Request::builder()
        .uri(uri)
        .header("keep-alive", "timeout=5") // must be stripped before forwarding
        .body(Empty::<Bytes>::new())
        .unwrap();
    let res = tx.send_request(req).await.expect("send request");
    let status = res.status();
    let body = res.into_body().collect().await.expect("body").to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// HTTP/1.1 backend: the request must reach nginx in origin-form with a `Host`
/// header carrying the original authority.
#[tokio::test]
async fn routes_to_http1_backend() {
    let key: SocketAddr = "10.0.0.11:8086".parse().unwrap();
    spawn_backend(key, BackendProto::Http1).await;

    let mut tx = client_for(config(BackendProto::Http1, HashMap::new()), key).await;
    let (status, body) = get(&mut tx, "http://svc.test/hello?a=1").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "svc.test /hello?a=1 hop=false");
}

/// h2c backend: the request keeps absolute-form (scheme + `:authority`).
#[tokio::test]
async fn routes_to_h2_backend() {
    let key: SocketAddr = "10.0.0.12:8086".parse().unwrap();
    spawn_backend(key, BackendProto::H2).await;

    let mut tx = client_for(config(BackendProto::H2, HashMap::new()), key).await;
    let (status, body) = get(&mut tx, "http://svc.test/hello").await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "svc.test /hello hop=false");
}

/// An authority in the route table overrides the connection's flow destination.
#[tokio::test]
async fn authority_route_overrides_flow_dst() {
    let flow_dst: SocketAddr = "10.0.0.13:8086".parse().unwrap();
    let routed: SocketAddr = "10.0.0.14:8086".parse().unwrap();
    spawn_backend(routed, BackendProto::Http1).await;

    let routes = HashMap::from([("svc.routed".to_string(), routed)]);
    let mut tx = client_for(config(BackendProto::Http1, routes), flow_dst).await;
    let (status, body) = get(&mut tx, "http://svc.routed/x").await;

    // Reaching a 200 at all proves the request went to `routed`: nothing is
    // registered for the flow destination.
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "svc.routed /x hop=false");
}

/// With no channel for the destination, the request fails fast rather than
/// hanging: the backend wait elapses and the router answers 503.
#[tokio::test]
async fn unrouted_destination_is_unavailable() {
    let key: SocketAddr = "10.0.0.15:8086".parse().unwrap();
    let mut tx = client_for(config(BackendProto::Http1, HashMap::new()), key).await;
    let (status, _) = get(&mut tx, "http://svc.missing/x").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
}

/// A request that arrives before its backend channel registers waits for it
/// instead of failing (the host bridges may start in either order).
#[tokio::test]
async fn request_waits_for_late_backend() {
    let key: SocketAddr = "10.0.0.16:8086".parse().unwrap();
    let mut tx = client_for(config(BackendProto::Http1, HashMap::new()), key).await;

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        spawn_backend(key, BackendProto::Http1).await;
    });

    let (status, body) = get(&mut tx, "http://svc.late/x").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, "svc.late /x hop=false");
}

/// Two channels under one key round-robin, which is how HTTP/1.1 backend
/// concurrency scales (one in-flight request per channel).
#[tokio::test]
async fn multiple_channels_round_robin() {
    let key: SocketAddr = "10.0.0.17:8086".parse().unwrap();
    spawn_backend(key, BackendProto::Http1).await;
    spawn_backend(key, BackendProto::Http1).await;

    let mut tx = client_for(config(BackendProto::Http1, HashMap::new()), key).await;
    for _ in 0..4 {
        let (status, _) = get(&mut tx, "http://svc.rr/x").await;
        assert_eq!(status, StatusCode::OK);
    }
}
