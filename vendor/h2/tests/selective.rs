//! End-to-end selective decoding over in-memory pipes:
//!
//! ```text
//! stock client A ─┐                                   ┌─ stock backend 0
//!                 ├─ selective server ("proxy") ── selective clients (2, per-request LB) ─┤
//! stock client B ─┘                                   └─ stock backend 1
//! ```
//!
//! The proxy forwards each request's sparse `HeaderMap` + `RawHeaderReps`
//! (appending one header), and relays the response headers/body/trailers
//! back the same way. The backends assert the full original header list.

use bytes::Bytes;
use h2::selective::{NeededSet, RawHeaderReps};
use h2::{client, server, RecvStream, SendStream};
use http::header::{HeaderName, HeaderValue};
use http::{HeaderMap, Method, Request, Response, StatusCode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::io::{duplex, DuplexStream};
use tokio::task::JoinHandle;

const N_REQ: usize = 200;
const PIPE: usize = 1 << 20;

type Pair = (String, String);

fn needed() -> NeededSet {
    NeededSet::new(
        ["content-type", "te", "grpc-status", "user-agent", "x-rsp"]
            .iter()
            .map(|s| HeaderName::from_static(s)),
    )
}

fn bearer() -> String {
    let mut s = String::from("Bearer ");
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    for k in 0..1500usize {
        s.push(alphabet[(k * 31 + k / 7) % alphabet.len()] as char);
    }
    s
}

fn trace_id(client: usize, r: usize) -> String {
    format!(
        "{:016x}:{:016x}:{}:1",
        ((client * 100_000 + r) as u64).wrapping_mul(2654435761),
        (r as u64).wrapping_mul(40503),
        client
    )
}

fn path(r: usize) -> &'static str {
    match r % 3 {
        0 => "/search.Search/Nearby",
        1 => "/ok",
        _ => "/api/v1/items?id=12345",
    }
}

/// The request the client sends, and the (name, value) list a backend must
/// see (pseudo-headers first, then fields in order).
fn make_request(client: usize, r: usize) -> (Request<()>, Vec<Pair>) {
    let uri = format!("http://backend.test{}", path(r));
    let mut b = Request::builder()
        .method(Method::POST)
        .uri(&uri)
        .header("content-type", "application/grpc")
        .header("user-agent", "grpc-go/1.71.0")
        .header("te", "trailers")
        .header("uber-trace-id", trace_id(client, r))
        .header("x-client", client.to_string())
        .header("x-req", r.to_string());
    let mut secret = HeaderValue::from_static("hunter2");
    secret.set_sensitive(true);
    b = b.header("x-secret", secret);
    let heavy = r % 4 == 0;
    if heavy {
        b = b.header("authorization", bearer());
    }
    let req = b.body(()).unwrap();
    let mut expect = vec![
        (":method".into(), "POST".into()),
        (":scheme".into(), "http".into()),
        (":authority".into(), "backend.test".into()),
        (":path".into(), path(r).into()),
        ("content-type".into(), "application/grpc".into()),
        ("user-agent".into(), "grpc-go/1.71.0".into()),
        ("te".into(), "trailers".into()),
        ("uber-trace-id".into(), trace_id(client, r)),
        ("x-client".into(), client.to_string()),
        ("x-req".into(), r.to_string()),
        ("x-secret".into(), "hunter2".into()),
    ];
    if heavy {
        expect.push(("authorization".into(), bearer()));
    }
    (req, expect)
}

fn request_pairs(req: &Request<RecvStream>) -> Vec<Pair> {
    let mut v = vec![
        (":method".to_string(), req.method().as_str().to_string()),
        (":scheme".into(), req.uri().scheme_str().unwrap_or("").into()),
        (":authority".into(), req.uri().authority().map(|a| a.as_str()).unwrap_or("").into()),
        (":path".into(), req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("").into()),
    ];
    for (n, val) in req.headers() {
        v.push((n.as_str().into(), String::from_utf8_lossy(val.as_bytes()).into()));
    }
    v
}

async fn pipe_body(mut body: RecvStream, mut tx: SendStream<Bytes>) -> Result<(), String> {
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|e| format!("body: {}", e))?;
        let _ = body.flow_control().release_capacity(chunk.len());
        tx.send_data(chunk, false).map_err(|e| format!("send_data: {}", e))?;
    }
    match body.trailers().await.map_err(|e| format!("trailers: {}", e))? {
        Some(t) => tx.send_trailers(t).map_err(|e| format!("send_trailers: {}", e))?,
        None => tx
            .send_data(Bytes::new(), true)
            .map_err(|e| format!("send_data eos: {}", e))?,
    }
    Ok(())
}

/// Stock backend: asserts the request header list, replies with headers,
/// a body chunk and trailers.
fn spawn_backend(io: DuplexStream, id: usize) -> JoinHandle<Result<usize, String>> {
    tokio::spawn(async move {
        let mut conn = server::handshake(io)
            .await
            .map_err(|e| format!("backend handshake: {}", e))?;
        let mut served = 0usize;
        while let Some(r) = conn.accept().await {
            let (req, mut respond) = r.map_err(|e| format!("backend accept: {}", e))?;
            let client: usize = req.headers()["x-client"].to_str().unwrap().parse().unwrap();
            let rn: usize = req.headers()["x-req"].to_str().unwrap().parse().unwrap();
            let (_, mut expect) = make_request(client, rn);
            expect.push(("l5d-added".into(), "yes".into()));
            let got = request_pairs(&req);
            if got != expect {
                return Err(format!(
                    "backend {} request {}/{} header mismatch:\n got {:?}\n exp {:?}",
                    id, client, rn, got, expect
                ));
            }
            // drain the request body
            let mut body = req.into_body();
            let mut data = Vec::new();
            while let Some(c) = body.data().await {
                let c = c.map_err(|e| format!("req body: {}", e))?;
                let _ = body.flow_control().release_capacity(c.len());
                data.extend_from_slice(&c);
            }
            if data != b"ping" {
                return Err(format!("backend {} bad request body {:?}", id, data));
            }
            let rsp = Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/grpc")
                .header("x-rsp", rn.to_string())
                .header("x-backend", id.to_string())
                .header("x-rsp-trace", trace_id(client, rn + 7))
                .body(())
                .unwrap();
            let mut tx = respond
                .send_response(rsp, false)
                .map_err(|e| format!("send_response: {}", e))?;
            tx.send_data(Bytes::from_static(b"hello"), false)
                .map_err(|e| format!("rsp data: {}", e))?;
            let mut trailers = HeaderMap::new();
            trailers.insert("grpc-status", HeaderValue::from_static("0"));
            trailers.insert("x-trailer-req", HeaderValue::from_str(&rn.to_string()).unwrap());
            tx.send_trailers(trailers)
                .map_err(|e| format!("rsp trailers: {}", e))?;
            served += 1;
        }
        Ok(served)
    })
}

/// Selective proxy: one selective server connection per client pipe, two
/// selective client connections to the backends, per-request round robin.
fn spawn_proxy(
    client_ios: Vec<DuplexStream>,
    backend_ios: Vec<DuplexStream>,
    needed: NeededSet,
) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let mut backends = Vec::new();
        let mut conn_tasks = Vec::new();
        for io in backend_ios {
            let (tx, conn) = client::Builder::new()
                .selective_headers(needed.clone())
                .handshake::<_, Bytes>(io)
                .await
                .map_err(|e| format!("proxy->backend handshake: {}", e))?;
            backends.push(tx);
            conn_tasks.push(tokio::spawn(async move {
                conn.await.map_err(|e| format!("proxy->backend conn: {}", e))
            }));
        }
        let rr = Arc::new(AtomicUsize::new(0));
        let mut server_tasks = Vec::new();
        for io in client_ios {
            let needed = needed.clone();
            let backends = backends.clone();
            let rr = rr.clone();
            server_tasks.push(tokio::spawn(async move {
                let mut conn = server::Builder::new()
                    .selective_headers(needed)
                    .handshake::<_, Bytes>(io)
                    .await
                    .map_err(|e| format!("proxy server handshake: {}", e))?;
                let mut stream_tasks: Vec<JoinHandle<Result<(), String>>> = Vec::new();
                while let Some(r) = conn.accept().await {
                    let (req, mut respond) = r.map_err(|e| format!("proxy accept: {}", e))?;
                    // Sparse map: pseudo + needed only; reps attached.
                    if req.extensions().get::<RawHeaderReps>().is_none() {
                        return Err("request without RawHeaderReps".into());
                    }
                    if req.headers().get("uber-trace-id").is_some()
                        || req.headers().get("x-secret").is_some()
                        || req.headers().get("authorization").is_some()
                    {
                        return Err(format!("sparse map materialized too much: {:?}", req.headers()));
                    }
                    for n in ["content-type", "user-agent", "te"] {
                        if req.headers().get(n).is_none() {
                            return Err(format!("sparse map missing needed {}", n));
                        }
                    }
                    let (parts, body) = req.into_parts();
                    let mut fwd = Request::from_parts(parts, ());
                    fwd.headers_mut()
                        .insert("l5d-added", HeaderValue::from_static("yes"));
                    let k = rr.fetch_add(1, Ordering::Relaxed) % backends.len();
                    let be = backends[k].clone();
                    stream_tasks.push(tokio::spawn(async move {
                        let mut be = be
                            .ready()
                            .await
                            .map_err(|e| format!("backend ready: {}", e))?;
                        let (rsp_fut, tx) = be
                            .send_request(fwd, false)
                            .map_err(|e| format!("proxy send_request: {}", e))?;
                        let body_task = tokio::spawn(pipe_body(body, tx));
                        let rsp = rsp_fut.await.map_err(|e| format!("proxy rsp: {}", e))?;
                        if rsp.extensions().get::<RawHeaderReps>().is_none() {
                            return Err("response without RawHeaderReps".into());
                        }
                        if rsp.headers().get("x-backend").is_some() {
                            return Err("response sparse map materialized x-backend".into());
                        }
                        let (parts, rbody) = rsp.into_parts();
                        let tx = respond
                            .send_response(Response::from_parts(parts, ()), false)
                            .map_err(|e| format!("proxy send_response: {}", e))?;
                        body_task.await.unwrap()?;
                        pipe_body(rbody, tx).await
                    }));
                }
                for t in stream_tasks {
                    t.await.unwrap()?;
                }
                Ok::<(), String>(())
            }));
        }
        for t in server_tasks {
            t.await.unwrap()?;
        }
        drop(backends);
        for t in conn_tasks {
            t.await.unwrap()?;
        }
        Ok(())
    })
}

/// Stock client: N sequential requests, asserting the relayed responses.
fn spawn_client(io: DuplexStream, id: usize, n: usize) -> JoinHandle<Result<(), String>> {
    tokio::spawn(async move {
        let (tx, conn) = client::handshake(io)
            .await
            .map_err(|e| format!("client handshake: {}", e))?;
        let conn_task = tokio::spawn(async move { conn.await });
        let mut tx = tx;
        for r in 0..n {
            let (req, _) = make_request(id, r);
            tx = tx
                .ready()
                .await
                .map_err(|e| format!("client ready: {}", e))?;
            let (rsp, mut body_tx) = tx
                .send_request(req, false)
                .map_err(|e| format!("client send_request: {}", e))?;
            body_tx
                .send_data(Bytes::from_static(b"ping"), true)
                .map_err(|e| format!("client body: {}", e))?;
            let rsp = rsp.await.map_err(|e| format!("client rsp {}/{}: {}", id, r, e))?;
            if rsp.status() != StatusCode::OK {
                return Err(format!("client {} r={} status {}", id, r, rsp.status()));
            }
            let h = rsp.headers();
            let want = [
                ("content-type", "application/grpc".to_string()),
                ("x-rsp", r.to_string()),
                ("x-rsp-trace", trace_id(id, r + 7)),
            ];
            for (n, v) in want.iter() {
                match h.get(*n) {
                    Some(got) if got.as_bytes() == v.as_bytes() => {}
                    other => {
                        return Err(format!("client {} r={} header {} = {:?}, want {}", id, r, n, other, v))
                    }
                }
            }
            if h.get("x-backend").is_none() {
                return Err(format!("client {} r={} missing x-backend", id, r));
            }
            let mut body = rsp.into_body();
            let mut data = Vec::new();
            while let Some(c) = body.data().await {
                let c = c.map_err(|e| format!("client body: {}", e))?;
                let _ = body.flow_control().release_capacity(c.len());
                data.extend_from_slice(&c);
            }
            if data != b"hello" {
                return Err(format!("client {} r={} body {:?}", id, r, data));
            }
            let trailers = body
                .trailers()
                .await
                .map_err(|e| format!("client trailers: {}", e))?
                .ok_or_else(|| format!("client {} r={} no trailers", id, r))?;
            if trailers.get("grpc-status").map(|v| v.as_bytes()) != Some(b"0") {
                return Err(format!("client {} r={} trailers {:?}", id, r, trailers));
            }
            if trailers.get("x-trailer-req").map(|v| v.as_bytes()) != Some(r.to_string().as_bytes()) {
                return Err(format!("client {} r={} trailers {:?}", id, r, trailers));
            }
        }
        drop(tx);
        conn_task
            .await
            .unwrap()
            .map_err(|e| format!("client conn: {}", e))?;
        Ok(())
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_clients_proxy_two_backends() {
    run_two_clients(needed()).await;
}

/// Same, with `trust_reps`: representations emitted without comparing
/// against the sparse map (the proxy only appends a header).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_clients_proxy_two_backends_trust_reps() {
    let needed = NeededSet::with_options(
        ["content-type", "te", "grpc-status", "user-agent", "x-rsp"]
            .iter()
            .map(|s| HeaderName::from_static(s)),
        true,
        false,
    );
    assert!(needed.trust_reps());
    run_two_clients(needed).await;
}

async fn run_two_clients(needed: NeededSet) {
    let (ca, pa) = duplex(PIPE);
    let (cb, pb) = duplex(PIPE);
    let (p0, b0) = duplex(PIPE);
    let (p1, b1) = duplex(PIPE);

    let backend0 = spawn_backend(b0, 0);
    let backend1 = spawn_backend(b1, 1);
    let proxy = spawn_proxy(vec![pa, pb], vec![p0, p1], needed.clone());
    let client_a = spawn_client(ca, 0, N_REQ);
    let client_b = spawn_client(cb, 1, N_REQ);

    client_a.await.unwrap().unwrap();
    client_b.await.unwrap().unwrap();
    proxy.await.unwrap().unwrap();
    let served0 = backend0.await.unwrap().unwrap();
    let served1 = backend1.await.unwrap().unwrap();
    assert_eq!(served0 + served1, 2 * N_REQ);
    assert!(served0 > N_REQ / 2 && served1 > N_REQ / 2, "{} {}", served0, served1);

    let s = needed.stats();
    println!("selective e2e stats: {:?}", s);
    // Both directions, all four proxy connections. Every request references
    // ~4 dynamic entries; misses happen only on first contact per backend
    // and on client-side table churn.
    assert!(s.dyn_hit > 4 * 2 * N_REQ as u64, "dyn_hit={}", s.dyn_hit);
    assert!(s.dyn_hit > s.dyn_miss * 8, "hit={} miss={}", s.dyn_hit, s.dyn_miss);
    assert_eq!(s.never_indexed, 2 * N_REQ as u64, "x-secret stays never-indexed");
    // out/in: +1 literal per request (l5d-added) and literal names for
    // dynamic name references; well under 2x.
    assert!(s.out_bytes < s.in_bytes * 3 / 2, "in={} out={}", s.in_bytes, s.out_bytes);
    assert!(s.in_bytes > 0);
    // The proxy appended `l5d-added` (a name outside the NeededSet), which
    // is not a rewrite; pseudo-headers and materialized names were untouched.
    assert_eq!(s.rewrites, 0);
}

/// One client, proxy with a single backend: header removal/modification by
/// the "stack" — a materialized name changed, a pseudo-header (path)
/// rewritten — is reflected on the wire.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stack_edits_are_authoritative() {
    let needed = needed();
    let (c, p) = duplex(PIPE);
    let (pb, b) = duplex(PIPE);

    let backend = tokio::spawn(async move {
        let mut conn = server::handshake(b).await.unwrap();
        let mut n = 0;
        while let Some(r) = conn.accept().await {
            let (req, mut respond) = r.unwrap();
            assert_eq!(req.uri().path(), "/rewritten");
            assert_eq!(req.headers().get("user-agent").unwrap(), "edited");
            assert!(req.headers().get("content-type").is_none(), "removed");
            assert_eq!(req.headers().get("uber-trace-id").unwrap(), &*trace_id(9, n));
            assert_eq!(req.headers().get("te").unwrap(), "trailers");
            let rsp = Response::builder().status(200).body(()).unwrap();
            respond.send_response(rsp, true).unwrap();
            n += 1;
        }
        n
    });
    let proxy = tokio::spawn(async move {
        let (be, conn) = client::Builder::new()
            .selective_headers(needed.clone())
            .handshake::<_, Bytes>(pb)
            .await
            .unwrap();
        let bconn = tokio::spawn(conn);
        let mut srv = server::Builder::new()
            .selective_headers(needed)
            .handshake::<_, Bytes>(p)
            .await
            .unwrap();
        while let Some(r) = srv.accept().await {
            let (req, mut respond) = r.unwrap();
            let (mut parts, _) = req.into_parts();
            parts.headers.remove("content-type");
            parts.headers.insert("user-agent", HeaderValue::from_static("edited"));
            parts.uri = "http://backend.test/rewritten".parse().unwrap();
            let fwd = Request::from_parts(parts, ());
            let mut be = be.clone().ready().await.unwrap();
            let (rsp, _) = be.send_request(fwd, true).unwrap();
            let rsp = rsp.await.unwrap();
            let (parts, _) = rsp.into_parts();
            respond.send_response(Response::from_parts(parts, ()), true).unwrap();
        }
        drop(be);
        bconn.await.unwrap().unwrap();
    });
    let (tx, conn) = client::handshake(c).await.unwrap();
    let cconn = tokio::spawn(conn);
    let mut tx = tx;
    for r in 0..20 {
        let (req, _) = make_request(9, r);
        tx = tx.ready().await.unwrap();
        let (rsp, _) = tx.send_request(req, true).unwrap();
        assert_eq!(rsp.await.unwrap().status(), 200);
    }
    drop(tx);
    cconn.await.unwrap().unwrap();
    proxy.await.unwrap();
    assert_eq!(backend.await.unwrap(), 20);
}
