//! Conformance: our nghttp2-backed server driven by a real hyper h2 client
//! over an in-memory duplex. These are the Phase-A gates for the server half.

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full, StreamBody};
use hyper_util::rt::{TokioExecutor, TokioIo};
use linkerd_error::Error as BoxError;
use linkerd_http_box::BoxBody;
use linkerd_http_h2 as h2c;
use linkerd_http_nghttp2 as ng;
use std::sync::Arc;
use tower::service_fn;

/// Every test runs under a hard deadline: a protocol deadlock must fail the
/// suite in seconds, not hang the runner (learned the hard way).
async fn with_timeout<F: std::future::Future>(f: F) -> F::Output {
    tokio::time::timeout(std::time::Duration::from_secs(10), f)
        .await
        .expect("test timed out — protocol deadlock")
}

fn params() -> h2c::ServerParams {
    h2c::ServerParams {
        max_concurrent_streams: Some(100),
        ..Default::default()
    }
}

/// Spawn our server over one end of a duplex; return a hyper h2 client handle.
async fn connect<S>(svc: S) -> hyper::client::conn::http2::SendRequest<BoxBody>
where
    S: tower::Service<
            http::Request<BoxBody>,
            Response = http::Response<BoxBody>,
            Error = BoxError,
        > + Clone
        + Send
        + Unpin
        + 'static,
    S::Future: Send + 'static,
{
    let (client_io, server_io) = tokio::io::duplex(1 << 18);
    tokio::spawn(async move {
        if let Err(e) = ng::server::serve(server_io, svc, params(), std::future::pending()).await {
            eprintln!("server ended: {e}");
        }
    });
    let (tx, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(client_io))
            .await
            .expect("handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    tx
}

fn empty() -> BoxBody {
    BoxBody::new(Empty::<Bytes>::new().map_err(BoxError::from))
}

fn full(s: &'static str) -> BoxBody {
    BoxBody::new(Full::new(Bytes::from_static(s.as_bytes())).map_err(BoxError::from))
}

#[tokio::test(flavor = "current_thread")]
async fn get_with_empty_response() {
    with_timeout(async move {
    let svc = service_fn(|req: http::Request<BoxBody>| async move {
        assert_eq!(req.method(), http::Method::GET);
        assert_eq!(req.uri().path(), "/hello");
        assert_eq!(req.headers().get("x-test").unwrap(), "yes");
        Ok::<_, BoxError>(
            http::Response::builder()
                .status(204)
                .header("x-server", "ng")
                .body(empty())
                .unwrap(),
        )
    });
    let mut tx = connect(svc).await;

    let req = http::Request::builder()
        .method("GET")
        .uri("http://example.com/hello")
        .header("x-test", "yes")
        .body(empty())
        .unwrap();
    let rsp = tx.send_request(req).await.expect("send");
    assert_eq!(rsp.status(), 204);
    assert_eq!(rsp.headers().get("x-server").unwrap(), "ng");
    let body = rsp.into_body().collect().await.unwrap().to_bytes();
    assert!(body.is_empty());}).await
}

#[tokio::test(flavor = "current_thread")]
async fn post_echoes_request_body() {
    with_timeout(async move {
    let svc = service_fn(|req: http::Request<BoxBody>| async move {
        let bytes = req.into_body().collect().await.unwrap().to_bytes();
        Ok::<_, BoxError>(
            http::Response::builder()
                .status(200)
                .body(BoxBody::new(
                    Full::new(bytes).map_err(BoxError::from),
                ))
                .unwrap(),
        )
    });
    let mut tx = connect(svc).await;

    let req = http::Request::builder()
        .method("POST")
        .uri("http://example.com/echo")
        .body(full("ping-pong-payload"))
        .unwrap();
    let rsp = tx.send_request(req).await.expect("send");
    assert_eq!(rsp.status(), 200);
    let body = rsp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"ping-pong-payload");}).await
}

#[tokio::test(flavor = "current_thread")]
async fn many_streams_on_one_connection() {
    with_timeout(async move {
    let svc = service_fn(|req: http::Request<BoxBody>| async move {
        let path = req.uri().path().to_owned();
        let _ = req.into_body().collect().await;
        Ok::<_, BoxError>(
            http::Response::builder()
                .status(200)
                .body(BoxBody::new(
                    Full::new(Bytes::from(path)).map_err(BoxError::from),
                ))
                .unwrap(),
        )
    });
    let tx = connect(svc).await;

    for i in 0..64u32 {
        let mut tx = tx.clone();
        let req = http::Request::builder()
            .uri(format!("http://example.com/n/{i}"))
            .body(empty())
            .unwrap();
        let rsp = tx.send_request(req).await.expect("send");
        assert_eq!(rsp.status(), 200);
        let body = rsp.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], format!("/n/{i}").as_bytes());
    }}).await
}

#[tokio::test(flavor = "current_thread")]
async fn grpc_trailers_round_trip() {
    with_timeout(async move {
    let svc = service_fn(|req: http::Request<BoxBody>| async move {
        let _ = req.into_body().collect().await;
        // Response with DATA then trailers, like a unary gRPC reply.
        let frames = futures::stream::iter(vec![
            Ok::<_, BoxError>(http_body::Frame::data(Bytes::from_static(
                b"\0\0\0\0\x05hello",
            ))),
            Ok(http_body::Frame::trailers({
                let mut t = http::HeaderMap::new();
                t.insert("grpc-status", http::HeaderValue::from_static("0"));
                t
            })),
        ]);
        Ok::<_, BoxError>(
            http::Response::builder()
                .status(200)
                .header("content-type", "application/grpc")
                .body(BoxBody::new(StreamBody::new(frames)))
                .unwrap(),
        )
    });
    let mut tx = connect(svc).await;

    let req = http::Request::builder()
        .method("POST")
        .uri("http://srv/pkg.Svc/Method")
        .header("content-type", "application/grpc")
        .body(empty())
        .unwrap();
    let rsp = tx.send_request(req).await.expect("send");
    assert_eq!(rsp.status(), 200);
    let collected = rsp.into_body().collect().await.unwrap();
    let trailers = collected.trailers().cloned();
    let data = collected.to_bytes();
    assert_eq!(&data[..], b"\0\0\0\0\x05hello");
    let trailers = trailers.expect("trailers present");
    assert_eq!(trailers.get("grpc-status").unwrap(), "0");}).await
}

#[tokio::test(flavor = "current_thread")]
async fn streaming_response_across_many_frames() {
    with_timeout(async move {
    const CHUNK: usize = 16 * 1024;
    const N: usize = 40; // 640 KiB — exercises flow control + DEFERRED/resume
    let svc = service_fn(|req: http::Request<BoxBody>| async move {
        let _ = req.into_body().collect().await;
        let frames = futures::stream::iter(
            (0..N).map(|_| Ok::<_, BoxError>(http_body::Frame::data(Bytes::from(vec![7u8; CHUNK])))),
        );
        Ok::<_, BoxError>(
            http::Response::builder()
                .status(200)
                .body(BoxBody::new(StreamBody::new(frames)))
                .unwrap(),
        )
    });
    let mut tx = connect(svc).await;

    let req = http::Request::builder()
        .uri("http://example.com/big")
        .body(empty())
        .unwrap();
    let rsp = tx.send_request(req).await.expect("send");
    let body = rsp.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(body.len(), CHUNK * N);
    assert!(body.iter().all(|&b| b == 7));}).await
}

/// Append a framed HTTP/2 frame (9-byte header + payload) to `buf`.
fn push_frame(buf: &mut Vec<u8>, ftype: u8, flags: u8, sid: u32, payload: &[u8]) {
    let len = payload.len();
    buf.push((len >> 16) as u8);
    buf.push((len >> 8) as u8);
    buf.push(len as u8);
    buf.push(ftype);
    buf.push(flags);
    buf.extend_from_slice(&(sid & 0x7fff_ffff).to_be_bytes());
    buf.extend_from_slice(payload);
}

/// Append an HPACK "Literal Header Field without Indexing — Indexed Name"
/// (no Huffman): a 4-bit-prefix name index, then a 7-bit-prefix value length
/// and the raw value. `name_index` must be < 15 and `value.len()` < 128 (true
/// for every field this test emits), so each integer fits in one byte.
fn push_literal(buf: &mut Vec<u8>, name_index: u8, value: &[u8]) {
    buf.push(name_index); // 0x0N: without-indexing representation, name = index N
    buf.push(value.len() as u8);
    buf.extend_from_slice(value);
}

/// Regression: a request whose HEADERS and body DATA arrive in the *same* read
/// (one `nghttp2_session_mem_recv`) must not lose the body.
///
/// We hand-roll the client so the preface, SETTINGS, HEADERS, and DATA land in
/// a single write — and therefore a single `mem_recv` — which is exactly the
/// ordering that dropped the body: the data-chunk callback fired while the head
/// was still queued for dispatch (`RecvShared` not yet created), and the old
/// code discarded those bytes. The hyper-driven `post_echoes_request_body`
/// misses this because the h2 handshake round-trip forces HEADERS and DATA into
/// separate server reads. The service here echoes the request body, so a lost
/// body surfaces as an empty response.
#[tokio::test(flavor = "current_thread")]
async fn request_body_coalesced_with_headers_is_not_lost() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    with_timeout(async move {
        let svc = service_fn(|req: http::Request<BoxBody>| async move {
            let bytes = req.into_body().collect().await.unwrap().to_bytes();
            Ok::<_, BoxError>(
                http::Response::builder()
                    .status(200)
                    .body(BoxBody::new(Full::new(bytes).map_err(BoxError::from)))
                    .unwrap(),
            )
        });

        let (client_io, server_io) = tokio::io::duplex(1 << 18);
        tokio::spawn(async move {
            if let Err(e) =
                ng::server::serve(server_io, svc, params(), std::future::pending()).await
            {
                eprintln!("server ended: {e}");
            }
        });
        let mut io = client_io;

        const PAYLOAD: &[u8] = b"coalesced-request-body-payload";

        // Everything the client says, in one buffer -> one server read.
        let mut buf = Vec::new();
        buf.extend_from_slice(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"); // connection preface
        push_frame(&mut buf, 0x4, 0x0, 0, &[]); // empty SETTINGS (required first frame)

        // HEADERS: END_HEADERS (0x4) but NOT END_STREAM — a body follows.
        //   :method POST            -> static index 3   (0x83)
        //   :scheme  http           -> static index 6   (0x86)
        //   :authority example.com  -> literal, name = static index 1
        //   :path    /echo          -> literal, name = static index 4
        let mut hpack = Vec::new();
        hpack.push(0x83);
        hpack.push(0x86);
        push_literal(&mut hpack, 1, b"example.com");
        push_literal(&mut hpack, 4, b"/echo");
        push_frame(&mut buf, 0x1, 0x4, 1, &hpack);

        // DATA: END_STREAM (0x1), carrying the request body.
        push_frame(&mut buf, 0x0, 0x1, 1, PAYLOAD);

        io.write_all(&buf).await.expect("write request");
        io.flush().await.expect("flush");

        // Read response frames until stream 1 ends; collect its DATA payloads.
        let mut body = Vec::new();
        loop {
            let mut fh = [0u8; 9];
            io.read_exact(&mut fh).await.expect("read frame header");
            let len = ((fh[0] as usize) << 16) | ((fh[1] as usize) << 8) | fh[2] as usize;
            let ftype = fh[3];
            let flags = fh[4];
            let sid = u32::from_be_bytes([fh[5], fh[6], fh[7], fh[8]]) & 0x7fff_ffff;
            let mut payload = vec![0u8; len];
            io.read_exact(&mut payload).await.expect("read frame payload");
            if sid == 1 && ftype == 0x0 {
                body.extend_from_slice(&payload);
            }
            // END_STREAM on stream 1 (via HEADERS or DATA) => response complete.
            if sid == 1 && (flags & 0x1) != 0 && (ftype == 0x0 || ftype == 0x1) {
                break;
            }
        }

        assert_eq!(
            body, PAYLOAD,
            "request body coalesced with HEADERS into one read was dropped by the server"
        );
    })
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn large_request_body_backpressure() {
    with_timeout(async move {
    // 1 MiB request body: forces WINDOW_UPDATEs driven by consume credits.
    let seen = Arc::new(std::sync::Mutex::new(0usize));
    let seen2 = seen.clone();
    let svc = service_fn(move |req: http::Request<BoxBody>| {
        let seen = seen2.clone();
        async move {
            let bytes = req.into_body().collect().await.unwrap().to_bytes();
            *seen.lock().unwrap() = bytes.len();
            Ok::<_, BoxError>(http::Response::builder().status(200).body(empty()).unwrap())
        }
    });
    let mut tx = connect(svc).await;

    let payload = Bytes::from(vec![3u8; 1024 * 1024]);
    let req = http::Request::builder()
        .method("POST")
        .uri("http://example.com/up")
        .body(BoxBody::new(Full::new(payload).map_err(BoxError::from)))
        .unwrap();
    let rsp = tx.send_request(req).await.expect("send");
    assert_eq!(rsp.status(), 200);
    assert_eq!(*seen.lock().unwrap(), 1024 * 1024);}).await
}
