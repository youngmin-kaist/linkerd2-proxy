//! Gate-A benchmark: our nghttp2 server + a real hyper h2 client over an
//! in-memory duplex, driving the DeathStarBench gRPC request shape. Compare
//! against the all-hyper pair (hpack-h2-bench/src/bin/hyper_bench.rs), which
//! measures the identical workload with hyper on both ends.
//!
//! usage: cargo bench -p linkerd-http-nghttp2 -- [iters] [concurrency]

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use linkerd_error::Error as BoxError;
use linkerd_http_box::BoxBody;
use linkerd_http_h2 as h2c;
use linkerd_http_nghttp2 as ng;
use std::time::Instant;
use tower::service_fn;

fn trace_id(i: usize) -> String {
    let h = |x: usize| -> u64 {
        let mut v = x as u64 ^ 0x9e37_79b9_7f4a_7c15;
        v = v.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        v ^= v >> 27;
        v.wrapping_mul(0x94d0_49bb_1331_11eb)
    };
    format!("{:016x}:{:016x}:{:016x}:{}", h(i), h(i * 3 + 1), h(i * 7 + 2), i & 1)
}

fn empty() -> BoxBody {
    BoxBody::new(Empty::<Bytes>::new().map_err(BoxError::from))
}

fn main() {
    let mut args = std::env::args().skip(1).filter(|a| !a.starts_with('-'));
    let iters: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let m: usize = args.next().and_then(|s| s.parse().ok()).unwrap_or(32);
    let tids: Vec<String> = (0..256).map(trace_id).collect();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let (client_io, server_io) = tokio::io::duplex(1 << 20);
        let mut client_io2 = Some(client_io);
        let client_io = client_io2.take().unwrap();
        let mut client_io2 = Some(client_io);
        let client_io = client_io2.take().unwrap();

        let svc = service_fn(|req: http::Request<BoxBody>| async move {
            let _ = req.into_body().collect().await;
            Ok::<_, BoxError>(
                http::Response::builder()
                    .status(200)
                    .header("content-type", "application/grpc")
                    .body(BoxBody::new(
                        Full::new(Bytes::from_static(b"\0\0\0\0\x05hello"))
                            .map_err(BoxError::from),
                    ))
                    .unwrap(),
            )
        });
        tokio::spawn(async move {
            let _ = ng::server::serve(
                server_io,
                svc,
                h2c::ServerParams {
                    max_concurrent_streams: Some(1024),
                    ..Default::default()
                },
                std::future::pending(),
            )
            .await;
        });

        // Peer selection: NG_CLIENT=1 puts our nghttp2 client on the other end
        // of the duplex, so both sessions are ours — which is what the real
        // proxy does (it terminates a server connection AND originates a
        // client one). Otherwise the peer is a real hyper h2 client.
        let use_ng_client = std::env::var_os("NG_CLIENT").is_some();
        let mut ng_sender = None;
        let mut sender = None;
        if use_ng_client {
            let (c, driver) = ng::client::handshake(client_io, h2c::ClientParams::default());
            tokio::spawn(async move {
                let _ = driver.await;
            });
            ng_sender = Some(c);
        } else {
            let (s, conn) = hyper::client::conn::http2::handshake(
                TokioExecutor::new(),
                TokioIo::new(client_io),
            )
            .await
            .unwrap();
            tokio::spawn(async move {
                let _ = conn.await;
            });
            sender = Some(s);
        }

        let mk = |tid: &str| {
            http::Request::builder()
                .method("POST")
                .uri("http://srv-search/search.Search/Nearby")
                .header("content-type", "application/grpc")
                .header("user-agent", "grpc-go/1.56.3")
                .header("te", "trailers")
                .header("uber-trace-id", tid)
                .body(empty())
                .unwrap()
        };

        // warmup
        for i in 0..256usize {
            if let Some(c) = ng_sender.as_ref() {
                let rsp = c.send_request(mk(&tids[i % 256])).await.unwrap();
                let _ = rsp.into_body().collect().await;
            } else {
                let mut s = sender.clone().unwrap();
                let rsp = s.send_request(mk(&tids[i % 256])).await.unwrap();
                let _ = rsp.into_body().collect().await;
            }
        }

        let guard = std::env::var("PROF").ok().map(|_| {
            pprof::ProfilerGuardBuilder::default()
                .frequency(499)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                .unwrap()
        });

        let per = iters / m;
        let t0 = Instant::now();
        let mut js = Vec::new();
        for w in 0..m {
            let tids = tids.clone();
            let ngc = ng_sender.clone();
            let mut s = sender.clone();
            js.push(tokio::spawn(async move {
                for i in 0..per {
                    let req = mk(&tids[(w * per + i) % 256]);
                    if let Some(c) = ngc.as_ref() {
                        let rsp = c.send_request(req).await.unwrap();
                        let _ = rsp.into_body().collect().await;
                    } else {
                        let rsp = s.as_mut().unwrap().send_request(req).await.unwrap();
                        let _ = rsp.into_body().collect().await;
                    }
                }
            }));
        }
        for j in js {
            j.await.unwrap();
        }
        let el = t0.elapsed();
        let n = per * m;
        let peer = if use_ng_client { "ng-server + ng-client" } else { "ng-server + hyper-client" };
        let peer = if use_ng_client {
            "ng-server + ng-client"
        } else {
            "ng-server + hyper-client"
        };
        println!(
            "{peer:<28} m={m:<4}: {:8.1} ns/req  ({:.0} req/s, n={n})",
            el.as_nanos() as f64 / n as f64,
            n as f64 / el.as_secs_f64()
        );
        if std::env::var_os("NG_STATS").is_some() {
            ng::dump_stats();
        }
        if let (Some(g), Ok(path)) = (guard, std::env::var("PROF")) {
            use pprof::protos::Message;
            let profile = g.report().build().unwrap().pprof().unwrap();
            let mut body = Vec::new();
            profile.write_to_vec(&mut body).unwrap();
            std::fs::write(&path, &body).unwrap();
            println!("profile written: {path}");
        }
    });
}
