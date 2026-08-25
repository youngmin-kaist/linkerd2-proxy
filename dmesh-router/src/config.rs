//! Environment configuration for the router.
//!
//! Deliberately env-only (no CLI): the router is started by the same benchmark
//! scripts that start the proxy, so its knobs are shaped like the proxy's.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;

/// Application protocol spoken to the backend over its DMA channel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackendProto {
    /// HTTP/1.1 keep-alive. One channel serves one request at a time, which is
    /// what linkerd does today for an endpoint with no `h2` protocol hint.
    Http1,
    /// HTTP/2 prior-knowledge (h2c). One channel multiplexes every concurrent
    /// stream; requires the backend to accept h2c (nginx `http2 on;`).
    H2,
}

#[derive(Debug)]
pub struct Config {
    /// DOCA device PCI address on the DPU (the proxy-side function).
    pub dev_pci: String,
    /// Representor PCI address of the host function.
    pub rep_pci: String,
    /// Comch server name; `host_worker.c` connects to `DPUMesh<idx % workers>`.
    pub server_name: String,
    /// Worker threads. 1 (the default) uses the current-thread scheduler.
    pub cores: usize,
    pub backend_proto: BackendProto,
    /// `:authority` (or `Host`) -> backend service key. Empty by default, in
    /// which case a connection routes to its own flow destination.
    pub routes: HashMap<String, SocketAddr>,
    /// Fallback when neither the route table nor the flow destination has a
    /// registered channel.
    pub default_backend: Option<SocketAddr>,
    /// How long a request waits for its backend channel to register.
    pub backend_wait: Duration,
    /// h2 server `SETTINGS_MAX_CONCURRENT_STREAMS` (h2load `-m` must fit).
    pub max_streams: u32,
}

impl Config {
    pub fn from_env() -> Result<Self, String> {
        Ok(Self {
            dev_pci: var("DMESH_ROUTER_DEV_PCI", "03:00.1"),
            rep_pci: var("DMESH_ROUTER_REP_PCI", "94:00.1"),
            server_name: var("DMESH_ROUTER_SERVER", "DPUMesh0"),
            cores: parse("DMESH_ROUTER_CORES", 1)?,
            backend_proto: match var("DMESH_ROUTER_BACKEND_PROTO", "http1").as_str() {
                "http1" | "h1" | "http/1.1" => BackendProto::Http1,
                "h2" | "http2" | "h2c" => BackendProto::H2,
                other => {
                    return Err(format!(
                        "DMESH_ROUTER_BACKEND_PROTO: expected http1 or h2, got '{other}'"
                    ))
                }
            },
            routes: parse_routes(&var("DMESH_ROUTER_ROUTES", ""))?,
            default_backend: match std::env::var("DMESH_ROUTER_DEFAULT_BACKEND") {
                Ok(v) if !v.is_empty() => Some(
                    v.parse()
                        .map_err(|e| format!("DMESH_ROUTER_DEFAULT_BACKEND '{v}': {e}"))?,
                ),
                _ => None,
            },
            backend_wait: Duration::from_millis(parse("DMESH_ROUTER_BACKEND_WAIT_MS", 5000)?),
            max_streams: parse("DMESH_ROUTER_MAX_STREAMS", 1000)?,
        })
    }

    /// Backend key for a request's authority, if the route table names one.
    /// Matches the full `host:port` first, then the bare host.
    pub fn route(&self, authority: Option<&str>) -> Option<SocketAddr> {
        let authority = authority?;
        if let Some(addr) = self.routes.get(authority) {
            return Some(*addr);
        }
        let host = authority.split(':').next()?;
        self.routes.get(host).copied()
    }
}

/// `"svc.example.com=10.0.0.1:8086,other:8080=10.0.0.2:8086"`
fn parse_routes(spec: &str) -> Result<HashMap<String, SocketAddr>, String> {
    let mut routes = HashMap::new();
    for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let (authority, addr) = entry
            .split_once('=')
            .ok_or_else(|| format!("DMESH_ROUTER_ROUTES: '{entry}' is not <authority>=<ip:port>"))?;
        let addr = addr
            .trim()
            .parse()
            .map_err(|e| format!("DMESH_ROUTER_ROUTES: backend of '{entry}': {e}"))?;
        routes.insert(authority.trim().to_string(), addr);
    }
    Ok(routes)
}

fn var(name: &str, default: &str) -> String {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

fn parse<T>(name: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v.parse().map_err(|e| format!("{name} '{v}': {e}")),
        _ => Ok(default),
    }
}
