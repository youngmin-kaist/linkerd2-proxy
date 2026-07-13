use futures::{stream, StreamExt as _};
use linkerd_app_integration::policy;
use linkerd2_proxy_api::{
    destination,
    inbound::{self, inbound_server_policies_server},
    outbound::{self, outbound_policies_server},
};
use std::{net::SocketAddr, pin::Pin};
use tokio_stream::Stream;
use tonic::{transport::Server, Request, Response, Status};

const DEFAULT_ADDR: &str = "127.0.0.1:8087";

type InboundStream =
    Pin<Box<dyn Stream<Item = Result<inbound::Server, Status>> + Send + Sync + 'static>>;
type OutboundStream =
    Pin<Box<dyn Stream<Item = Result<outbound::OutboundPolicy, Status>> + Send + Sync + 'static>>;

#[derive(Clone, Debug)]
struct Policy {
    inbound: inbound::Server,
    backend: SocketAddr,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("MOCK_POLICY_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse::<SocketAddr>()?;
    let backend = std::env::var("MOCK_POLICY_BACKEND")
        .unwrap_or_else(|_| "127.0.0.1:8086".to_string())
        .parse::<SocketAddr>()?;

    let svc = Policy {
        inbound: policy::all_unauthenticated(),
        backend,
    };

    eprintln!("mock policy serving on {addr}");
    eprintln!("inbound policy: all-unauthenticated");
    eprintln!("outbound policy: forward to {backend}");

    Server::builder()
        .add_service(inbound_server_policies_server::InboundServerPoliciesServer::new(
            svc.clone(),
        ))
        .add_service(outbound_policies_server::OutboundPoliciesServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}

#[tonic::async_trait]
impl inbound_server_policies_server::InboundServerPolicies for Policy {
    type WatchPortStream = InboundStream;

    async fn get_port(
        &self,
        req: Request<inbound::PortSpec>,
    ) -> Result<Response<inbound::Server>, Status> {
        let req = req.into_inner();
        eprintln!(
            "inbound get_port workload={} port={}",
            req.workload, req.port
        );
        Ok(Response::new(self.inbound.clone()))
    }

    async fn watch_port(
        &self,
        req: Request<inbound::PortSpec>,
    ) -> Result<Response<Self::WatchPortStream>, Status> {
        let req = req.into_inner();
        eprintln!(
            "inbound watch_port workload={} port={}",
            req.workload, req.port
        );
        let policy = self.inbound.clone();
        let stream = stream::once(async move { Ok(policy) }).chain(stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }
}

#[tonic::async_trait]
impl outbound_policies_server::OutboundPolicies for Policy {
    type WatchStream = OutboundStream;

    async fn get(
        &self,
        req: Request<outbound::TrafficSpec>,
    ) -> Result<Response<outbound::OutboundPolicy>, Status> {
        let req = req.into_inner();
        eprintln!("outbound get target={:?} backend={}", req.target, self.backend);
        Ok(Response::new(forward_policy(self.backend)))
    }

    async fn watch(
        &self,
        req: Request<outbound::TrafficSpec>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        let req = req.into_inner();
        eprintln!(
            "outbound watch target={:?} backend={}",
            req.target, self.backend
        );
        let policy = forward_policy(self.backend);
        let stream = stream::once(async move { Ok(policy) }).chain(stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }
}

fn forward_policy(addr: SocketAddr) -> outbound::OutboundPolicy {
    let mut policy = policy::outbound_default("local-backend.example:80");

    if let Some(outbound::ProxyProtocol {
        kind: Some(outbound::proxy_protocol::Kind::Detect(detect)),
    }) = policy.protocol.as_mut()
    {
        if let Some(http1) = detect.http1.as_mut() {
            for route in &mut http1.routes {
                replace_http_route_backends(route, addr);
            }
        }

        if let Some(http2) = detect.http2.as_mut() {
            for route in &mut http2.routes {
                replace_http_route_backends(route, addr);
            }
        }

        if let Some(opaque) = detect.opaque.as_mut() {
            for route in &mut opaque.routes {
                replace_opaque_route_backends(route, addr);
            }
        }
    }

    policy
}

fn replace_http_route_backends(route: &mut outbound::HttpRoute, addr: SocketAddr) {
    for rule in &mut route.rules {
        let Some(outbound::http_route::Distribution {
            kind:
                Some(outbound::http_route::distribution::Kind::FirstAvailable(first_available)),
        }) = rule.backends.as_mut()
        else {
            continue;
        };

        for backend in &mut first_available.backends {
            if let Some(backend) = backend.backend.as_mut() {
                replace_backend(backend, addr);
            }
        }
    }
}

fn replace_opaque_route_backends(route: &mut outbound::OpaqueRoute, addr: SocketAddr) {
    for rule in &mut route.rules {
        let Some(outbound::opaque_route::Distribution {
            kind:
                Some(outbound::opaque_route::distribution::Kind::FirstAvailable(first_available)),
        }) = rule.backends.as_mut()
        else {
            continue;
        };

        for backend in &mut first_available.backends {
            if let Some(backend) = backend.backend.as_mut() {
                replace_backend(backend, addr);
            }
        }
    }
}

fn replace_backend(backend: &mut outbound::Backend, addr: SocketAddr) {
    backend.kind = Some(outbound::backend::Kind::Forward(destination::WeightedAddr {
        addr: Some(addr.try_into().expect("socket addr must convert to protobuf")),
        weight: 1,
        metric_labels: Default::default(),
        protocol_hint: None,
        tls_identity: None,
        authority_override: None,
        http2: None,
        resource_ref: None,
    }));
}
