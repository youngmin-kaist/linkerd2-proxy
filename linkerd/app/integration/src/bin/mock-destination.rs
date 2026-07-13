use futures::{stream, StreamExt as _};
use linkerd2_proxy_api::{destination, destination::destination_server};
use std::{convert::TryInto, net::SocketAddr, pin::Pin};
use tokio_stream::Stream;
use tonic::{transport::Server, Request, Response, Status};

const DEFAULT_ADDR: &str = "127.0.0.1:8089";
const DEFAULT_BACKEND: &str = "127.0.0.1:8086";

type GetStream =
    Pin<Box<dyn Stream<Item = Result<destination::Update, Status>> + Send + Sync + 'static>>;
type GetProfileStream = Pin<
    Box<dyn Stream<Item = Result<destination::DestinationProfile, Status>> + Send + Sync + 'static>,
>;

#[derive(Clone, Debug)]
struct Destination {
    backend: SocketAddr,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = std::env::var("MOCK_DESTINATION_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse::<SocketAddr>()?;
    let backend = std::env::var("MOCK_DESTINATION_BACKEND")
        .unwrap_or_else(|_| DEFAULT_BACKEND.to_string())
        .parse::<SocketAddr>()?;

    eprintln!("mock destination serving on {addr}");
    eprintln!("destination endpoint: {backend}");

    Server::builder()
        .add_service(destination_server::DestinationServer::new(Destination {
            backend,
        }))
        .serve(addr)
        .await?;

    Ok(())
}

#[tonic::async_trait]
impl destination_server::Destination for Destination {
    type GetStream = GetStream;
    type GetProfileStream = GetProfileStream;

    async fn get(
        &self,
        req: Request<destination::GetDestination>,
    ) -> Result<Response<Self::GetStream>, Status> {
        let req = req.into_inner();
        eprintln!("destination get path={} backend={}", req.path, self.backend);

        let update = destination::Update {
            update: Some(destination::update::Update::Add(
                destination::WeightedAddrSet {
                    addrs: vec![destination::WeightedAddr {
                        addr: Some(self.backend.try_into().map_err(|error| {
                            Status::internal(format!("invalid backend address: {error}"))
                        })?),
                        weight: 1,
                        metric_labels: Default::default(),
                        protocol_hint: None,
                        tls_identity: None,
                        authority_override: None,
                        http2: None,
                        resource_ref: None,
                    }],
                    metric_labels: Default::default(),
                },
            )),
        };
        let stream = stream::once(async move { Ok(update) }).chain(stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_profile(
        &self,
        req: Request<destination::GetDestination>,
    ) -> Result<Response<Self::GetProfileStream>, Status> {
        let req = req.into_inner();
        eprintln!("destination get_profile path={}", req.path);
        let profile = destination::DestinationProfile {
            fully_qualified_name: req.path,
            ..Default::default()
        };
        let stream = stream::once(async move { Ok(profile) }).chain(stream::pending());
        Ok(Response::new(Box::pin(stream)))
    }
}
