use linkerd2_proxy_api::identity as pb;
use rustls_pki_types::{pem::PemObject as _, CertificateDer};
use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};
use tonic::{transport::Server, Request, Response, Status};

const DEFAULT_ADDR: &str = "127.0.0.1:8088";
const DEFAULT_IDENTITY: &str = "default-default";

#[derive(Clone, Debug)]
struct Identity {
    response: pb::CertifyResponse,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    linkerd_rustls::install_default_provider();

    let addr = std::env::var("MOCK_IDENTITY_ADDR")
        .unwrap_or_else(|_| DEFAULT_ADDR.to_string())
        .parse::<SocketAddr>()?;
    let data_dir = std::env::var("MOCK_IDENTITY_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            let mut dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            dir.push("src/data");
            dir
        });
    let identity = std::env::var("MOCK_IDENTITY_FIXTURE")
        .unwrap_or_else(|_| DEFAULT_IDENTITY.to_string());

    let cert_path = data_dir.join(&identity).join("ca1-cert.pem");
    let response = load_certify_response(&cert_path)?;
    let svc = Identity { response };

    eprintln!("mock identity serving on {addr}");
    eprintln!("using certificate fixture {}", cert_path.display());

    Server::builder()
        .add_service(pb::identity_server::IdentityServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}

fn load_certify_response(path: &Path) -> Result<pb::CertifyResponse, Box<dyn std::error::Error>> {
    let mut certs = CertificateDer::pem_file_iter(path)?;
    let leaf = certs
        .next()
        .ok_or_else(|| format!("{} contains no certificates", path.display()))??
        .to_vec();
    let intermediate_certificates = certs
        .map(|cert| cert.map(|cert| cert.to_vec()))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(pb::CertifyResponse {
        leaf_certificate: leaf,
        intermediate_certificates,
        valid_until: Some((SystemTime::now() + Duration::from_secs(24 * 60 * 60)).into()),
    })
}

#[tonic::async_trait]
impl pb::identity_server::Identity for Identity {
    async fn certify(
        &self,
        req: Request<pb::CertifyRequest>,
    ) -> Result<Response<pb::CertifyResponse>, Status> {
        let req = req.into_inner();
        eprintln!("certifying identity {}", req.identity);
        Ok(Response::new(self.response.clone()))
    }
}
