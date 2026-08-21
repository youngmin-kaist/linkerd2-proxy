mod api;
mod config;
pub mod defaults;
mod http;
mod store;
mod tcp;

use crate::metrics::authz::HTTPLocalRateLimitLabels;

pub(crate) use self::store::Store;
pub use self::{
    config::Config,
    http::{
        HttpInvalidPolicy, HttpRouteInvalidRedirect, HttpRouteNotFound, HttpRouteRedirect,
        HttpRouteUnauthorized, NewHttpPolicy, PermitVariant, Permitted,
    },
    tcp::NewTcpPolicy,
};

pub use linkerd_app_core::metrics::ServerLabel;
use linkerd_app_core::{
    identity as id,
    metrics::{RouteAuthzLabels, ServerAuthzLabels},
    tls,
    transport::{ClientAddr, OrigDstAddr, Remote},
};
use linkerd_idle_cache::Cached;
pub use linkerd_proxy_server_policy::{
    authz::Suffix,
    grpc::Route as GrpcRoute,
    http::{filter::Redirection, Route as HttpRoute},
    route, Authentication, Authorization, Meta, Protocol, RateLimitError, RoutePolicy,
    ServerPolicy,
};
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::watch;

#[derive(Clone, Debug, Error)]
#[error("unauthorized connection on {}/{}", server.kind(), server.name())]
pub struct ServerUnauthorized {
    server: Arc<Meta>,
}

pub trait GetPolicy {
    /// Returns the traffic policy configured for the destination address.
    fn get_policy(&self, dst: OrigDstAddr) -> AllowPolicy;
}

/// One policy store may be held behind a pointer, so that a caller which binds
/// a store per workload rather than per process can keep them in a map.
impl<T: GetPolicy + ?Sized> GetPolicy for Arc<T> {
    fn get_policy(&self, dst: OrigDstAddr) -> AllowPolicy {
        (**self).get_policy(dst)
    }
}

#[derive(Clone, Debug)]
pub enum DefaultPolicy {
    Allow(ServerPolicy),
    Deny,
}

#[derive(Clone, Debug)]
pub struct AllowPolicy {
    dst: OrigDstAddr,
    server: Cached<watch::Receiver<ServerPolicy>>,
}

/// Describes an authorized non-HTTP connection.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServerPermit {
    pub dst: OrigDstAddr,
    pub protocol: Protocol,
    pub labels: ServerAuthzLabels,
}

/// Describes an authorized HTTP request.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct HttpRoutePermit {
    pub dst: OrigDstAddr,
    pub labels: RouteAuthzLabels,
}

pub enum Routes {
    Http(Arc<[HttpRoute]>),
    Grpc(Arc<[GrpcRoute]>),
}

// === impl DefaultPolicy ===

impl From<ServerPolicy> for DefaultPolicy {
    fn from(p: ServerPolicy) -> Self {
        DefaultPolicy::Allow(p)
    }
}

impl From<DefaultPolicy> for ServerPolicy {
    fn from(d: DefaultPolicy) -> Self {
        match d {
            DefaultPolicy::Allow(p) => p,
            DefaultPolicy::Deny => ServerPolicy {
                protocol: Protocol::Opaque(Arc::new([])),
                local_rate_limit: Default::default(),
                meta: Meta::new_default("deny"),
            },
        }
    }
}

// === impl AllowPolicy ===

impl AllowPolicy {
    #[cfg(any(test, fuzzing, feature = "test-util"))]
    pub fn for_test(dst: OrigDstAddr, server: ServerPolicy) -> (Self, watch::Sender<ServerPolicy>) {
        let (tx, server) = watch::channel(server);
        let server = Cached::uncached(server);
        let p = Self { dst, server };
        (p, tx)
    }

    #[inline]
    pub(crate) fn borrow(&self) -> tokio::sync::watch::Ref<'_, ServerPolicy> {
        self.server.borrow()
    }

    #[inline]
    pub(crate) fn protocol(&self) -> Protocol {
        self.server.borrow().protocol.clone()
    }

    #[inline]
    pub fn dst_addr(&self) -> OrigDstAddr {
        self.dst
    }

    #[inline]
    pub fn meta(&self) -> Arc<Meta> {
        self.server.borrow().meta.clone()
    }

    #[inline]
    pub fn server_label(&self) -> ServerLabel {
        ServerLabel(self.server.borrow().meta.clone(), self.dst.port())
    }

    pub fn ratelimit_label(&self, error: &RateLimitError) -> HTTPLocalRateLimitLabels {
        use RateLimitError::*;

        let scope = match error {
            Total(_) => "total",
            PerIdentity(_) | Override(_) => "identity",
        };
        HTTPLocalRateLimitLabels {
            server: self.server_label(),
            rate_limit: self.server.borrow().local_rate_limit.meta(),
            scope,
        }
    }

    async fn changed(&mut self) {
        if self.server.changed().await.is_err() {
            // If the sender was dropped, then there can be no further changes.
            futures::future::pending::<()>().await;
        }
    }

    /// Whether the policy currently in effect admits a connection from
    /// `client`, without building a stack for it.
    ///
    /// A destination that terminates no stream still has to decide admission,
    /// and `connection_verdict` is the evaluation it must use — the same one
    /// the stack applies, against the same watched policy, so the two cannot
    /// disagree.
    pub fn admits(&self, client: Remote<ClientAddr>, tls: &tls::ConditionalServerTls) -> bool {
        connection_verdict(&self.server.borrow(), client, tls)
    }

    fn routes(&self) -> Option<Routes> {
        let borrow = self.server.borrow();
        match &borrow.protocol {
            Protocol::Detect { http, .. } | Protocol::Http1(http) | Protocol::Http2(http) => {
                Some(Routes::Http(http.clone()))
            }
            Protocol::Grpc(grpc) => Some(Routes::Grpc(grpc.clone())),
            _ => None,
        }
    }
}

fn is_tls_authorized(tls: &tls::ConditionalServerTls, authz: &Authorization) -> bool {
    match authz.authentication {
        Authentication::Unauthenticated => true,

        Authentication::TlsUnauthenticated => {
            matches!(
                tls,
                tls::ConditionalServerTls::Some(tls::ServerTls::Established { .. })
            )
        }

        Authentication::TlsAuthenticated {
            ref identities,
            ref suffixes,
        } => match tls {
            tls::ConditionalServerTls::Some(tls::ServerTls::Established {
                client_id: Some(tls::server::ClientId(ref id)),
                ..
            }) => match id {
                id::Id::Uri(_) => identities.contains(&*id.to_str()),
                id::Id::Dns(_) => {
                    identities.contains(&*id.to_str())
                        || suffixes.iter().any(|s| s.contains(&id.to_str()))
                }
            },
            _ => false,
        },
    }
}

fn is_authorized(
    authz: &Authorization,
    client_addr: Remote<ClientAddr>,
    tls: &tls::ConditionalServerTls,
) -> bool {
    if !authz.networks.iter().any(|n| n.contains(&client_addr.ip())) {
        return false;
    }

    is_tls_authorized(tls, authz)
}

/// Whether this policy admits a connection from `client`, without building a
/// proxy for it.
///
/// A destination that terminates no stream still has to decide admission, and
/// the evaluation it must use is this crate's own — the authorization types
/// and their matching rules are private here, and reimplementing them outside
/// would be a second policy engine that could disagree with this one.
///
/// The verdict is per protocol variant, because that is how the stock policy
/// carries its authorizations. `Detect`, `Tls` and `Opaque` name a
/// connection-level list. The HTTP variants name none: they carry only
/// per-route authorizations, so the connection verdict is the union — the
/// connection is refused exactly when no route could ever admit this client.
/// Route-level differences are not enforced, which over-admits in the same way
/// and for the same reason a connection-level enforcement point elsewhere
/// does; enforcing them needs a second parser.
pub fn connection_verdict(
    policy: &ServerPolicy,
    client: Remote<ClientAddr>,
    tls: &tls::ConditionalServerTls,
) -> bool {
    fn any_route_admits<M, F>(
        routes: &[route::Route<M, RoutePolicy<F>>],
        client: Remote<ClientAddr>,
        tls: &tls::ConditionalServerTls,
    ) -> bool {
        routes.iter().any(|route| {
            route.rules.iter().any(|rule| {
                rule.policy
                    .authorizations
                    .iter()
                    .any(|authz| is_authorized(authz, client, tls))
            })
        })
    }

    match &policy.protocol {
        Protocol::Detect {
            tcp_authorizations, ..
        } => tcp_authorizations
            .iter()
            .any(|authz| is_authorized(authz, client, tls)),
        Protocol::Opaque(authorizations) | Protocol::Tls(authorizations) => authorizations
            .iter()
            .any(|authz| is_authorized(authz, client, tls)),
        Protocol::Http1(routes) | Protocol::Http2(routes) => any_route_admits(routes, client, tls),
        Protocol::Grpc(routes) => any_route_admits(routes, client, tls),
    }
}

// === impl Permit ===

impl ServerPermit {
    fn new(dst: OrigDstAddr, server: &ServerPolicy, authz: &Authorization) -> Self {
        Self {
            dst,
            protocol: server.protocol.clone(),
            labels: ServerAuthzLabels {
                authz: authz.meta.clone(),
                server: ServerLabel(server.meta.clone(), dst.port()),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::is_tls_authorized;
    use super::Meta;
    use super::Suffix;
    use super::{connection_verdict, Authentication, Authorization};
    use linkerd_app_core::{
        tls,
        transport::{ClientAddr, Remote},
    };
    use linkerd_proxy_server_policy::{
        http::{Filter as HttpFilter, Route as HttpRoute},
        route, LocalRateLimit, Protocol, RoutePolicy, ServerPolicy,
    };
    use std::collections::BTreeSet;
    use std::str::FromStr;
    use std::sync::Arc;

    fn meta() -> Arc<Meta> {
        Arc::new(Meta::Default {
            name: "test".into(),
        })
    }

    fn admits_everyone() -> Authorization {
        Authorization {
            networks: vec![Default::default()],
            meta: meta(),
            authentication: Authentication::Unauthenticated,
        }
    }

    fn admits_nobody() -> Authorization {
        Authorization {
            networks: vec![],
            meta: meta(),
            authentication: Authentication::Unauthenticated,
        }
    }

    fn one_route(authorizations: Vec<Authorization>) -> Arc<[HttpRoute]> {
        std::iter::once(route::Route {
            hosts: Vec::new(),
            rules: vec![route::Rule {
                matches: Vec::new(),
                policy: RoutePolicy::<HttpFilter> {
                    meta: meta(),
                    authorizations: authorizations.into(),
                    filters: Vec::new(),
                },
            }],
        })
        .collect()
    }

    fn policy(protocol: Protocol) -> ServerPolicy {
        ServerPolicy {
            protocol,
            meta: meta(),
            local_rate_limit: Arc::new(LocalRateLimit::default()),
        }
    }

    /// The connection verdict is per protocol variant, because that is how a
    /// `ServerPolicy` carries its authorizations. The HTTP variants name no
    /// connection-level list, so the verdict is the union of their routes':
    /// the connection is refused exactly when no route could ever admit this
    /// client. Enforcing route-level *differences* would need a second parser.
    #[test]
    fn connection_verdict_is_per_protocol_variant() {
        let client = Remote(ClientAddr("10.244.0.7:4001".parse().unwrap()));
        let tls = tls::ConditionalServerTls::Some(tls::ServerTls::Established {
            client_id: None,
            negotiated_protocol: None,
        });

        let opaque = |a| policy(Protocol::Opaque(vec![a].into()));
        assert!(connection_verdict(&opaque(admits_everyone()), client, &tls));
        assert!(!connection_verdict(&opaque(admits_nobody()), client, &tls));

        let detect = |a| {
            policy(Protocol::Detect {
                http: one_route(vec![]),
                timeout: std::time::Duration::from_secs(1),
                tcp_authorizations: vec![a].into(),
            })
        };
        assert!(connection_verdict(&detect(admits_everyone()), client, &tls));
        assert!(!connection_verdict(&detect(admits_nobody()), client, &tls));

        // An HTTP variant with no route admits nobody: there is no
        // authorization anywhere that could.
        assert!(!connection_verdict(
            &policy(Protocol::Http1(Arc::from(Vec::new()))),
            client,
            &tls
        ));
        // One route that admits is enough — the union, and the over-admission
        // the source/destination split accepts.
        assert!(connection_verdict(
            &policy(Protocol::Http2(one_route(vec![
                admits_nobody(),
                admits_everyone()
            ]))),
            client,
            &tls
        ));
        assert!(!connection_verdict(
            &policy(Protocol::Http1(one_route(vec![admits_nobody()]))),
            client,
            &tls
        ));
    }

    fn authorization(identities: BTreeSet<String>, suffixes: Vec<Suffix>) -> Authorization {
        Authorization {
            networks: vec![],
            meta: Arc::new(Meta::Default {
                name: "name".into(),
            }),
            authentication: Authentication::TlsAuthenticated {
                identities,
                suffixes,
            },
        }
    }

    fn server_tls(identity: &str) -> tls::ConditionalServerTls {
        let client_id = tls::ClientId::from_str(identity).expect("should parse id");
        tls::ConditionalServerTls::Some(tls::ServerTls::Established {
            client_id: Some(client_id),
            negotiated_protocol: None,
        })
    }

    #[test]
    fn is_authorized_for_matching_spiffe_ids() {
        let tls = server_tls("spiffe://some-root/some-workload");
        let authz = authorization(
            BTreeSet::from(["spiffe://some-root/some-workload".into()]),
            vec![],
        );
        assert!(is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_not_authorized_for_non_matching_spiffe_ids() {
        let tls = server_tls("spiffe://some-root/some-workload-1");
        let authz = authorization(
            BTreeSet::from(["spiffe://some-root/some-workload-2".into()]),
            vec![],
        );
        assert!(!is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_authorized_for_matching_dns_ids() {
        let tls = server_tls("some.id.local");
        let authz = authorization(BTreeSet::from(["some.id.local".into()]), vec![]);
        assert!(is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_not_authorized_for_non_matching_dns_ids() {
        let tls = server_tls("some.id.local.one");
        let authz = authorization(BTreeSet::from(["some.id.local.two".into()]), vec![]);
        assert!(!is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_authorized_for_matching_dns_suffixes_ids() {
        let tls = server_tls("some.id.local");
        let authz = authorization(BTreeSet::new(), vec![Suffix::new("id.local")]);
        assert!(is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_not_authorized_for_non_matching_suffixes_ids() {
        let tls = server_tls("some.id.local");
        let authz = authorization(BTreeSet::new(), vec![Suffix::new("another-id.local")]);
        assert!(!is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_not_authorized_for_suffixes_and_spiffe_id() {
        let tls = server_tls("spiffe://some-root/some-workload-1");
        let authz = authorization(BTreeSet::new(), vec![Suffix::new("some-workload-1")]);
        assert!(!is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_authorized_for_one_matching_spiffe_id() {
        let tls = server_tls("spiffe://some-root/some-workload-1");
        let authz = authorization(
            BTreeSet::from([
                "spiffe://some-root/some-workload-1".into(),
                "some.workload.one".into(),
                "some.workload.two".into(),
            ]),
            vec![],
        );
        assert!(is_tls_authorized(&tls, &authz))
    }

    #[test]
    fn is_authorized_for_one_matching_dns_id() {
        let tls = server_tls("some.workload.one");
        let authz = authorization(
            BTreeSet::from([
                "spiffe://some-root/some-workload-1".into(),
                "some.workload.one".into(),
                "some.workload.two".into(),
            ]),
            vec![],
        );
        assert!(is_tls_authorized(&tls, &authz))
    }
}
