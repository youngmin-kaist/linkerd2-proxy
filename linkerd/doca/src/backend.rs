//! Registry of DMA backend channels.
//!
//! A DPUmesh session publishes the endpoint that provides its service here, and
//! the outbound connector takes it instead of dialing TCP. Entries are keyed by
//! [`BackendKey`], so a closing session evicts its own channel and cannot
//! withdraw the one a later session published for the same service.
//!
//! The registry is owned, not global: each ARM worker builds one and hands it
//! to its adapter and its connector, so no lock is shared between workers.

use crate::{api::SessionToken, DmeshIo};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;
use std::sync::Arc;

/// What an address the balancer selected resolves to.
///
/// DPUmesh chooses the backend Pod itself on the data path, so a selected
/// endpoint is translated rather than dialled. Every outcome other than
/// `Live` and `SessionOwn` declines the connection: a round robin or a TCP
/// fallback would carry a protected stream somewhere its policy never named.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointVerdict {
    /// The session's own service address; there is no endpoint to resolve.
    SessionOwn,
    /// A live registration serves it.
    Live,
    /// No live registration serves it.
    Unresolved,
    /// The generation places it on another node.
    Remote,
    /// The mapping predates the held generation.
    Stale,
}

/// Resolves an address the balancer selected to a live destination.
pub type EndpointResolver = Arc<dyn Fn(SocketAddr) -> EndpointVerdict + Send + Sync>;

/// Identifies one session's backend channel.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BackendKey {
    pub worker: u16,
    pub service: SocketAddr,
    pub session: SessionToken,
}

impl BackendKey {
    pub fn new(service: SocketAddr, session: SessionToken) -> Self {
        Self {
            worker: session.worker,
            service,
            session,
        }
    }
}

impl fmt::Display for BackendKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}[{}]", self.service, self.session)
    }
}

/// Why a channel could not be published.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublishError {
    /// The same key is already live. Publishing would drop a channel a session
    /// still owns.
    AlreadyLive,
}

/// Why a channel could not be taken.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TakeError {
    /// No session provides this service.
    NotPublished,
    /// The channel was already handed to a connector.
    AlreadyTaken,
    /// The newest signed Service snapshot places the endpoint Linkerd selected
    /// in a different Service than the session's.
    TargetMismatch,
    /// No live registration serves the selected endpoint.
    EndpointUnresolved,
    /// The generation places the selected endpoint on another node.
    EndpointRemote,
    /// The mapping from the selected endpoint to a Pod predates the held
    /// generation.
    EndpointStale,
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyLive => write!(f, "a live dmesh backend channel holds this key"),
        }
    }
}

impl fmt::Display for TakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPublished => write!(f, "no dmesh backend channel for this service"),
            Self::AlreadyTaken => write!(f, "the dmesh backend channel was already taken"),
            Self::TargetMismatch => {
                write!(f, "the selected endpoint belongs to another dmesh Service")
            }
            Self::EndpointUnresolved => {
                write!(f, "no live registration serves the selected endpoint")
            }
            Self::EndpointRemote => write!(f, "the selected endpoint is placed on another node"),
            Self::EndpointStale => write!(
                f,
                "the selected endpoint's mapping predates the held generation"
            ),
        }
    }
}

impl std::error::Error for PublishError {}
impl std::error::Error for TakeError {}

#[derive(Debug)]
struct Entry {
    service: SocketAddr,
    /// `None` once a connector has taken the channel.
    io: Option<DmeshIo>,
}

#[derive(Default)]
struct Registry {
    /// Preserves each service's publication order for diagnostics.
    by_service: HashMap<SocketAddr, Vec<SessionToken>>,
    /// Resolves a generation-safe session directly.
    by_session: HashMap<SessionToken, Entry>,
    /// Which Service each address the newest signed generation names belongs
    /// to. Replaced wholesale, so an address a generation drops is unplaced.
    service_of_target: HashMap<SocketAddr, SocketAddr>,
    /// Resolves a selected endpoint to a live destination. Absent until the
    /// adapter installs one, in which case every placed address is taken as
    /// this session's, which is the behaviour before endpoints were
    /// authoritative.
    #[allow(clippy::type_complexity)]
    endpoints: Option<EndpointResolver>,
}

impl fmt::Debug for Registry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Registry")
            .field("by_service", &self.by_service)
            .field("by_session", &self.by_session)
            .field("service_of_target", &self.service_of_target)
            .field("endpoints", &self.endpoints.is_some())
            .finish()
    }
}

/// Backend channels published by the sessions of one worker.
#[derive(Debug, Default)]
pub struct Backends {
    registry: Mutex<Registry>,
}

impl Backends {
    pub fn new() -> Self {
        Self::default()
    }

    fn registry(&self) -> parking_lot::MutexGuard<'_, Registry> {
        self.registry.lock()
    }

    /// Offer a session's channel. A duplicate live key is refused rather than
    /// silently replacing the endpoint another session still owns.
    pub fn publish(&self, key: BackendKey, io: DmeshIo) -> Result<(), PublishError> {
        let mut registry = self.registry();
        if registry.by_session.contains_key(&key.session) {
            return Err(PublishError::AlreadyLive);
        }
        registry
            .by_service
            .entry(key.service)
            .or_default()
            .push(key.session);
        registry.by_session.insert(
            key.session,
            Entry {
                service: key.service,
                io: Some(io),
            },
        );
        Ok(())
    }

    /// Take the backend channel owned by one session.
    ///
    /// Linkerd discovery may replace the Service ClusterIP with a concrete
    /// endpoint, but it may not move a DMesh session to another Service. The
    /// newest signed generation decides: an address it places in another
    /// Service is a mismatch.
    ///
    /// Once endpoints are authoritative, an address the snapshot does not
    /// place is no longer assumed to be this session's. The session's own
    /// addresses resolve as such, and everything else must resolve to a live
    /// destination — an endpoint that does not is declined by the reason it
    /// failed for, never round-robined and never dialled over TCP.
    pub fn take_session(
        &self,
        session: SessionToken,
        selected_target: SocketAddr,
    ) -> Result<DmeshIo, TakeError> {
        let mut registry = self.registry();
        let Some(entry) = registry.by_session.get(&session) else {
            return Err(TakeError::NotPublished);
        };
        let service = entry.service;
        if registry
            .service_of_target
            .get(&selected_target)
            .is_some_and(|placed| *placed != service)
        {
            return Err(TakeError::TargetMismatch);
        }
        if let Some(resolve) = registry.endpoints.clone() {
            match resolve(selected_target) {
                EndpointVerdict::SessionOwn | EndpointVerdict::Live => {}
                EndpointVerdict::Unresolved => return Err(TakeError::EndpointUnresolved),
                EndpointVerdict::Remote => return Err(TakeError::EndpointRemote),
                EndpointVerdict::Stale => return Err(TakeError::EndpointStale),
            }
        }
        let entry = registry
            .by_session
            .get_mut(&session)
            .expect("the session entry was just read");
        entry.io.take().ok_or(TakeError::AlreadyTaken)
    }

    /// Install the newest signed view of which Service each address belongs to.
    /// Live sessions are judged against it, so a generation adopted after a
    /// session opened still governs that session's dial.
    pub fn place_targets(&self, placements: impl IntoIterator<Item = (SocketAddr, SocketAddr)>) {
        self.registry().service_of_target = placements.into_iter().collect();
    }

    /// Install the resolver that translates a selected endpoint to a live
    /// destination. Applied to every subsequent take, including one for a
    /// session that opened before it.
    pub fn set_endpoint_resolver(&self, resolve: EndpointResolver) {
        self.registry().endpoints = Some(resolve);
    }

    /// Evict exactly one session's entry, whether or not it was taken.
    ///
    /// A close must run this before the next generation publishes, so the new
    /// session's channel is the only one on offer.
    pub fn remove(&self, key: &BackendKey) -> Option<DmeshIo> {
        let mut registry = self.registry();
        if registry.by_session.get(&key.session)?.service != key.service {
            return None;
        }
        let io = registry.by_session.remove(&key.session)?.io;
        let sessions = registry
            .by_service
            .get_mut(&key.service)
            .expect("session entry must name a published service");
        let idx = sessions
            .iter()
            .position(|session| *session == key.session)
            .expect("session entry must be present in its service index");
        sessions.remove(idx);
        let service_empty = sessions.is_empty();
        if service_empty {
            registry.by_service.remove(&key.service);
        }
        io
    }

    /// True while any session provides this service.
    pub fn contains_service(&self, service: &SocketAddr) -> bool {
        self.registry().by_service.contains_key(service)
    }

    /// True when DPUmesh provides the address: it names a published Service or
    /// an endpoint the newest signed generation places. A sessionless dial to
    /// such an address must be refused rather than fall through to TCP.
    pub fn manages(&self, addr: &SocketAddr) -> bool {
        let registry = self.registry();
        registry.by_service.contains_key(addr) || registry.service_of_target.contains_key(addr)
    }

    /// Sessions currently providing a service, in publication order.
    pub fn sessions_for(&self, service: &SocketAddr) -> Vec<SessionToken> {
        self.registry()
            .by_service
            .get(service)
            .cloned()
            .unwrap_or_default()
    }

    /// Entries held for all services.
    pub fn len(&self) -> usize {
        self.registry().by_session.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dmesh_io_pair;

    fn addr(last: u8) -> SocketAddr {
        SocketAddr::from(([10, 96, 0, last], 9092))
    }

    fn io(peer: SocketAddr) -> DmeshIo {
        dmesh_io_pair(peer, None).0
    }

    #[test]
    fn take_names_the_publishing_session() {
        let backends = Backends::new();
        let service = addr(11);
        let first = SessionToken::new(0, 0, 0);
        let second = SessionToken::new(0, 1, 0);

        backends
            .publish(BackendKey::new(service, first), io(service))
            .unwrap();
        backends
            .publish(BackendKey::new(service, second), io(service))
            .unwrap();
        assert_eq!(backends.sessions_for(&service).len(), 2);

        backends.take_session(second, service).unwrap();
        assert_eq!(
            backends.take_session(second, service).err(),
            Some(TakeError::AlreadyTaken)
        );
        backends.take_session(first, service).unwrap();
    }

    #[test]
    fn a_duplicate_live_key_is_refused() {
        let backends = Backends::new();
        let service = addr(12);
        let key = BackendKey::new(service, SessionToken::new(0, 0, 0));
        backends.publish(key, io(service)).unwrap();
        assert_eq!(
            backends.publish(key, io(service)),
            Err(PublishError::AlreadyLive)
        );
    }

    #[test]
    fn a_closed_generation_evicts_only_itself() {
        let backends = Backends::new();
        let service = addr(13);
        let old = BackendKey::new(service, SessionToken::new(0, 0, 0));
        let new = BackendKey::new(service, SessionToken::new(0, 0, 1));

        backends.publish(old, io(service)).unwrap();
        assert!(backends.remove(&old).is_some());
        assert!(!backends.contains_service(&service));

        backends.publish(new, io(service)).unwrap();
        assert_eq!(
            backends.take_session(old.session, service).err(),
            Some(TakeError::NotPublished)
        );
        // The closed generation's second close must not withdraw the live one.
        assert!(backends.remove(&old).is_none());
        backends.take_session(new.session, service).unwrap();
    }

    #[test]
    fn an_unpublished_session_has_no_channel() {
        let backends = Backends::new();
        let service = addr(14);
        let key = BackendKey::new(service, SessionToken::new(0, 0, 0));
        assert_eq!(
            backends.take_session(key.session, service).err(),
            Some(TakeError::NotPublished)
        );
        backends.publish(key, io(service)).unwrap();
        backends.take_session(key.session, service).unwrap();
        assert_eq!(
            backends.take_session(key.session, service).err(),
            Some(TakeError::AlreadyTaken)
        );
        backends.remove(&key);
        assert!(backends.is_empty());
    }

    #[test]
    fn discovery_address_does_not_replace_session_ownership() {
        let backends = Backends::new();
        let service = addr(13);
        let endpoint = SocketAddr::from(([10, 244, 0, 13], 9092));
        let other_service = addr(99);
        let other_endpoint = SocketAddr::from(([10, 244, 0, 99], 9092));
        let token = SessionToken::new(2, 7, 4);
        backends
            .publish(BackendKey::new(service, token), io(service))
            .unwrap();
        backends.place_targets([
            (endpoint, service),
            (other_service, other_service),
            (other_endpoint, other_service),
        ]);

        assert_eq!(
            backends.take_session(token, other_service).err(),
            Some(TakeError::TargetMismatch)
        );
        assert_eq!(
            backends.take_session(token, other_endpoint).err(),
            Some(TakeError::TargetMismatch)
        );
        backends.take_session(token, endpoint).unwrap();
        assert_eq!(
            backends.take_session(token, endpoint).err(),
            Some(TakeError::AlreadyTaken)
        );
    }

    #[test]
    fn an_unplaced_endpoint_is_this_session() {
        let backends = Backends::new();
        let service = addr(13);
        let known = SocketAddr::from(([10, 244, 0, 13], 9092));
        let unplaced = SocketAddr::from(([10, 244, 0, 36], 9092));
        let token = SessionToken::new(1, 1, 1);
        backends
            .publish(BackendKey::new(service, token), io(service))
            .unwrap();
        backends.place_targets([(known, service)]);

        backends.take_session(token, unplaced).unwrap();
    }

    #[test]
    fn a_later_generation_governs_a_live_session() {
        let backends = Backends::new();
        let service = addr(13);
        let moved = SocketAddr::from(([10, 244, 0, 36], 9092));
        let token = SessionToken::new(1, 2, 3);
        backends
            .publish(BackendKey::new(service, token), io(service))
            .unwrap();
        backends.place_targets([(moved, addr(99))]);

        assert_eq!(
            backends.take_session(token, moved).err(),
            Some(TakeError::TargetMismatch)
        );
    }

    #[test]
    fn session_lookup_uses_the_direct_service_index() {
        let backends = Backends::new();
        let wanted = SessionToken::new(2, 7, 4);
        for slot in 0..8 {
            let service = addr(20 + slot);
            let token = SessionToken::new(2, slot as u32, 4);
            backends
                .publish(BackendKey::new(service, token), io(service))
                .unwrap();
        }

        let wanted_key = BackendKey::new(addr(27), wanted);
        backends.take_session(wanted, wanted_key.service).unwrap();
        backends.remove(&wanted_key);
        assert_eq!(
            backends.take_session(wanted, wanted_key.service).err(),
            Some(TakeError::NotPublished)
        );
        assert_eq!(backends.len(), 7);
    }

    #[test]
    fn one_session_cannot_publish_under_two_services() {
        let backends = Backends::new();
        let token = SessionToken::new(1, 2, 3);
        let first = BackendKey::new(addr(30), token);
        let second = BackendKey::new(addr(31), token);

        backends.publish(first, io(first.service)).unwrap();
        assert_eq!(
            backends.publish(second, io(second.service)),
            Err(PublishError::AlreadyLive)
        );
        assert_eq!(backends.len(), 1);
    }
}
