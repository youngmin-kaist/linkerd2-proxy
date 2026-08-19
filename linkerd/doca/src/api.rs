//! Flow and event types shared by both datapath configurations.

use tokio::sync::mpsc;

/// Flow identity presented to the Linkerd acceptor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlowId {
    pub src: std::net::SocketAddrV4,
    pub dst: std::net::SocketAddrV4,
    /// Source workload identity.
    pub workload: String,
    /// The connection provides the service at `dst`.
    pub is_backend: bool,
}

/// Names one session for its whole lifetime.
///
/// A slot is an index into the datapath's session table and is reused. The
/// generation counts how often that slot has been handed out, so an event
/// carrying an old generation cannot bind to the session now holding the slot.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SessionToken {
    /// ARM worker owning the session.
    pub worker: u16,
    pub slot: u32,
    pub generation: u32,
}

impl SessionToken {
    pub fn new(worker: u16, slot: u32, generation: u32) -> Self {
        Self {
            worker,
            slot,
            generation,
        }
    }
}

impl std::fmt::Display for SessionToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "w{}/s{}/g{}", self.worker, self.slot, self.generation)
    }
}

/// Slot allocator handing out monotonically increasing generations.
///
/// A slot returns to the free list on release and is handed out again with the
/// next generation. A slot whose generation would wrap is retired instead: no
/// token is ever issued twice.
#[derive(Debug)]
pub struct Slots {
    worker: u16,
    next_slot: u32,
    generations: Vec<u32>,
    free: Vec<u32>,
    retired: u32,
}

impl Slots {
    pub fn new(worker: u16) -> Self {
        Self {
            worker,
            next_slot: 0,
            generations: Vec::new(),
            free: Vec::new(),
            retired: 0,
        }
    }

    /// Take a token, or `None` once every slot is retired or exhausted.
    pub fn alloc(&mut self) -> Option<SessionToken> {
        if let Some(slot) = self.free.pop() {
            let generation = &mut self.generations[slot as usize];
            // The wrap check happens on release, so a slot on the free list
            // always has a generation left.
            *generation += 1;
            return Some(SessionToken::new(self.worker, slot, *generation));
        }
        let slot = self.next_slot;
        self.next_slot = self.next_slot.checked_add(1)?;
        self.generations.push(0);
        Some(SessionToken::new(self.worker, slot, 0))
    }

    /// Return a slot for reuse. A slot on its last generation is retired.
    pub fn release(&mut self, token: SessionToken) {
        let Some(&generation) = self.generations.get(token.slot as usize) else {
            return;
        };
        // Only the live generation owns the slot; a late release names a
        // session that already gave it back.
        if generation != token.generation || self.free.contains(&token.slot) {
            return;
        }
        if generation == u32::MAX {
            self.retired += 1;
            return;
        }
        self.free.push(token.slot);
    }

    /// Slots withdrawn from reuse because their generation space ran out.
    pub fn retired(&self) -> u32 {
        self.retired
    }
}

/// Datapath lifecycle events.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DmeshEvent {
    /// Shared infrastructure is ready.
    InfraReady,
    /// A connection is ready in this session.
    ConnReady(SessionToken, FlowId),
    /// A connection was unbound.
    ConnClosed(SessionToken),
    /// Connection setup failed.
    ConnError(SessionToken),
    /// Datapath counter deltas over `elapsed_ms`.
    Stats {
        elapsed_ms: u64,
        recv_msgs: i64,
        recv_bytes: i64,
        sent_msgs: i64,
        dma_pending: i64,
        dma_dropped: i64,
    },
}

/// Acceptor registration for one connection endpoint.
pub struct Registration {
    pub token: SessionToken,
    pub handle: crate::DmeshIoHandle,
}

/// Acceptor registration sender.
pub type Registrar = mpsc::UnboundedSender<Registration>;

/// Connection slots used by the bundled datapath.
pub const MAX_CONNS: usize = 8;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuse_advances_the_generation() {
        let mut slots = Slots::new(3);
        let first = slots.alloc().unwrap();
        assert_eq!(first, SessionToken::new(3, 0, 0));
        let second = slots.alloc().unwrap();
        assert_eq!(second, SessionToken::new(3, 1, 0));

        slots.release(first);
        let reused = slots.alloc().unwrap();
        assert_eq!(reused, SessionToken::new(3, 0, 1));
        assert_ne!(reused, first, "a reused slot is a different session");
    }

    #[test]
    fn late_release_does_not_free_the_live_session() {
        let mut slots = Slots::new(0);
        let first = slots.alloc().unwrap();
        slots.release(first);
        let reused = slots.alloc().unwrap();

        slots.release(first); // the closed session's second close
        assert_eq!(
            slots.alloc().unwrap(),
            SessionToken::new(0, 1, 0),
            "the live slot must not return to the free list"
        );
        slots.release(reused);
        assert_eq!(slots.alloc().unwrap(), SessionToken::new(0, 0, 2));
    }

    #[test]
    fn a_slot_out_of_generations_is_retired() {
        let mut slots = Slots::new(0);
        let token = slots.alloc().unwrap();
        slots.generations[0] = u32::MAX;
        slots.release(SessionToken {
            generation: u32::MAX,
            ..token
        });
        assert_eq!(slots.retired(), 1);
        assert_eq!(
            slots.alloc().unwrap(),
            SessionToken::new(0, 1, 0),
            "the retired slot is not handed out again"
        );
    }
}
