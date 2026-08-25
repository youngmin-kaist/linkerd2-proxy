//! Stream-id keyed map without SipHash.
//!
//! Stream ids are small dense integers, so the default hasher's SipHash is
//! pure overhead (it showed up as ~1.4µs/req at m=32). We keep a `HashMap`
//! (bounded by `max_concurrent_streams`) but hash with a single multiply.
//!
//! Why not identity hashing: on the server the peer chooses stream ids, and
//! identity hashing over power-of-two buckets would let it force every id into
//! one bucket. A bare multiply is not enough either — it leaves the LOW bits
//! (the ones the table indexes with) unmixed, which the adversarial test below
//! caught. Multiply then fold the high bits down: still ~3 ops, but ids spaced
//! by any power of two now spread.

use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};

const MIX: u64 = 0x517c_c1b7_2722_0a95;

#[inline]
fn mix(v: u64) -> u64 {
    let h = v.wrapping_mul(MIX);
    h ^ (h >> 32)
}

#[derive(Default)]
pub(crate) struct IdHasher(u64);

impl Hasher for IdHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        // Not used for our i32 keys; keep it correct if it ever is.
        for &b in bytes {
            self.0 = (self.0 ^ b as u64).wrapping_mul(MIX);
        }
    }

    #[inline]
    fn write_i32(&mut self, v: i32) {
        self.0 = mix(v as u64);
    }

    #[inline]
    fn write_u32(&mut self, v: u32) {
        self.0 = mix(v as u64);
    }
}

pub(crate) type IdMap<V> = HashMap<i32, V, BuildHasherDefault<IdHasher>>;

pub(crate) fn new_map<V>() -> IdMap<V> {
    IdMap::default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn behaves_like_a_map() {
        let mut m: IdMap<u32> = new_map();
        for i in (1..200).step_by(2) {
            m.insert(i, i as u32 * 3);
        }
        for i in (1..200).step_by(2) {
            assert_eq!(m.get(&i), Some(&(i as u32 * 3)));
        }
        assert_eq!(m.remove(&51), Some(153));
        assert_eq!(m.get(&51), None);
        assert_eq!(m.len(), 99);
    }

    /// Ids an attacker could pick to collide under identity hashing must still
    /// spread across buckets here.
    #[test]
    fn adversarial_ids_spread() {
        use std::collections::HashSet;
        let buckets = 1024u64;
        let mut seen = HashSet::new();
        for k in 0..64u64 {
            let id = (k * buckets * 2 + 1) as i32; // same bucket under identity
            let mut h = IdHasher::default();
            h.write_i32(id);
            seen.insert(h.finish() % buckets);
        }
        assert!(seen.len() > 32, "ids collapsed into {} buckets", seen.len());
    }
}
