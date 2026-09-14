//! HPACK dynamic-table replicas used by the re-indexing transcoder
//! ([`super::transcode`]).
//!
//! Two flavours share one storage type ([`Table`]):
//!
//! * [`MirrorTable`] — a replica of a *peer's* HPACK **encoder** table, kept in
//!   sync by walking every header block that peer sends to us. Entries keep the
//!   name (static index or the encoded bytes) and the value **as encoded on the
//!   wire** (Huffman or raw); nothing is decoded except the values of the
//!   pseudo-headers and of the names in the connection's
//!   [`NeededSet`](super::selective::NeededSet).
//! * [`EncoderTable`] — *our* encoder table toward a peer, i.e. a replica of
//!   that peer's decoder table. Entries record their provenance
//!   `(src_tid, src_serial)`: which mirror entry they were copied from, and the
//!   table keeps a small direct-mapped provenance → absolute-insertion-number
//!   cache so that a dynamic reference from any mirror resolves in O(1)
//!   without touching that mirror (the mirror may live in another
//!   connection's decoder) and without a hash map.
//!
//! # Storage
//!
//! Entries are small `Copy` records; their bytes live in a per-table
//! [`Arena`]: an append-only buffer with FIFO reclamation. Because HPACK
//! eviction is strictly oldest-first, the live entries' bytes always form a
//! contiguous tail of the arena, so eviction is a `pop_back` (no free) and
//! reclamation is an occasional `memmove` of the live tail to the front.
//! Nothing is allocated per entry.
//!
//! This mirrors `HpTable` / `HpEntry` in `dmesh-router-cpp/src/relay3.{hpp,cpp}`.

#![allow(dead_code)]
#![allow(missing_docs)]

use super::selective::NeededSet;
use bytes::BytesMut;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU32, Ordering};

/// Header field kinds the walker classifies by name: the pseudo-headers
/// (whose values are always decoded) and `None` for everything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Kind {
    /// Not a pseudo-header.
    None = 0,
    /// `:method`
    Method = 1,
    /// `:path`
    Path = 2,
    /// `:authority`
    Authority = 3,
    /// `:scheme`
    Scheme = 4,
    /// `:status`
    Status = 5,
    /// `:protocol`
    Protocol = 6,
}

impl Kind {
    /// Kind implied by a static-table index used as a *name* reference (or a
    /// fully indexed static field).
    #[inline]
    pub fn of_static(idx: u32) -> Kind {
        match idx {
            1 => Kind::Authority,
            2 | 3 => Kind::Method,
            4 | 5 => Kind::Path,
            6 | 7 => Kind::Scheme,
            8..=14 => Kind::Status,
            _ => Kind::None,
        }
    }

    /// Kind of a literal (decoded) header name.
    #[inline]
    pub fn of_name(name: &[u8]) -> Kind {
        if name.first() != Some(&b':') {
            return Kind::None;
        }
        match name {
            b":path" => Kind::Path,
            b":method" => Kind::Method,
            b":authority" => Kind::Authority,
            b":scheme" => Kind::Scheme,
            b":status" => Kind::Status,
            b":protocol" => Kind::Protocol,
            _ => Kind::None,
        }
    }

    /// Bit for `PolicyFields::have` (`0` for `Kind::None`).
    #[inline]
    pub fn bit(self) -> u8 {
        match self {
            Kind::None => 0,
            k => 1 << (k as u8 - 1),
        }
    }

    #[inline]
    pub fn is_pseudo(self) -> bool {
        self != Kind::None
    }

    /// Wire name of a pseudo kind.
    pub fn pseudo_name(self) -> &'static [u8] {
        match self {
            Kind::None => b"",
            Kind::Method => b":method",
            Kind::Path => b":path",
            Kind::Authority => b":authority",
            Kind::Scheme => b":scheme",
            Kind::Status => b":status",
            Kind::Protocol => b":protocol",
        }
    }
}

/// RFC 7541 Appendix A static-table name lengths (index 1..=61), for exact
/// entry-size accounting when a literal references a static name.
pub const STATIC_NAME_LEN: [u8; 62] = [
    0, 10, 7, 7, 5, 5, 7, 7, 7, 7, 7, 7, 7, 7, 7, 14, 15, 15, 13, 6, 27, 3, 5, 13, 13, 19, 16, 16,
    14, 16, 13, 12, 6, 4, 4, 6, 7, 4, 4, 8, 17, 13, 8, 19, 13, 4, 8, 12, 18, 19, 5, 7, 7, 11, 6,
    10, 25, 17, 10, 4, 3, 16,
];

/// Values of the fully-indexed static entries that carry a pseudo kind
/// (indices 1..=14).
#[inline]
pub fn static_value(idx: u32) -> &'static [u8] {
    match idx {
        1 => b"",
        2 => b"GET",
        3 => b"POST",
        4 => b"/",
        5 => b"/index.html",
        6 => b"http",
        7 => b"https",
        8 => b"200",
        9 => b"204",
        10 => b"206",
        11 => b"304",
        12 => b"400",
        13 => b"404",
        14 => b"500",
        _ => b"",
    }
}

static NEXT_TID: AtomicU32 = AtomicU32::new(1);

/// Allocate a process-unique table id (never 0).
pub fn next_tid() -> u32 {
    loop {
        let t = NEXT_TID.fetch_add(1, Ordering::Relaxed);
        if t != 0 {
            return t;
        }
    }
}

/// "no decoded value stored"
pub const NO_DEC: u32 = u32::MAX;

// ---- arena -------------------------------------------------------------------------

/// Append-only byte store with FIFO reclamation. Positions are absolute
/// (monotonically increasing), so reclaiming never invalidates a live
/// position.
#[derive(Debug)]
pub struct Arena {
    buf: Vec<u8>,
    /// Absolute position of `buf[0]`.
    base: u64,
}

impl Arena {
    fn with_capacity(cap: usize) -> Arena {
        Arena {
            buf: Vec::with_capacity(cap),
            base: 0,
        }
    }

    #[inline]
    fn end(&self) -> u64 {
        self.base + self.buf.len() as u64
    }

    #[inline]
    fn push(&mut self, s: &[u8]) -> u64 {
        let p = self.end();
        self.buf.extend_from_slice(s);
        p
    }

    #[inline]
    fn get(&self, pos: u64, len: u32) -> &[u8] {
        let o = (pos - self.base) as usize;
        &self.buf[o..o + len as usize]
    }

    /// Everything before `live_start` is dead. Cheap check; occasional
    /// memmove of the live tail.
    #[inline]
    fn reclaim(&mut self, live_start: u64) {
        let dead = (live_start - self.base) as usize;
        if dead == 0 {
            return;
        }
        if dead >= self.buf.len() {
            self.base += self.buf.len() as u64;
            self.buf.clear();
        } else if dead > self.buf.len() / 2 {
            self.buf.copy_within(dead.., 0);
            let n = self.buf.len() - dead;
            self.buf.truncate(n);
            self.base += dead as u64;
        }
    }
}

// ---- entries ---------------------------------------------------------------------------

/// One dynamic-table entry (shared layout for mirror and encoder tables).
/// Bytes live in the table's arena at `pos`:
/// `[name_enc][val_enc][val_dec]` (the last only when `dec_len != NO_DEC`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Entry {
    pub(crate) pos: u64,
    /// Unique per table; equals the absolute insertion number.
    pub(crate) serial: u64,
    /// Encoder side: provenance (mirror tid + serial), `(0, 0)` if none.
    pub(crate) src_serial: u64,
    pub(crate) src_tid: u32,
    pub(crate) name_enc_len: u32,
    pub(crate) val_enc_len: u32,
    /// Decoded (RFC 7541 §4.1) lengths.
    pub(crate) name_dec_len: u32,
    pub(crate) val_dec_len: u32,
    /// Length of the stored decoded value (`NO_DEC` if none).
    pub(crate) dec_len: u32,
    /// Mirror side: `NeededSet` tag of the name (see `selective::TAG_*`).
    pub(crate) tag: u16,
    /// Static index (1..=61) the name refers to, or 0 if the name is literal.
    pub(crate) name_static: u8,
    /// Mirror side: pseudo kind.
    pub(crate) kind: Kind,
    pub(crate) name_huff: bool,
    pub(crate) val_huff: bool,
}

impl Entry {
    /// RFC 7541 §4.1 size.
    #[inline]
    pub fn size(&self) -> usize {
        (self.name_dec_len + self.val_dec_len + 32) as usize
    }

    #[inline]
    pub fn kind(&self) -> Kind {
        self.kind
    }

    #[inline]
    pub fn tag(&self) -> u16 {
        self.tag
    }

    #[inline]
    pub fn serial(&self) -> u64 {
        self.serial
    }

    #[inline]
    pub fn name_static(&self) -> u8 {
        self.name_static
    }

    #[inline]
    pub fn has_dec(&self) -> bool {
        self.dec_len != NO_DEC
    }
}

/// An entry together with its bytes.
#[derive(Debug, Clone, Copy)]
pub struct EntryRef<'a> {
    pub entry: &'a Entry,
    arena: &'a Arena,
}

impl<'a> EntryRef<'a> {
    /// Encoded name bytes (empty when the name is a static index).
    #[inline]
    pub fn name_enc(&self) -> &'a [u8] {
        self.arena.get(self.entry.pos, self.entry.name_enc_len)
    }

    /// Encoded value bytes exactly as received.
    #[inline]
    pub fn val_enc(&self) -> &'a [u8] {
        self.arena.get(
            self.entry.pos + self.entry.name_enc_len as u64,
            self.entry.val_enc_len,
        )
    }

    /// `[name_enc][val_enc]`
    #[inline]
    pub fn wire(&self) -> &'a [u8] {
        self.arena
            .get(self.entry.pos, self.entry.name_enc_len + self.entry.val_enc_len)
    }

    /// Decoded value, if stored (pseudo kinds and needed names).
    #[inline]
    pub fn val_dec(&self) -> Option<&'a [u8]> {
        if self.entry.dec_len == NO_DEC {
            return None;
        }
        Some(self.arena.get(
            self.entry.pos + (self.entry.name_enc_len + self.entry.val_enc_len) as u64,
            self.entry.dec_len,
        ))
    }

    #[inline]
    pub fn kind(&self) -> Kind {
        self.entry.kind
    }

    #[inline]
    pub fn tag(&self) -> u16 {
        self.entry.tag
    }
}

/// What to insert.
#[derive(Debug, Clone, Copy)]
pub struct Insert<'a> {
    pub name_static: u8,
    pub name_enc: &'a [u8],
    pub name_huff: bool,
    pub name_dec_len: usize,
    pub val_enc: &'a [u8],
    pub val_huff: bool,
    pub val_dec_len: usize,
    pub kind: Kind,
    pub tag: u16,
    pub val_dec: Option<&'a [u8]>,
    pub src_tid: u32,
    pub src_serial: u64,
}

impl<'a> Insert<'a> {
    /// A plain entry with no provenance and nothing decoded.
    pub fn plain(
        name_static: u8,
        name_enc: &'a [u8],
        name_huff: bool,
        name_dec_len: usize,
        val_enc: &'a [u8],
        val_huff: bool,
        val_dec_len: usize,
    ) -> Insert<'a> {
        Insert {
            name_static,
            name_enc,
            name_huff,
            name_dec_len,
            val_enc,
            val_huff,
            val_dec_len,
            kind: Kind::None,
            tag: 0,
            val_dec: None,
            src_tid: 0,
            src_serial: 0,
        }
    }
}

// ---- table ---------------------------------------------------------------------------

const PROV_SLOTS: usize = 256;

#[derive(Debug, Clone, Copy)]
struct ProvSlot {
    tid: u32,
    serial: u64,
    abs: u64,
}

const EMPTY_SLOT: ProvSlot = ProvSlot {
    tid: 0,
    serial: 0,
    abs: 0,
};

#[inline]
fn prov_index(tid: u32, serial: u64) -> usize {
    let h = (serial ^ ((tid as u64) << 20)).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (h >> 56) as usize & (PROV_SLOTS - 1)
}

/// HPACK dynamic table with absolute-insertion-number addressing.
///
/// Newest entry is dynamic index 62. `insert` evicts by RFC 7541 §4.4 (an
/// entry larger than the table max empties the table and is itself dropped).
#[derive(Debug)]
pub struct Table {
    tid: u32,
    max: usize,
    size: usize,
    inserts: u64,
    /// front = newest
    entries: VecDeque<Entry>,
    arena: Arena,
    /// Encoder side: direct-mapped `(src_tid, src_serial)` → abs cache.
    /// Allocated on first use.
    prov: Option<Box<[ProvSlot; PROV_SLOTS]>>,
}

impl Table {
    /// `tid` identifies the table in provenance records. Use [`next_tid`];
    /// `0` is reserved for "no provenance".
    pub fn new(tid: u32, max: usize) -> Table {
        debug_assert!(tid != 0, "table id 0 is reserved");
        Table {
            tid,
            max,
            size: 0,
            inserts: 0,
            entries: VecDeque::with_capacity(64),
            arena: Arena::with_capacity(max.min(1 << 16) * 2 + 1024),
            prov: None,
        }
    }

    #[inline]
    pub fn tid(&self) -> u32 {
        self.tid
    }

    /// Maximum size (RFC 7541 §4.2).
    #[inline]
    pub fn max(&self) -> usize {
        self.max
    }

    /// Current size (sum of entry sizes).
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Number of entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Set the maximum size, evicting as needed.
    pub fn set_max(&mut self, max: usize) {
        self.max = max;
        self.evict();
    }

    /// Absolute insertion number the next `insert` will get.
    #[inline]
    pub fn next_abs(&self) -> u64 {
        self.inserts
    }

    /// Push a new entry at the head. Returns its absolute insertion number
    /// and whether it survived the post-insert eviction (it is evicted
    /// immediately when larger than the table max — legal, RFC 7541 §4.4;
    /// then the table is empty afterwards).
    pub fn insert(&mut self, ins: Insert<'_>) -> (u64, bool) {
        let abs = self.inserts;
        self.inserts += 1;
        let size = ins.name_dec_len + ins.val_dec_len + 32;
        if size > self.max {
            // Never fits: empty the table without touching the arena.
            self.entries.clear();
            self.size = 0;
            let end = self.arena.end();
            self.arena.reclaim(end);
            return (abs, false);
        }
        let pos = self.arena.push(ins.name_enc);
        self.arena.push(ins.val_enc);
        let dec_len = match ins.val_dec {
            Some(d) => {
                self.arena.push(d);
                d.len() as u32
            }
            None => NO_DEC,
        };
        let e = Entry {
            pos,
            serial: abs,
            src_serial: ins.src_serial,
            src_tid: ins.src_tid,
            name_enc_len: ins.name_enc.len() as u32,
            val_enc_len: ins.val_enc.len() as u32,
            name_dec_len: ins.name_dec_len as u32,
            val_dec_len: ins.val_dec_len as u32,
            dec_len,
            tag: ins.tag,
            name_static: ins.name_static,
            kind: ins.kind,
            name_huff: ins.name_huff,
            val_huff: ins.val_huff,
        };
        self.size += size;
        self.entries.push_front(e);
        if ins.src_tid != 0 {
            let inserts = self.inserts;
            let entries = self.entries.len() as u64;
            let prov = self
                .prov
                .get_or_insert_with(|| Box::new([EMPTY_SLOT; PROV_SLOTS]));
            // 2-way: prefer a slot whose entry is already gone.
            let h = prov_index(ins.src_tid, ins.src_serial);
            let alt = h ^ 1;
            let live = |s: &ProvSlot| s.tid != 0 && s.abs < inserts && inserts - 1 - s.abs < entries;
            let i = if !live(&prov[h]) {
                h
            } else if !live(&prov[alt]) {
                alt
            } else if prov[h].abs <= prov[alt].abs {
                h
            } else {
                alt
            };
            prov[i] = ProvSlot {
                tid: ins.src_tid,
                serial: ins.src_serial,
                abs,
            };
        }
        self.evict();
        (abs, true)
    }

    /// Dynamic index (62-based) → entry record.
    #[inline]
    pub fn get_entry(&self, idx: u32) -> Option<&Entry> {
        if idx < 62 {
            return None;
        }
        self.entries.get((idx - 62) as usize)
    }

    /// Dynamic index (62-based) → entry with bytes.
    #[inline]
    pub fn get(&self, idx: u32) -> Option<EntryRef<'_>> {
        let entry = self.get_entry(idx)?;
        Some(EntryRef {
            entry,
            arena: &self.arena,
        })
    }

    /// Absolute insertion number → current dynamic index, `None` if evicted.
    #[inline]
    pub fn index_of(&self, abs: u64) -> Option<u32> {
        if abs >= self.inserts {
            return None;
        }
        let off = self.inserts - 1 - abs;
        if off < self.entries.len() as u64 {
            Some(62 + off as u32)
        } else {
            None
        }
    }

    /// Absolute insertion number → entry, `None` if evicted.
    #[inline]
    pub fn by_abs(&self, abs: u64) -> Option<EntryRef<'_>> {
        self.get(self.index_of(abs)?)
    }

    /// Encoder side: current dynamic index of the live copy of mirror entry
    /// `(src_tid, src_serial)`, if any. O(1): one direct-mapped slot; a
    /// slot is only trusted if the absolute number it names is still live
    /// (absolute numbers are never reused, so no further validation is
    /// needed).
    #[inline]
    pub fn lookup_provenance(&self, src_tid: u32, src_serial: u64) -> Option<u32> {
        let prov = self.prov.as_ref()?;
        let h = prov_index(src_tid, src_serial);
        let s = &prov[h];
        if s.tid == src_tid && s.serial == src_serial {
            return self.index_of(s.abs);
        }
        let s = &prov[h ^ 1];
        if s.tid == src_tid && s.serial == src_serial {
            return self.index_of(s.abs);
        }
        None
    }

    #[inline]
    fn evict(&mut self) {
        while self.size > self.max {
            match self.entries.pop_back() {
                Some(last) => self.size -= last.size(),
                None => {
                    debug_assert!(false, "table size != 0 with no entries");
                    self.size = 0;
                    break;
                }
            }
        }
        let live_start = match self.entries.back() {
            Some(e) => e.pos,
            None => self.arena.end(),
        };
        self.arena.reclaim(live_start);
    }

    /// Bytes currently held by the arena (for tests).
    pub fn arena_len(&self) -> usize {
        self.arena.buf.len()
    }
}

/// Replica of a peer's HPACK encoder table (see module docs).
#[derive(Debug)]
pub struct MirrorTable {
    pub(crate) table: Table,
    /// Scratch for the few Huffman decodes the walk performs (literal names,
    /// pseudo/needed values).
    pub(crate) scratch: BytesMut,
    /// Names whose values must be decoded and tagged (selective mode).
    pub(crate) needed: Option<NeededSet>,
}

impl MirrorTable {
    /// `max` is the peer's initial dynamic table size — the value of *our*
    /// `SETTINGS_HEADER_TABLE_SIZE` as acknowledged by the peer, 4096 by
    /// default. Later changes arrive in-band as dynamic table size updates.
    pub fn new(tid: u32, max: usize) -> MirrorTable {
        MirrorTable {
            table: Table::new(tid, max),
            scratch: BytesMut::with_capacity(512),
            needed: None,
        }
    }

    /// Tag names against `needed` (and decode the values of needed names).
    pub fn set_needed(&mut self, needed: NeededSet) {
        self.needed = Some(needed);
    }

    #[inline]
    pub fn tid(&self) -> u32 {
        self.table.tid()
    }

    #[inline]
    pub fn max(&self) -> usize {
        self.table.max()
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.table.size()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Dynamic index (62-based) → entry.
    #[inline]
    pub fn get(&self, idx: u32) -> Option<EntryRef<'_>> {
        self.table.get(idx)
    }

    /// Apply a dynamic table size update seen in a block from the peer
    /// (the transcoder does this itself).
    pub fn set_max(&mut self, max: usize) {
        self.table.set_max(max)
    }

    #[inline]
    pub fn table(&self) -> &Table {
        &self.table
    }
}

/// Our HPACK encoder table toward a peer (see module docs).
#[derive(Debug)]
pub struct EncoderTable {
    pub(crate) table: Table,
    /// Queued dynamic table size update(s) `(min, final)` to emit at the start
    /// of the next block (RFC 7541 §4.2).
    pending: Option<(usize, usize)>,
}

impl EncoderTable {
    /// `max` is the peer's decoder table size (its `SETTINGS_HEADER_TABLE_SIZE`,
    /// 4096 by default).
    pub fn new(tid: u32, max: usize) -> EncoderTable {
        EncoderTable {
            table: Table::new(tid, max),
            pending: None,
        }
    }

    #[inline]
    pub fn tid(&self) -> u32 {
        self.table.tid()
    }

    #[inline]
    pub fn max(&self) -> usize {
        self.table.max()
    }

    #[inline]
    pub fn size(&self) -> usize {
        self.table.size()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    /// Dynamic index (62-based) → entry.
    #[inline]
    pub fn get(&self, idx: u32) -> Option<EntryRef<'_>> {
        self.table.get(idx)
    }

    /// Absolute insertion number → current dynamic index.
    #[inline]
    pub fn index_of(&self, abs: u64) -> Option<u32> {
        self.table.index_of(abs)
    }

    /// Change the maximum size *without* signalling the peer (the C++
    /// `HpTable::set_max`). Only valid when the peer already agrees, e.g. at
    /// setup.
    pub fn set_max(&mut self, max: usize) {
        self.table.set_max(max)
    }

    /// Queue a maximum-size change to be signalled at the start of the next
    /// block emitted through this table (the peer lowered/raised its
    /// `SETTINGS_HEADER_TABLE_SIZE`). If the size shrinks and grows again
    /// before a block is emitted, both the minimum and the final value are
    /// signalled, as RFC 7541 §4.2 requires.
    pub fn resize(&mut self, new_max: usize) {
        self.pending = match self.pending {
            None if new_max == self.table.max() => None,
            None => Some((new_max, new_max)),
            Some((min, _)) => Some((min.min(new_max), new_max)),
        };
    }

    /// Take the queued size update(s), applying them to the table.
    #[inline]
    pub(crate) fn take_pending_resize(&mut self) -> Option<(usize, usize)> {
        let (min, fin) = self.pending.take()?;
        self.table.set_max(min);
        self.table.set_max(fin);
        Some((min, fin))
    }

    #[inline]
    pub fn table(&self) -> &Table {
        &self.table
    }
}
