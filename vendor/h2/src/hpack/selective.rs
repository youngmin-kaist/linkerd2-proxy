//! Selective-decoding connection state and the public types it exposes.
//!
//! In selective mode a connection's HPACK decoder keeps a [`MirrorTable`] of
//! the peer's encoder instead of a decoded table, and its encoder keeps an
//! [`EncoderTable`] toward the peer. Header blocks received are walked into
//! (a) a **sparse** field set — every pseudo-header plus the names in the
//! connection's [`NeededSet`] — and (b) the full list of resolved
//! representations ([`RawHeaderReps`]), carried in the request/response
//! `Extensions`. Header blocks sent with a `RawHeaderReps` attached are
//! produced by re-indexing those representations against the encoder table
//! (never decode → re-encode); fields the stack added are appended.
//!
//! # Trailers
//!
//! `http::HeaderMap` has no extension slot, and trailers travel through hyper
//! as a bare `HeaderMap`. Received trailer blocks are therefore **fully
//! materialized** (the mirror is still walked, so table sync is unaffected)
//! and sent trailers are encoded as literals without indexing.
//!
//! # Header removal / modification by the stack
//!
//! By default the sparse map is authoritative at send time for
//! pseudo-headers and materialized names: a representation is emitted only
//! if the map still holds the same value (compared in order for repeated
//! names); otherwise it is skipped — skipping is always legal, an unsent
//! representation simply never enters the peer's table — and the map's value
//! is sent instead. With [`NeededSet::trust_reps`] the comparison is skipped:
//! every representation is emitted as is (pseudo-headers missing from the
//! representations are still added). Names the stack never saw (not in the
//! `NeededSet`) are always emitted from the representations. Names in the
//! map that came from nowhere (added by the stack, e.g. `l5d-*`) are
//! appended; the encoder remembers the last few of them so a repeat costs one
//! index byte instead of a Huffman-coded literal.
//!
//! # Allocation
//!
//! Per block the hot path allocates nothing: representation lists and byte
//! stores are recycled through per-connection pools, tables use arenas, and
//! statistics are per-connection counters folded into the shared set every
//! [`STATS_FOLD_EVERY`] blocks. The only per-request allocations left are the
//! sparse `HeaderMap` entries themselves and the `Box` the `Extensions` map
//! keeps the `RawHeaderReps` in.

#![allow(dead_code)]
#![allow(missing_docs)]

use super::huffman;
use super::mirror::{next_tid, EncoderTable, Insert, MirrorTable};
use super::transcode::{self, Rep, RepBuf, RepKind, Sink, TranscodeStats};
use super::DecoderError;
use bytes::{BufMut, Bytes, BytesMut};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Name tag: not a needed name.
pub const TAG_OTHER: u16 = 0;
/// Name tag: connection-specific header (RFC 7540 §8.1.2.2), malformed.
pub const TAG_FORBIDDEN: u16 = 1;
/// Name tags `>= TAG_NEEDED_BASE` are `TAG_NEEDED_BASE + index` into the
/// `NeededSet`.
pub const TAG_NEEDED_BASE: u16 = 2;

/// Maximum number of names in a `NeededSet` (the materialized-set bitmask).
pub const MAX_NEEDED: usize = 64;

const FORBIDDEN: [&str; 5] = [
    "connection",
    "transfer-encoding",
    "upgrade",
    "keep-alive",
    "proxy-connection",
];

struct Inner {
    names: Vec<HeaderName>,
    /// Tag of each static-table index (0 unused).
    static_tag: [u16; 62],
    trust_reps: bool,
    print_stats: bool,
    stats: StatsSink,
}

/// The header names (besides pseudo-headers) a selective connection
/// materializes into the `HeaderMap`. Cheap to clone; also aggregates
/// [`TranscodeStats`] from every connection created with it.
#[derive(Clone)]
pub struct NeededSet(Arc<Inner>);

impl NeededSet {
    /// Build a set from `names` (deduplicated). Nothing is added implicitly:
    /// `content-length`/`te` are only validated by h2/hyper when present, so
    /// leaving them out is safe (hyper never adds a `content-length` unless
    /// the body length is exactly known, and it is not for a sparse map). At
    /// most [`MAX_NEEDED`] names are kept.
    pub fn new<I>(names: I) -> NeededSet
    where
        I: IntoIterator<Item = HeaderName>,
    {
        NeededSet::with_options(names, false, false)
    }

    /// [`new`](Self::new) with the send-side policy and stats printing:
    /// `trust_reps` skips the "sparse map is authoritative" comparison;
    /// `print_stats` prints each connection's [`TranscodeStats`] to stderr
    /// when it closes.
    pub fn with_options<I>(names: I, trust_reps: bool, print_stats: bool) -> NeededSet
    where
        I: IntoIterator<Item = HeaderName>,
    {
        let mut v: Vec<HeaderName> = Vec::new();
        for n in names {
            if !v.contains(&n) && v.len() < MAX_NEEDED {
                v.push(n);
            }
        }
        let mut inner = Inner {
            names: v,
            static_tag: [TAG_OTHER; 62],
            trust_reps,
            print_stats,
            stats: StatsSink::default(),
        };
        let mut static_tag = [TAG_OTHER; 62];
        for (idx, slot) in static_tag.iter_mut().enumerate().skip(1) {
            let h = super::decoder::get_static(idx);
            *slot = inner.tag_of(h.name().as_slice());
        }
        inner.static_tag = static_tag;
        NeededSet(Arc::new(inner))
    }

    /// The names in the set.
    pub fn names(&self) -> &[HeaderName] {
        &self.0.names
    }

    /// Whether representations are emitted without checking the sparse map.
    #[inline]
    pub fn trust_reps(&self) -> bool {
        self.0.trust_reps
    }

    /// Tag of a (decoded, lowercase) header name.
    #[inline]
    pub fn tag_of(&self, name: &[u8]) -> u16 {
        self.0.tag_of(name)
    }

    /// Tag of the name of static-table entry `idx`.
    #[inline]
    pub fn tag_of_static(&self, idx: u32) -> u16 {
        if idx == 0 || idx > 61 {
            return TAG_OTHER;
        }
        self.0.static_tag[idx as usize]
    }

    /// Whether `tag` refers to a needed name.
    #[inline]
    pub fn is_needed(tag: u16) -> bool {
        tag >= TAG_NEEDED_BASE
    }

    /// Index into [`names`](Self::names) for a needed tag.
    #[inline]
    pub fn needed_index(tag: u16) -> Option<usize> {
        Self::is_needed(tag).then(|| (tag - TAG_NEEDED_BASE) as usize)
    }

    /// The `HeaderName` for a needed tag.
    #[inline]
    pub fn name_of_tag(&self, tag: u16) -> Option<&HeaderName> {
        self.0.names.get(Self::needed_index(tag)?)
    }

    /// Aggregate transcode statistics of all connections using this set
    /// (folded in periodically and when a connection closes).
    pub fn stats(&self) -> TranscodeStats {
        self.0.stats.snapshot()
    }

    pub(crate) fn add_stats(&self, s: &TranscodeStats) {
        self.0.stats.add(s)
    }
}

impl Inner {
    fn tag_of(&self, name: &[u8]) -> u16 {
        for (i, n) in self.names.iter().enumerate() {
            if n.as_str().as_bytes() == name {
                return TAG_NEEDED_BASE + i as u16;
            }
        }
        for f in FORBIDDEN {
            if f.as_bytes() == name {
                return TAG_FORBIDDEN;
            }
        }
        TAG_OTHER
    }
}

impl PartialEq for NeededSet {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for NeededSet {}

impl Default for NeededSet {
    fn default() -> Self {
        NeededSet::new(std::iter::empty())
    }
}

impl fmt::Debug for NeededSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NeededSet")
            .field("names", &self.0.names)
            .field("trust_reps", &self.0.trust_reps)
            .finish()
    }
}

#[derive(Default)]
struct StatsSink {
    fields: AtomicU64,
    static_idx: AtomicU64,
    dyn_hit: AtomicU64,
    dyn_miss: AtomicU64,
    lit_insert: AtomicU64,
    lit_noindex: AtomicU64,
    never_indexed: AtomicU64,
    size_updates: AtomicU64,
    in_bytes: AtomicU64,
    out_bytes: AtomicU64,
    rewrites: AtomicU64,
    extra_lit: AtomicU64,
    extra_hit: AtomicU64,
}

impl StatsSink {
    fn add(&self, s: &TranscodeStats) {
        let o = Ordering::Relaxed;
        self.fields.fetch_add(s.fields, o);
        self.static_idx.fetch_add(s.static_idx, o);
        self.dyn_hit.fetch_add(s.dyn_hit, o);
        self.dyn_miss.fetch_add(s.dyn_miss, o);
        self.lit_insert.fetch_add(s.lit_insert, o);
        self.lit_noindex.fetch_add(s.lit_noindex, o);
        self.never_indexed.fetch_add(s.never_indexed, o);
        self.size_updates.fetch_add(s.size_updates, o);
        self.in_bytes.fetch_add(s.in_bytes, o);
        self.out_bytes.fetch_add(s.out_bytes, o);
        self.rewrites.fetch_add(s.rewrites, o);
        self.extra_lit.fetch_add(s.extra_lit, o);
        self.extra_hit.fetch_add(s.extra_hit, o);
    }

    fn snapshot(&self) -> TranscodeStats {
        let o = Ordering::Relaxed;
        TranscodeStats {
            fields: self.fields.load(o),
            static_idx: self.static_idx.load(o),
            dyn_hit: self.dyn_hit.load(o),
            dyn_miss: self.dyn_miss.load(o),
            lit_insert: self.lit_insert.load(o),
            lit_noindex: self.lit_noindex.load(o),
            never_indexed: self.never_indexed.load(o),
            size_updates: self.size_updates.load(o),
            in_bytes: self.in_bytes.load(o),
            out_bytes: self.out_bytes.load(o),
            rewrites: self.rewrites.load(o),
            extra_lit: self.extra_lit.load(o),
            extra_hit: self.extra_hit.load(o),
        }
    }
}

/// The resolved representations of a received header block, attached to the
/// `http::Request` / `http::Response` extensions by a selective connection.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RawHeaderReps {
    /// The representations, in wire order (size updates included; they are
    /// never re-emitted), and the bytes they refer to.
    pub buf: RepBuf,
    /// Table id of the mirror the representations were resolved against.
    pub mirror_tid: u32,
    /// Bitmask over `needed.names()` of the names present in the sparse map.
    pub names_materialized: u64,
    /// The set the sparse map was built with.
    pub needed: NeededSet,
}

impl RawHeaderReps {
    /// Number of representations.
    pub fn len(&self) -> usize {
        self.buf.reps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.reps.is_empty()
    }

    /// Whether the name with `tag` was materialized into the sparse map.
    #[inline]
    pub fn is_materialized(&self, tag: u16) -> bool {
        match NeededSet::needed_index(tag) {
            Some(i) => self.names_materialized & (1u64 << i) != 0,
            None => false,
        }
    }

    /// Decode every regular field that was **not** materialized and append
    /// it to `map` (used for trailers, which cannot carry extensions).
    pub fn materialize_rest(&self, map: &mut HeaderMap) -> Result<(), DecoderError> {
        let mut scratch = BytesMut::new();
        for rep in &self.buf.reps {
            if rep.is_pseudo() || rep.is_size_update() || self.is_materialized(rep.tag) {
                continue;
            }
            let (name, value) = rep_decode(rep, &self.buf.bytes, &mut scratch)?;
            let name = HeaderName::from_lowercase(&name)?;
            let value = HeaderValue::from_maybe_shared(value)?;
            map.append(name, value);
        }
        Ok(())
    }
}

/// Fully decode a representation's name and value.
pub(crate) fn rep_decode(
    rep: &Rep,
    bytes: &[u8],
    scratch: &mut BytesMut,
) -> Result<(Bytes, Bytes), DecoderError> {
    fn dec(enc: &[u8], huff: bool, scratch: &mut BytesMut) -> Result<Bytes, DecoderError> {
        if huff {
            Ok(huffman::decode(enc, scratch)?.freeze())
        } else {
            Ok(Bytes::copy_from_slice(enc))
        }
    }
    if rep.kind == RepKind::SizeUpdate {
        return Err(DecoderError::InvalidRepresentation);
    }
    if rep.kind == RepKind::Static {
        let h = super::decoder::get_static(rep.num as usize);
        return Ok((
            Bytes::copy_from_slice(h.name().as_slice()),
            Bytes::copy_from_slice(h.value_slice()),
        ));
    }
    let name = if rep.name_static != 0 {
        Bytes::copy_from_slice(
            super::decoder::get_static(rep.name_static as usize)
                .name()
                .as_slice(),
        )
    } else {
        dec(rep.name(bytes), rep.name_huff, scratch)?
    };
    let value = match rep.dec(bytes) {
        Some(d) => Bytes::copy_from_slice(d),
        None => dec(rep.val(bytes), rep.val_huff, scratch)?,
    };
    Ok((name, value))
}

/// Value of static-table entry `idx` (empty for most).
pub(crate) fn static_field_value(idx: u32) -> &'static [u8] {
    match idx {
        1..=14 => super::mirror::static_value(idx),
        16 => b"gzip, deflate",
        _ => b"",
    }
}

/// Static-table index whose *name* is `name`, or 0.
pub(crate) fn static_name_index(name: &[u8]) -> u8 {
    match name {
        b":authority" => 1,
        b":method" => 2,
        b":path" => 4,
        b":scheme" => 6,
        b":status" => 8,
        b"accept-charset" => 15,
        b"accept-encoding" => 16,
        b"accept-language" => 17,
        b"accept-ranges" => 18,
        b"accept" => 19,
        b"access-control-allow-origin" => 20,
        b"age" => 21,
        b"allow" => 22,
        b"authorization" => 23,
        b"cache-control" => 24,
        b"content-disposition" => 25,
        b"content-encoding" => 26,
        b"content-language" => 27,
        b"content-length" => 28,
        b"content-location" => 29,
        b"content-range" => 30,
        b"content-type" => 31,
        b"cookie" => 32,
        b"date" => 33,
        b"etag" => 34,
        b"expect" => 35,
        b"expires" => 36,
        b"from" => 37,
        b"host" => 38,
        b"if-match" => 39,
        b"if-modified-since" => 40,
        b"if-none-match" => 41,
        b"if-range" => 42,
        b"if-unmodified-since" => 43,
        b"last-modified" => 44,
        b"link" => 45,
        b"location" => 46,
        b"max-forwards" => 47,
        b"proxy-authenticate" => 48,
        b"proxy-authorization" => 49,
        b"range" => 50,
        b"referer" => 51,
        b"refresh" => 52,
        b"retry-after" => 53,
        b"server" => 54,
        b"set-cookie" => 55,
        b"strict-transport-security" => 56,
        b"transfer-encoding" => 57,
        b"user-agent" => 58,
        b"vary" => 59,
        b"via" => 60,
        b"www-authenticate" => 61,
        _ => 0,
    }
}

/// Blocks between folds of per-connection stats into the shared set.
pub const STATS_FOLD_EVERY: u32 = 256;

const POOL_MAX: usize = 64;

/// Recycled representation buffers.
#[derive(Debug, Default)]
pub(crate) struct Pool(Vec<RepBuf>);

impl Pool {
    #[inline]
    pub(crate) fn take(&mut self) -> RepBuf {
        match self.0.pop() {
            Some(mut b) => {
                b.clear();
                b
            }
            None => RepBuf::with_capacity(24, 1024),
        }
    }

    #[inline]
    pub(crate) fn put(&mut self, mut b: RepBuf) {
        if self.0.len() < POOL_MAX && b.reps.capacity() <= 1024 && b.bytes.capacity() <= 1 << 16 {
            b.clear();
            self.0.push(b);
        }
    }
}

/// Per-connection selective decoder state (inside `hpack::Decoder`).
#[derive(Debug)]
pub(crate) struct SelectiveDecoder {
    pub(crate) mirror: MirrorTable,
    pub(crate) needed: NeededSet,
    pub(crate) stats: TranscodeStats,
    pub(crate) pool: Pool,
    /// Backing store for the `Bytes` handed to the sparse map (one
    /// allocation shared by all materialized values of a block).
    pub(crate) dec_buf: BytesMut,
    blocks: u32,
    /// Connection lifetime totals (what `stats` folded so far).
    total: TranscodeStats,
}

impl SelectiveDecoder {
    pub(crate) fn new(needed: NeededSet, max: usize) -> Self {
        let mut mirror = MirrorTable::new(next_tid(), max);
        mirror.set_needed(needed.clone());
        SelectiveDecoder {
            mirror,
            needed,
            stats: TranscodeStats::default(),
            pool: Pool::default(),
            dec_buf: BytesMut::with_capacity(512),
            blocks: 0,
            total: TranscodeStats::default(),
        }
    }

    /// A `Bytes` copy of `v` from the block-shared store.
    #[inline]
    pub(crate) fn share(&mut self, v: &[u8]) -> Bytes {
        if self.dec_buf.capacity() - self.dec_buf.len() < v.len() {
            self.dec_buf.reserve(v.len().max(512));
        }
        self.dec_buf.extend_from_slice(v);
        self.dec_buf.split().freeze()
    }

    /// Called once per completed block.
    #[inline]
    pub(crate) fn block_done(&mut self) {
        self.blocks += 1;
        if self.blocks % STATS_FOLD_EVERY == 0 {
            self.fold();
        }
    }

    fn fold(&mut self) {
        self.needed.add_stats(&self.stats);
        self.total.add(&self.stats);
        self.stats = TranscodeStats::default();
    }
}

impl Drop for SelectiveDecoder {
    fn drop(&mut self) {
        if self.needed.0.print_stats {
            let mut total = self.total;
            total.add(&self.stats);
            eprintln!(
                "[selective] decoder tid={} blocks={} {:?} mirror(len={} size={} arena={})",
                self.mirror.tid(),
                self.blocks,
                total,
                self.mirror.len(),
                self.mirror.size(),
                self.mirror.table().arena_len()
            );
        }
        self.fold();
    }
}

const EXTRA_SLOTS: usize = 8;

/// A field the stack added that we inserted into the encoder table:
/// `key = name ++ 0 ++ value`.
#[derive(Debug, Default)]
struct ExtraSlot {
    key: Vec<u8>,
    abs: u64,
}

/// Per-connection selective encoder state (inside `hpack::Encoder`).
#[derive(Debug)]
pub(crate) struct SelectiveEncoder {
    pub(crate) table: EncoderTable,
    pub(crate) needed: NeededSet,
    pub(crate) stats: TranscodeStats,
    pub(crate) pool: Pool,
    /// Huffman scratch for stack-added fields.
    pub(crate) scratch: BytesMut,
    extras: Vec<ExtraSlot>,
    extras_rr: usize,
    blocks: u32,
    /// Connection lifetime totals (what `stats` folded so far).
    total: TranscodeStats,
}

impl SelectiveEncoder {
    pub(crate) fn new(needed: NeededSet, max: usize) -> Self {
        SelectiveEncoder {
            table: EncoderTable::new(next_tid(), max),
            needed,
            stats: TranscodeStats::default(),
            pool: Pool::default(),
            scratch: BytesMut::with_capacity(256),
            extras: Vec::new(),
            extras_rr: 0,
            blocks: 0,
            total: TranscodeStats::default(),
        }
    }

    #[inline]
    pub(crate) fn block_done(&mut self) {
        self.blocks += 1;
        if self.blocks % STATS_FOLD_EVERY == 0 {
            self.fold();
        }
    }

    fn fold(&mut self) {
        self.needed.add_stats(&self.stats);
        self.total.add(&self.stats);
        self.stats = TranscodeStats::default();
    }

    /// Emit a field the stack produced (no wire representation). Sensitive
    /// values go out never-indexed. Others are inserted into the encoder
    /// table and remembered, so a repeat (same name and value) is sent as an
    /// index — like h2's stock encoder does.
    pub(crate) fn emit_extra<B: BufMut>(
        &mut self,
        sink: &mut Sink<'_, B>,
        name: &[u8],
        value: &[u8],
        never: bool,
    ) {
        let s = static_name_index(name);
        // Like h2's stock encoder: values of these names are never worth
        // indexing (they change per request or are secrets).
        let noindex = matches!(
            name,
            b":path"
                | b"age"
                | b"authorization"
                | b"content-length"
                | b"etag"
                | b"if-modified-since"
                | b"if-none-match"
                | b"location"
                | b"cookie"
                | b"set-cookie"
        );
        if never || noindex {
            sink.int(if never { 0x10 } else { 0x00 }, 4, s as usize);
            if s == 0 {
                transcode::huff_into(name, &mut self.scratch);
                sink.str(true, &self.scratch);
            }
            transcode::huff_into(value, &mut self.scratch);
            sink.str(true, &self.scratch);
            if never {
                self.stats.never_indexed += 1;
            } else {
                self.stats.lit_noindex += 1;
            }
            return;
        }
        // repeat?
        for slot in &self.extras {
            let k = &slot.key;
            if k.len() == name.len() + 1 + value.len()
                && &k[..name.len()] == name
                && &k[name.len() + 1..] == value
            {
                if let Some(j) = self.table.index_of(slot.abs) {
                    sink.int(0x80, 7, j as usize);
                    self.stats.extra_hit += 1;
                    return;
                }
            }
        }
        // literal with incremental indexing
        let mut name_scratch = BytesMut::new();
        sink.int(0x40, 6, s as usize);
        if s == 0 {
            transcode::huff_into(name, &mut self.scratch);
            sink.str(true, &self.scratch);
            name_scratch = std::mem::replace(&mut self.scratch, BytesMut::with_capacity(256));
        }
        transcode::huff_into(value, &mut self.scratch);
        sink.str(true, &self.scratch);
        let (abs, survived) = self.table.table.insert(Insert::plain(
            s,
            &name_scratch,
            true,
            name.len(),
            &self.scratch,
            true,
            value.len(),
        ));
        if s == 0 {
            // keep the larger buffer around
            if name_scratch.capacity() > self.scratch.capacity() {
                self.scratch = name_scratch;
            }
        }
        self.stats.extra_lit += 1;
        if survived {
            if self.extras.len() < EXTRA_SLOTS {
                self.extras.push(ExtraSlot::default());
            }
            let i = self.extras_rr % self.extras.len();
            self.extras_rr += 1;
            let slot = &mut self.extras[i];
            slot.key.clear();
            slot.key.extend_from_slice(name);
            slot.key.push(0);
            slot.key.extend_from_slice(value);
            slot.abs = abs;
        }
    }
}

impl Drop for SelectiveEncoder {
    fn drop(&mut self) {
        if self.needed.0.print_stats {
            let mut total = self.total;
            total.add(&self.stats);
            eprintln!(
                "[selective] encoder tid={} blocks={} {:?} table(len={} size={} arena={})",
                self.table.tid(),
                self.blocks,
                total,
                self.table.len(),
                self.table.size(),
                self.table.table().arena_len()
            );
        }
        self.fold();
    }
}
