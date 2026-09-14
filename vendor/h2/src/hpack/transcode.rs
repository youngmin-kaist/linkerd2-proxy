//! HPACK re-indexing transcoder ("selective decoding").
//!
//! Translates a header block encoded by peer C (against C's encoder table)
//! into a header block for peer B (against *our* encoder table toward B)
//! **representation by representation, without decoding header values**:
//!
//! * indexed static (≤61): copied as is;
//! * indexed dynamic: resolved in C's [`MirrorTable`], then re-resolved against
//!   B's [`EncoderTable`] — provenance hit → indexed with the new index, miss →
//!   literal *with incremental indexing* carrying the entry's name and the
//!   **original encoded value bytes**, followed by an insert into B's table;
//! * literal with incremental indexing: name re-resolved (static index kept,
//!   dynamic name index → that mirror entry's name bytes, literal → verbatim),
//!   value verbatim, inserted into the mirror *and* the encoder table;
//! * literal without indexing / never indexed: same name handling, value
//!   verbatim, no inserts, class preserved (never-indexed stays never-indexed,
//!   RFC 7541 §6.2.3);
//! * dynamic table size update: applied to the mirror only, never forwarded.
//!
//! Only the values of pseudo-headers and of the names in the mirror's
//! [`NeededSet`](super::selective::NeededSet) are ever Huffman-decoded;
//! everything else is walked count-only (for exact RFC 7541 §4.1 size
//! accounting) or skipped by encoded length.
//!
//! # Representation lists
//!
//! [`parse_one`] resolves the next representation against the mirror
//! (mutating it for inserts and size updates, delivering policy fields) and
//! appends one small `Copy` [`Rep`] to a [`RepBuf`]. All bytes a `Rep` refers
//! to — the block itself (copied once), snapshots of dynamic mirror entries,
//! decoded pseudo/needed values — live in `RepBuf::bytes`, addressed by
//! offsets; a `RepBuf` therefore owns nothing but two recyclable vectors and
//! can be parsed on one connection and emitted on any other ([`emit_one`]),
//! with no per-representation allocation or refcount traffic. Hit validation
//! uses the encoder table's direct-mapped provenance cache
//! (`(mirror tid, mirror serial)` → live entry), see [`super::mirror`].
//!
//! This is a port of `transcode_impl` in `dmesh-router-cpp/src/relay3.cpp`.
//!
//! # Errors
//!
//! Any malformed varint/index/string returns [`Error`]. The mirror may have
//! been partially updated by the failing block; since the peer sent a block
//! its own decoder-side accounting would reject too, the connection must be
//! failed with `COMPRESSION_ERROR`.

#![allow(dead_code)]
#![allow(missing_docs)]

use super::huffman;
use super::mirror::{static_value, EncoderTable, Insert, Kind, MirrorTable, STATIC_NAME_LEN};
use super::selective::{self, NeededSet};
use bytes::{BufMut, BytesMut};
use std::fmt;

/// Malformed input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    /// The block ended inside a representation.
    Truncated,
    /// Integer overflow / too many continuation bytes.
    InvalidInteger,
    /// Index 0, or a dynamic index beyond the mirror table.
    InvalidIndex,
    /// Invalid Huffman string (EOS inside, bad padding).
    InvalidHuffman,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Error::Truncated => "truncated header block",
            Error::InvalidInteger => "invalid HPACK integer",
            Error::InvalidIndex => "invalid HPACK table index",
            Error::InvalidHuffman => "invalid Huffman string",
        })
    }
}

impl std::error::Error for Error {}

/// The routing/policy fields extracted while walking a block. Buffers are
/// reused across calls; `have` says which fields the last block carried.
#[derive(Debug, Default, Clone)]
pub struct PolicyFields {
    /// Bitmask of `Kind::bit()` for the fields present.
    pub have: u8,
    /// `:method`
    pub method: Vec<u8>,
    /// `:path`
    pub path: Vec<u8>,
    /// `:authority`
    pub authority: Vec<u8>,
}

impl PolicyFields {
    /// Forget the fields of the previous block (keeps allocations).
    #[inline]
    pub fn clear(&mut self) {
        self.have = 0;
    }

    #[inline]
    pub fn has(&self, kind: Kind) -> bool {
        self.have & kind.bit() != 0
    }

    #[inline]
    pub fn method(&self) -> Option<&[u8]> {
        self.has(Kind::Method).then(|| self.method.as_slice())
    }

    #[inline]
    pub fn path(&self) -> Option<&[u8]> {
        self.has(Kind::Path).then(|| self.path.as_slice())
    }

    #[inline]
    pub fn authority(&self) -> Option<&[u8]> {
        self.has(Kind::Authority).then(|| self.authority.as_slice())
    }

    #[inline]
    fn set(&mut self, kind: Kind, v: &[u8]) {
        let dst = match kind {
            Kind::Method => &mut self.method,
            Kind::Path => &mut self.path,
            Kind::Authority => &mut self.authority,
            _ => return,
        };
        dst.clear();
        dst.extend_from_slice(v);
        self.have |= kind.bit();
    }
}

/// Counters, cumulative across calls.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TranscodeStats {
    /// Representations walked (including size updates).
    pub fields: u64,
    /// Indexed static representations.
    pub static_idx: u64,
    /// Indexed dynamic representations re-indexed (provenance hit).
    pub dyn_hit: u64,
    /// Indexed dynamic representations re-sent as literals (miss).
    pub dyn_miss: u64,
    /// Literals with incremental indexing emitted.
    pub lit_insert: u64,
    /// Literals without indexing emitted.
    pub lit_noindex: u64,
    /// Never-indexed literals emitted.
    pub never_indexed: u64,
    /// Dynamic table size updates received (never forwarded).
    pub size_updates: u64,
    /// Header block bytes walked.
    pub in_bytes: u64,
    /// Header block bytes emitted.
    pub out_bytes: u64,
    /// Blocks whose sparse map disagreed with the representations (a
    /// pseudo-header or materialized field was changed/removed/added by the
    /// stack, so a representation was skipped or a literal substituted).
    pub rewrites: u64,
    /// Fields the stack added, sent as literals (first time) …
    pub extra_lit: u64,
    /// … or as an index into the encoder table (repeat).
    pub extra_hit: u64,
}

impl TranscodeStats {
    pub fn add(&mut self, o: &TranscodeStats) {
        self.fields += o.fields;
        self.static_idx += o.static_idx;
        self.dyn_hit += o.dyn_hit;
        self.dyn_miss += o.dyn_miss;
        self.lit_insert += o.lit_insert;
        self.lit_noindex += o.lit_noindex;
        self.never_indexed += o.never_indexed;
        self.size_updates += o.size_updates;
        self.in_bytes += o.in_bytes;
        self.out_bytes += o.out_bytes;
        self.rewrites += o.rewrites;
        self.extra_lit += o.extra_lit;
        self.extra_hit += o.extra_hit;
    }
}

// ---- wire primitives ---------------------------------------------------------

/// RFC 7541 §5.1 integer with an N-bit prefix. `*i` must point at the prefix
/// byte; it is advanced past the integer.
#[inline]
fn varint(b: &[u8], i: &mut usize, prefix: u32) -> Result<u32, Error> {
    let mask = (1u32 << prefix) - 1;
    let &c = b.get(*i).ok_or(Error::Truncated)?;
    *i += 1;
    let v = (c as u32) & mask;
    if v != mask {
        return Ok(v);
    }
    let mut acc = v as u64;
    let mut m = 0u32;
    loop {
        if m > 28 {
            return Err(Error::InvalidInteger);
        }
        let &c = b.get(*i).ok_or(Error::Truncated)?;
        *i += 1;
        acc += ((c & 0x7f) as u64) << m;
        m += 7;
        if c & 0x80 == 0 {
            break;
        }
    }
    if acc > u32::MAX as u64 {
        return Err(Error::InvalidInteger);
    }
    Ok(acc as u32)
}

/// RFC 7541 §5.2 string: returns `(huffman, start, end)`.
#[inline]
fn read_str(b: &[u8], i: &mut usize) -> Result<(bool, usize, usize), Error> {
    let huff = *b.get(*i).ok_or(Error::Truncated)? & 0x80 != 0;
    let l = varint(b, i, 7)? as usize;
    let start = *i;
    if b.len() - start < l {
        return Err(Error::Truncated);
    }
    *i += l;
    Ok((huff, start, start + l))
}

/// Byte-counting writer over any `BufMut`.
pub(crate) struct Sink<'a, B: BufMut> {
    pub(crate) out: &'a mut B,
    pub(crate) n: usize,
}

impl<'a, B: BufMut> Sink<'a, B> {
    pub(crate) fn new(out: &'a mut B) -> Self {
        Sink { out, n: 0 }
    }

    #[inline]
    pub(crate) fn int(&mut self, first_bits: u8, prefix: u32, v: usize) {
        let m = (1usize << prefix) - 1;
        if v < m {
            self.out.put_u8(first_bits | v as u8);
            self.n += 1;
            return;
        }
        self.out.put_u8(first_bits | m as u8);
        self.n += 1;
        let mut v = v - m;
        while v >= 128 {
            self.out.put_u8((v & 127) as u8 | 128);
            self.n += 1;
            v >>= 7;
        }
        self.out.put_u8(v as u8);
        self.n += 1;
    }

    #[inline]
    pub(crate) fn str(&mut self, huff: bool, s: &[u8]) {
        self.int(if huff { 0x80 } else { 0x00 }, 7, s.len());
        self.out.put_slice(s);
        self.n += s.len();
    }

    /// Name reference: static index if it has one, else the literal bytes.
    #[inline]
    pub(crate) fn name(&mut self, first_bits: u8, prefix: u32, name_static: u8, huff: bool, enc: &[u8]) {
        if name_static != 0 {
            self.int(first_bits, prefix, name_static as usize);
        } else {
            self.int(first_bits, prefix, 0);
            self.str(huff, enc);
        }
    }
}

// ---- representations ---------------------------------------------------------

/// Wire class of a representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RepKind {
    /// Indexed, static table (`num` = index).
    Static = 0,
    /// Indexed, dynamic table (`num` = mirror serial; name/value bytes are a
    /// snapshot of the mirror entry).
    Dynamic = 1,
    /// Literal with incremental indexing (`num` = mirror abs, or `NO_NUM` if
    /// it did not survive the mirror insert).
    Insert = 2,
    /// Literal without indexing.
    NoIndex = 3,
    /// Literal never indexed.
    Never = 4,
    /// Dynamic table size update (`num` = new max; already applied to the
    /// mirror, never re-emitted).
    SizeUpdate = 5,
}

/// "no number"
pub const NO_NUM: u64 = u64::MAX;

/// One representation, resolved against a mirror. All byte ranges index the
/// owning [`RepBuf`]'s `bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rep {
    pub kind: RepKind,
    /// Pseudo kind of the name (`Kind::None` for regular fields).
    pub hkind: Kind,
    pub name_huff: bool,
    pub val_huff: bool,
    /// Whether `dec_off..dec_off+dec_len` holds the decoded value.
    pub has_dec: bool,
    /// Static index the name refers to (0 = literal name at `name_off`).
    pub name_static: u8,
    /// `NeededSet` tag of the name.
    pub tag: u16,
    pub name_off: u32,
    pub name_len: u32,
    pub val_off: u32,
    pub val_len: u32,
    pub dec_off: u32,
    pub dec_len: u32,
    pub name_dec_len: u32,
    pub val_dec_len: u32,
    pub num: u64,
}

impl Rep {
    #[inline]
    pub fn is_size_update(&self) -> bool {
        self.kind == RepKind::SizeUpdate
    }

    #[inline]
    pub fn is_pseudo(&self) -> bool {
        self.hkind != Kind::None
    }

    /// RFC 7541 §4.1 decoded size (for `SETTINGS_MAX_HEADER_LIST_SIZE`).
    #[inline]
    pub fn decoded_size(&self) -> usize {
        (self.name_dec_len + self.val_dec_len + 32) as usize
    }

    /// Encoded name bytes (empty for static names).
    #[inline]
    pub fn name<'a>(&self, bytes: &'a [u8]) -> &'a [u8] {
        &bytes[self.name_off as usize..(self.name_off + self.name_len) as usize]
    }

    /// Encoded value bytes.
    #[inline]
    pub fn val<'a>(&self, bytes: &'a [u8]) -> &'a [u8] {
        &bytes[self.val_off as usize..(self.val_off + self.val_len) as usize]
    }

    /// Decoded value, available for pseudo-headers, needed names and all
    /// static references.
    #[inline]
    pub fn dec<'a>(&self, bytes: &'a [u8]) -> Option<&'a [u8]> {
        if self.kind == RepKind::Static {
            return Some(selective::static_field_value(self.num as u32));
        }
        if !self.has_dec {
            return None;
        }
        Some(&bytes[self.dec_off as usize..(self.dec_off + self.dec_len) as usize])
    }
}

/// A representation list and the bytes it refers to. Both vectors are
/// recycled through per-connection pools.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepBuf {
    pub reps: Vec<Rep>,
    pub bytes: Vec<u8>,
}

impl RepBuf {
    pub fn with_capacity(reps: usize, bytes: usize) -> RepBuf {
        RepBuf {
            reps: Vec::with_capacity(reps),
            bytes: Vec::with_capacity(bytes),
        }
    }

    #[inline]
    pub fn clear(&mut self) {
        self.reps.clear();
        self.bytes.clear();
    }

    /// Append a block fragment; returns its offset.
    #[inline]
    pub fn push_block(&mut self, block: &[u8]) -> usize {
        let off = self.bytes.len();
        self.bytes.extend_from_slice(block);
        off
    }
}

// ---- the walk --------------------------------------------------------------------

/// Parse the representation at `buf.bytes[*i]` (the block occupies
/// `..block_end` of `buf.bytes`), resolving it against `mirror` (which is
/// updated for inserts and size updates), delivering policy fields, and
/// appending one [`Rep`] to `buf.reps`. Advances `*i`. On `Err(Truncated)`
/// nothing has been applied, so the caller may retry from the old `*i` once
/// more bytes arrive.
#[inline]
pub fn parse_one(
    buf: &mut RepBuf,
    block_end: usize,
    i: &mut usize,
    mirror: &mut MirrorTable,
    policy: Option<&mut PolicyFields>,
    stats: &mut TranscodeStats,
) -> Result<(), Error> {
    let b: &[u8] = &buf.bytes[..block_end];
    let c = *b.get(*i).ok_or(Error::Truncated)?;
    stats.fields += 1;

    // ---- indexed
    if c & 0x80 != 0 {
        let idx = varint(b, i, 7)?;
        if idx == 0 {
            return Err(Error::InvalidIndex);
        }
        if idx <= 61 {
            stats.static_idx += 1;
            let hkind = Kind::of_static(idx);
            if let Some(pf) = policy {
                if hkind != Kind::None {
                    pf.set(hkind, static_value(idx));
                }
            }
            let tag = mirror.needed.as_ref().map_or(0, |n| n.tag_of_static(idx));
            buf.reps.push(Rep {
                kind: RepKind::Static,
                hkind,
                name_huff: false,
                val_huff: false,
                has_dec: false,
                name_static: idx as u8,
                tag,
                name_off: 0,
                name_len: 0,
                val_off: 0,
                val_len: 0,
                dec_off: 0,
                dec_len: 0,
                name_dec_len: STATIC_NAME_LEN[idx as usize] as u32,
                val_dec_len: selective::static_field_value(idx).len() as u32,
                num: idx as u64,
            });
            return Ok(());
        }
        let e = mirror.table.get(idx).ok_or(Error::InvalidIndex)?;
        let ent = *e.entry;
        if let Some(pf) = policy {
            if ent.kind != Kind::None {
                if let Some(d) = e.val_dec() {
                    pf.set(ent.kind, d);
                }
            }
        }
        // Snapshot the entry bytes: the mirror may evict it before emission.
        let off = buf.bytes.len() as u32;
        buf.bytes.extend_from_slice(e.wire());
        let dec_len = match e.val_dec() {
            Some(d) => {
                buf.bytes.extend_from_slice(d);
                d.len() as u32
            }
            None => 0,
        };
        let wire_len = ent.name_enc_len + ent.val_enc_len;
        buf.reps.push(Rep {
            kind: RepKind::Dynamic,
            hkind: ent.kind,
            name_huff: ent.name_huff,
            val_huff: ent.val_huff,
            has_dec: ent.has_dec(),
            name_static: ent.name_static,
            tag: ent.tag,
            name_off: off,
            name_len: ent.name_enc_len,
            val_off: off + ent.name_enc_len,
            val_len: ent.val_enc_len,
            dec_off: off + wire_len,
            dec_len,
            name_dec_len: ent.name_dec_len,
            val_dec_len: ent.val_dec_len,
            num: ent.serial,
        });
        return Ok(());
    }

    // ---- dynamic table size update (peer-local; never forwarded)
    if c & 0xe0 == 0x20 {
        let max = varint(b, i, 5)? as usize;
        mirror.table.set_max(max);
        stats.size_updates += 1;
        buf.reps.push(Rep {
            kind: RepKind::SizeUpdate,
            hkind: Kind::None,
            name_huff: false,
            val_huff: false,
            has_dec: false,
            name_static: 0,
            tag: 0,
            name_off: 0,
            name_len: 0,
            val_off: 0,
            val_len: 0,
            dec_off: 0,
            dec_len: 0,
            name_dec_len: 0,
            val_dec_len: 0,
            num: max as u64,
        });
        return Ok(());
    }

    // ---- literal: 01 (insert), 0000 (no index), 0001 (never index)
    let insert = c & 0x40 != 0;
    let kind = if insert {
        RepKind::Insert
    } else if c & 0x10 != 0 {
        RepKind::Never
    } else {
        RepKind::NoIndex
    };
    let prefix = if insert { 6 } else { 4 };
    let idx = varint(b, i, prefix)?;

    // name
    let mut name_static = 0u8;
    let mut name_huff = false;
    let mut name_off = 0usize;
    let mut name_len = 0usize;
    // dynamic name index whose name is a literal: bytes copied below
    let mut name_from_mirror: Option<u32> = None;
    let name_dec_len;
    let hkind;
    let tag;
    if idx != 0 {
        if idx <= 61 {
            name_static = idx as u8;
            name_dec_len = STATIC_NAME_LEN[idx as usize] as usize;
            hkind = Kind::of_static(idx);
            tag = mirror.needed.as_ref().map_or(0, |n| n.tag_of_static(idx));
        } else {
            let ne = mirror.table.get_entry(idx).ok_or(Error::InvalidIndex)?;
            name_static = ne.name_static;
            name_huff = ne.name_huff;
            name_len = ne.name_enc_len as usize;
            name_dec_len = ne.name_dec_len as usize;
            hkind = ne.kind;
            tag = ne.tag;
            if name_static == 0 {
                name_from_mirror = Some(idx);
            }
        }
    } else {
        let (huff, s0, s1) = read_str(b, i)?;
        name_huff = huff;
        name_off = s0;
        name_len = s1 - s0;
        let s = &b[s0..s1];
        // Decode the name only to classify it (and for its exact length).
        if huff {
            let d = huffman::decode(s, &mut mirror.scratch).map_err(|_| Error::InvalidHuffman)?;
            name_dec_len = d.len();
            hkind = Kind::of_name(&d);
            tag = mirror.needed.as_ref().map_or(0, |n| n.tag_of(&d));
        } else {
            name_dec_len = s.len();
            hkind = Kind::of_name(s);
            tag = mirror.needed.as_ref().map_or(0, |n| n.tag_of(s));
        }
    }

    // value
    let (val_huff, v0, v1) = read_str(b, i)?;
    let val_len = v1 - v0;
    let want_dec = hkind != Kind::None || NeededSet::is_needed(tag);
    // Decoded value: raw → the value bytes themselves; Huffman → decoded into
    // `scratch` (a `BytesMut` split off the mirror's scratch).
    let mut huff_dec = None;
    let val_dec_len = if want_dec {
        if val_huff {
            let d = huffman::decode(&b[v0..v1], &mut mirror.scratch).map_err(|_| Error::InvalidHuffman)?;
            let n = d.len();
            huff_dec = Some(d);
            n
        } else {
            val_len
        }
    } else if insert && val_huff {
        // Exact decoded length for table accounting, no output.
        huffman::decoded_len(&b[v0..v1]).map_err(|_| Error::InvalidHuffman)?
    } else {
        val_len
    };

    // All reads of the block are done: extend `buf.bytes` with what the rep
    // needs beyond the block (mirror-sourced name, Huffman-decoded value).
    if let Some(nidx) = name_from_mirror {
        let off = buf.bytes.len();
        let e = mirror.table.get(nidx).ok_or(Error::InvalidIndex)?;
        buf.bytes.extend_from_slice(e.name_enc());
        name_off = off;
    }
    let (dec_off, dec_len) = match &huff_dec {
        Some(d) => {
            let off = buf.bytes.len();
            buf.bytes.extend_from_slice(d);
            (off, d.len())
        }
        None => (v0, val_len),
    };
    drop(huff_dec);

    let bytes: &[u8] = &buf.bytes;
    let name_enc = &bytes[name_off..name_off + name_len];
    let val_enc = &bytes[v0..v1];
    let val_dec: Option<&[u8]> = if want_dec {
        Some(&bytes[dec_off..dec_off + dec_len])
    } else {
        None
    };

    if let Some(pf) = policy {
        if hkind != Kind::None {
            if let Some(d) = val_dec {
                pf.set(hkind, d);
            }
        }
    }

    let mut num = NO_NUM;
    if insert {
        let (abs, survived) = mirror.table.insert(Insert {
            name_static,
            name_enc,
            name_huff,
            name_dec_len,
            val_enc,
            val_huff,
            val_dec_len,
            kind: hkind,
            tag,
            val_dec,
            src_tid: 0,
            src_serial: 0,
        });
        if survived {
            num = abs;
        }
    }

    buf.reps.push(Rep {
        kind,
        hkind,
        name_huff,
        val_huff,
        has_dec: want_dec,
        name_static,
        tag,
        name_off: name_off as u32,
        name_len: name_len as u32,
        val_off: v0 as u32,
        val_len: val_len as u32,
        dec_off: dec_off as u32,
        dec_len: dec_len as u32,
        name_dec_len: name_dec_len as u32,
        val_dec_len: val_dec_len as u32,
        num,
    });
    Ok(())
}

/// Emit one representation against `enc`, updating `enc` (inserts and
/// provenance). `bytes` is the owning `RepBuf`'s byte store.
#[inline]
pub(crate) fn emit_one<B: BufMut>(
    rep: &Rep,
    bytes: &[u8],
    mirror_tid: u32,
    enc: &mut EncoderTable,
    sink: &mut Sink<'_, B>,
    stats: &mut TranscodeStats,
) {
    match rep.kind {
        RepKind::Static => sink.int(0x80, 7, rep.num as usize),
        RepKind::Dynamic => match enc.table.lookup_provenance(mirror_tid, rep.num) {
            Some(j) => {
                stats.dyn_hit += 1;
                sink.int(0x80, 7, j as usize);
            }
            None => {
                stats.dyn_miss += 1;
                let name = rep.name(bytes);
                let val = rep.val(bytes);
                sink.name(0x40, 6, rep.name_static, rep.name_huff, name);
                sink.str(rep.val_huff, val);
                enc.table.insert(Insert {
                    name_static: rep.name_static,
                    name_enc: name,
                    name_huff: rep.name_huff,
                    name_dec_len: rep.name_dec_len as usize,
                    val_enc: val,
                    val_huff: rep.val_huff,
                    val_dec_len: rep.val_dec_len as usize,
                    kind: Kind::None,
                    tag: 0,
                    val_dec: None,
                    src_tid: mirror_tid,
                    src_serial: rep.num,
                });
            }
        },
        RepKind::Insert | RepKind::NoIndex | RepKind::Never => {
            let (first, prefix) = match rep.kind {
                RepKind::Insert => (0x40, 6),
                RepKind::NoIndex => (0x00, 4),
                _ => (0x10, 4),
            };
            let name = rep.name(bytes);
            let val = rep.val(bytes);
            sink.name(first, prefix, rep.name_static, rep.name_huff, name);
            sink.str(rep.val_huff, val);
            match rep.kind {
                RepKind::Insert => {
                    stats.lit_insert += 1;
                    let (src_tid, src_serial) = if rep.num != NO_NUM {
                        (mirror_tid, rep.num)
                    } else {
                        (0, 0)
                    };
                    enc.table.insert(Insert {
                        name_static: rep.name_static,
                        name_enc: name,
                        name_huff: rep.name_huff,
                        name_dec_len: rep.name_dec_len as usize,
                        val_enc: val,
                        val_huff: rep.val_huff,
                        val_dec_len: rep.val_dec_len as usize,
                        kind: Kind::None,
                        tag: 0,
                        val_dec: None,
                        src_tid,
                        src_serial,
                    });
                }
                RepKind::NoIndex => stats.lit_noindex += 1,
                _ => stats.never_indexed += 1,
            }
        }
        RepKind::SizeUpdate => {}
    }
}

/// Emit any queued [`EncoderTable::resize`] at the start of a block.
#[inline]
pub(crate) fn emit_pending_resize<B: BufMut>(enc: &mut EncoderTable, sink: &mut Sink<'_, B>) {
    if let Some((min, fin)) = enc.take_pending_resize() {
        if min < fin {
            sink.int(0x20, 5, min);
        }
        sink.int(0x20, 5, fin);
    }
}

/// Transcode `block` (a complete header block as sent by the peer owning
/// `mirror`) into `out` against `enc`, using `buf` as scratch (cleared).
///
/// * `enc == None`: mirror-only walk (table sync + policy extraction, nothing
///   written) — for trailers with no live backend, denied streams, etc.
/// * `policy`: receives `:method`/`:path`/`:authority` of this block (cleared
///   first).
///
/// A queued [`EncoderTable::resize`] is signalled at the start of `out`.
pub fn transcode_with<B: BufMut>(
    buf: &mut RepBuf,
    block: &[u8],
    mirror: &mut MirrorTable,
    mut enc: Option<&mut EncoderTable>,
    out: &mut B,
    mut policy: Option<&mut PolicyFields>,
    stats: &mut TranscodeStats,
) -> Result<(), Error> {
    if let Some(pf) = policy.as_deref_mut() {
        pf.clear();
    }
    stats.in_bytes += block.len() as u64;
    buf.clear();
    let end = buf.push_block(block) + block.len();
    let mut i = 0usize;
    while i < end {
        parse_one(buf, end, &mut i, mirror, policy.as_deref_mut(), stats)?;
    }
    if let Some(enc) = enc.as_deref_mut() {
        let mut sink = Sink::new(out);
        emit_pending_resize(enc, &mut sink);
        for rep in &buf.reps {
            emit_one(rep, &buf.bytes, mirror.tid(), enc, &mut sink, stats);
        }
        stats.out_bytes += sink.n as u64;
    }
    Ok(())
}

/// [`transcode_with`] with a temporary scratch buffer.
pub fn transcode<B: BufMut>(
    block: &[u8],
    mirror: &mut MirrorTable,
    enc: Option<&mut EncoderTable>,
    out: &mut B,
    policy: Option<&mut PolicyFields>,
    stats: &mut TranscodeStats,
) -> Result<(), Error> {
    let mut buf = RepBuf::with_capacity(16, block.len() + 256);
    transcode_with(&mut buf, block, mirror, enc, out, policy, stats)
}

/// Huffman-encode `s` into `scratch` (cleared first) — for stack-added
/// fields; mirrors h2's stock `encode_str`.
pub(crate) fn huff_into(s: &[u8], scratch: &mut BytesMut) {
    scratch.clear();
    if !s.is_empty() {
        huffman::encode(s, scratch);
    }
}

// ---- tests ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::hpack::{BytesStr, Decoder, DecoderError, Encoder, Header};
    use bytes::Bytes;
    use http::header::{HeaderName, HeaderValue};
    use http::Method;
    use std::io::Cursor;

    type Pair = (Vec<u8>, Vec<u8>);

    fn field(name: &str, val: &[u8], sensitive: bool) -> Header<Option<HeaderName>> {
        let mut value = HeaderValue::from_bytes(val).unwrap();
        value.set_sensitive(sensitive);
        Header::Field {
            name: Some(HeaderName::from_bytes(name.as_bytes()).unwrap()),
            value,
        }
    }

    /// The 9-field request list of the C++ test (`test/test_transcode.cpp`),
    /// plus the expected (name, value) pairs. `authority` varies per client.
    fn request(r: usize, authority: &str) -> (Vec<Header<Option<HeaderName>>>, Vec<Pair>) {
        let tid = format!(
            "{:016x}:{:016x}:0:1",
            (r as u64).wrapping_mul(2654435761),
            (r as u64).wrapping_mul(40503)
        );
        let path = match r % 3 {
            0 => "/search.Search/Nearby",
            1 => "/ok",
            _ => "/api/v1/items?id=12345",
        };
        let hdrs = vec![
            Header::Method(Method::POST),
            Header::Scheme(BytesStr::from("http")),
            Header::Path(BytesStr::from(path)),
            Header::Authority(BytesStr::from(authority)),
            field("content-type", b"application/grpc", false),
            field("user-agent", b"grpc-go/1.71.0", false),
            field("te", b"trailers", false),
            field("uber-trace-id", tid.as_bytes(), false),
            field("x-secret", b"hunter2", true), // never-indexed
        ];
        let expect: Vec<Pair> = vec![
            (b":method".to_vec(), b"POST".to_vec()),
            (b":scheme".to_vec(), b"http".to_vec()),
            (b":path".to_vec(), path.as_bytes().to_vec()),
            (b":authority".to_vec(), authority.as_bytes().to_vec()),
            (b"content-type".to_vec(), b"application/grpc".to_vec()),
            (b"user-agent".to_vec(), b"grpc-go/1.71.0".to_vec()),
            (b"te".to_vec(), b"trailers".to_vec()),
            (b"uber-trace-id".to_vec(), tid.into_bytes()),
            (b"x-secret".to_vec(), b"hunter2".to_vec()),
        ];
        (hdrs, expect)
    }

    fn encode(enc: &mut Encoder, hdrs: Vec<Header<Option<HeaderName>>>) -> Bytes {
        let mut dst = BytesMut::with_capacity(1024);
        enc.encode(hdrs, &mut dst);
        dst.freeze()
    }

    fn decode_pairs(dec: &mut Decoder, block: &[u8]) -> Result<Vec<Pair>, DecoderError> {
        let mut buf = BytesMut::from(block);
        let mut cur = Cursor::new(&mut buf);
        let mut out = Vec::new();
        dec.decode(&mut cur, |h| {
            out.push((h.name().as_slice().to_vec(), h.value_slice().to_vec()))
        })?;
        Ok(out)
    }

    fn check_policy(pf: &PolicyFields, expect: &[Pair]) {
        assert_eq!(pf.method(), Some(&expect[0].1[..]));
        assert_eq!(pf.path(), Some(&expect[2].1[..]));
        assert_eq!(pf.authority(), Some(&expect[3].1[..]));
    }

    #[test]
    fn round_trip_300_blocks() {
        let mut client = Encoder::new(4096, 0);
        let mut backend = Decoder::new(4096);
        let mut mirror = MirrorTable::new(2, 4096);
        let mut enc = EncoderTable::new(3, 4096);
        let mut stats = TranscodeStats::default();
        let mut pf = PolicyFields::default();
        let mut buf = RepBuf::default();

        for r in 0..300 {
            let (hdrs, expect) = request(r, "srv-search:8082");
            // Client shrinks its table to 0 for one block, then restores it
            // (the curl behaviour). Both updates are mirror-only.
            if r == 5 {
                client.update_max_size(0);
            }
            if r == 6 {
                client.update_max_size(4096);
            }
            let blk = encode(&mut client, hdrs);
            let mut out = BytesMut::new();
            transcode_with(
                &mut buf,
                &blk,
                &mut mirror,
                Some(&mut enc),
                &mut out,
                Some(&mut pf),
                &mut stats,
            )
            .unwrap_or_else(|e| panic!("r={} transcode failed: {}", r, e));
            let got = decode_pairs(&mut backend, &out)
                .unwrap_or_else(|e| panic!("r={} backend decode failed: {:?}", r, e));
            assert_eq!(got, expect, "r={}", r);
            check_policy(&pf, &expect);
            if r == 5 {
                assert_eq!(mirror.max(), 0);
                assert_eq!(mirror.len(), 0);
            }
            if r == 6 {
                assert_eq!(mirror.max(), 4096);
            }
        }
        println!("round_trip_300_blocks: {:?}", stats);
        assert_eq!(stats.dyn_miss, 0, "1:1 must never miss");
        assert_eq!(stats.never_indexed, 300);
        assert_eq!(stats.size_updates, 2);
        assert_eq!(stats.fields, 300 * 9 + 2);
        assert!(stats.dyn_hit > 0);
        assert!(stats.out_bytes > stats.in_bytes); // literal names for dynamic name refs
        assert!(stats.out_bytes < stats.in_bytes + 20 * 300);
        // arena stays bounded (FIFO reclamation)
        assert!(mirror.table().arena_len() < 4 * 4096, "{}", mirror.table().arena_len());
        assert!(enc.table().arena_len() < 4 * 4096, "{}", enc.table().arena_len());
    }

    /// The curl case, hand-built: size update to 0, then a literal WITH
    /// incremental indexing (immediately evicted from the mirror), then the
    /// table restored and an indexed reference to a fresh insert.
    #[test]
    fn size_update_zero_then_immediate_eviction() {
        let mut backend = Decoder::new(4096);
        let mut mirror = MirrorTable::new(2, 4096);
        let mut enc = EncoderTable::new(3, 4096);
        let mut stats = TranscodeStats::default();

        let blk1: &[u8] = &[
            0x20, // size update 0
            0x40, 0x03, b'f', b'o', b'o', 0x03, b'b', b'a', b'r', // insert foo: bar
            0x40, 0x03, b'b', b'a', b'z', 0x03, b'q', b'u', b'x', // insert baz: qux
        ];
        let mut out = BytesMut::new();
        transcode(blk1, &mut mirror, Some(&mut enc), &mut out, None, &mut stats).unwrap();
        assert_eq!(mirror.len(), 0, "evicted on insert");
        assert_eq!(mirror.max(), 0);
        assert_eq!(enc.len(), 2, "encoder side keeps them (backend max is 4096)");
        assert_eq!(
            decode_pairs(&mut backend, &out).unwrap(),
            vec![
                (b"foo".to_vec(), b"bar".to_vec()),
                (b"baz".to_vec(), b"qux".to_vec())
            ]
        );
        assert!(!out.starts_with(&[0x20]), "size update must not be forwarded");

        let blk2: &[u8] = &[
            0x3f, 0xe1, 0x1f, // size update 4096
            0x40, 0x03, b'f', b'o', b'o', 0x03, b'b', b'a', b'r', // insert foo: bar
            0xbe, // indexed 62 -> foo: bar
        ];
        let mut out = BytesMut::new();
        transcode(blk2, &mut mirror, Some(&mut enc), &mut out, None, &mut stats).unwrap();
        assert_eq!(mirror.len(), 1);
        assert_eq!(
            decode_pairs(&mut backend, &out).unwrap(),
            vec![
                (b"foo".to_vec(), b"bar".to_vec()),
                (b"foo".to_vec(), b"bar".to_vec())
            ]
        );
        assert_eq!(stats.dyn_hit, 1);
        assert_eq!(stats.dyn_miss, 0);
        assert_eq!(stats.size_updates, 2);
        assert_eq!(stats.lit_insert, 3);
    }

    /// N:M — two clients (two stock encoders, two mirrors) interleaved into ONE
    /// encoder table / backend decoder, request by request.
    #[test]
    fn two_clients_one_backend() {
        let mut clients = [Encoder::new(4096, 0), Encoder::new(4096, 0)];
        let mut mirrors = [MirrorTable::new(10, 4096), MirrorTable::new(11, 4096)];
        let auth = ["srv-a:8082", "srv-b:8082"];
        let mut backend = Decoder::new(4096);
        let mut enc = EncoderTable::new(20, 4096);
        let mut stats = TranscodeStats::default();
        let mut pf = PolicyFields::default();

        for r in 0..300 {
            for c in 0..2 {
                let (hdrs, expect) = request(r * 7 + c, auth[c]);
                let blk = encode(&mut clients[c], hdrs);
                let mut out = BytesMut::new();
                transcode(
                    &blk,
                    &mut mirrors[c],
                    Some(&mut enc),
                    &mut out,
                    Some(&mut pf),
                    &mut stats,
                )
                .unwrap_or_else(|e| panic!("r={} c={} transcode failed: {}", r, c, e));
                let got = decode_pairs(&mut backend, &out)
                    .unwrap_or_else(|e| panic!("r={} c={} decode failed: {:?}", r, c, e));
                assert_eq!(got, expect, "r={} c={}", r, c);
                check_policy(&pf, &expect);
            }
        }
        println!("two_clients_one_backend: {:?}", stats);
        assert_eq!(stats.never_indexed, 600);
        assert!(stats.dyn_hit > stats.dyn_miss * 10);
    }

    /// One client transcoded alternately into TWO encoder tables / decoders
    /// (per-request load balancing). Misses happen only when a mirror entry
    /// first meets a backend that has not seen it; they must not recur once
    /// both backends hold the entry.
    #[test]
    fn one_client_two_backends() {
        let mut client = Encoder::new(4096, 0);
        let mut mirror = MirrorTable::new(2, 4096);
        let mut backends = [Decoder::new(4096), Decoder::new(4096)];
        let mut encs = [EncoderTable::new(30, 4096), EncoderTable::new(31, 4096)];
        let mut stats = TranscodeStats::default();
        let mut pf = PolicyFields::default();
        let mut misses_per_block = Vec::new();

        for r in 0..300 {
            let (hdrs, expect) = request(r, "srv-search:8082");
            let blk = encode(&mut client, hdrs);
            let bk = r % 2;
            let mut out = BytesMut::new();
            let before = stats.dyn_miss;
            transcode(
                &blk,
                &mut mirror,
                Some(&mut encs[bk]),
                &mut out,
                Some(&mut pf),
                &mut stats,
            )
            .unwrap_or_else(|e| panic!("r={} transcode failed: {}", r, e));
            misses_per_block.push(stats.dyn_miss - before);
            let got = decode_pairs(&mut backends[bk], &out)
                .unwrap_or_else(|e| panic!("r={} decode failed: {:?}", r, e));
            assert_eq!(got, expect, "r={}", r);
            check_policy(&pf, &expect);
        }
        println!("one_client_two_backends: {:?}", stats);
        println!("  misses per block: {:?}", &misses_per_block[..12]);
        assert_eq!(misses_per_block[0], 0);
        assert_eq!(misses_per_block[1], 4);
        assert_eq!(misses_per_block[2], 0);
        assert_eq!(misses_per_block[3], 0);
        for w in misses_per_block[4..].windows(2) {
            assert!(w[0] == 0 || w[1] == 0, "misses {:?}", w);
        }
        let total: u64 = misses_per_block.iter().sum();
        assert!(total < 40, "dyn_miss={}", total);
        assert!(stats.dyn_hit > 1000);
    }

    /// A 1.5 KB `authorization: Bearer …` value. h2 encodes it as a literal
    /// without indexing (static name 23); we pass it verbatim.
    #[test]
    fn heavy_header() {
        let bearer = {
            let mut s = String::from("Bearer ");
            let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
            for k in 0..1500usize {
                s.push(alphabet[(k * 31 + k / 7) % alphabet.len()] as char);
            }
            s
        };
        let mut client = Encoder::new(4096, 0);
        let mut backend = Decoder::new(4096);
        let mut mirror = MirrorTable::new(2, 4096);
        let mut enc = EncoderTable::new(3, 4096);
        let mut stats = TranscodeStats::default();

        let blk = encode(
            &mut client,
            vec![field("authorization", bearer.as_bytes(), false)],
        );
        let mut out = BytesMut::new();
        transcode(&blk, &mut mirror, Some(&mut enc), &mut out, None, &mut stats).unwrap();
        assert_eq!(&out[..], &blk[..]);
        assert_eq!(
            decode_pairs(&mut backend, &out).unwrap(),
            vec![(b"authorization".to_vec(), bearer.clone().into_bytes())]
        );

        for r in 0..20 {
            let (mut hdrs, mut expect) = request(r, "srv-search:8082");
            hdrs.push(field("authorization", bearer.as_bytes(), false));
            expect.push((b"authorization".to_vec(), bearer.clone().into_bytes()));
            let blk = encode(&mut client, hdrs);
            let mut out = BytesMut::new();
            transcode(&blk, &mut mirror, Some(&mut enc), &mut out, None, &mut stats).unwrap();
            assert_eq!(decode_pairs(&mut backend, &out).unwrap(), expect, "r={}", r);
            assert!(out.len() >= blk.len() && out.len() <= blk.len() + 16, "r={} in={} out={}", r, blk.len(), out.len());
            assert!(blk.len() > 1100, "huffman-coded 1.5 KB value");
        }
        println!("heavy_header: {:?}", stats);
        assert_eq!(stats.dyn_miss, 0);
    }

    #[test]
    fn malformed_input_returns_err() {
        let mut client = Encoder::new(4096, 0);
        let (hdrs, _) = request(0, "srv-search:8082");
        let blk = encode(&mut client, hdrs);
        let cases: Vec<(&str, Vec<u8>, Error)> = vec![
            ("truncated inside :path value", blk[..4].to_vec(), Error::Truncated),
            ("truncated after prefix byte", vec![0x40], Error::Truncated),
            ("string longer than block", vec![0x40, 0x05, b'a', b'b', 0x00], Error::Truncated),
            ("index 0", vec![0x80], Error::InvalidIndex),
            ("dynamic index beyond table", vec![0xbe], Error::InvalidIndex),
            ("literal name index beyond table", vec![0x7e, 0x01, b'x'], Error::InvalidIndex),
            ("no-index name index beyond table", vec![0x0f, 0x2f, 0x01, b'x'], Error::InvalidIndex),
            ("varint overflow", vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f], Error::InvalidInteger),
            ("huffman EOS in literal name", vec![0x00, 0x84, 0xff, 0xff, 0xff, 0xff, 0x00], Error::InvalidHuffman),
            ("huffman EOS in policy value", vec![0x44, 0x84, 0xff, 0xff, 0xff, 0xff], Error::InvalidHuffman),
            ("huffman EOS in count-only value", vec![0x5f, 0x84, 0xff, 0xff, 0xff, 0xff], Error::InvalidHuffman),
        ];
        for (what, bytes, want) in cases {
            let mut mirror = MirrorTable::new(2, 4096);
            let mut enc = EncoderTable::new(3, 4096);
            let mut stats = TranscodeStats::default();
            let mut pf = PolicyFields::default();
            let mut out = Vec::new();
            let got = transcode(&bytes, &mut mirror, Some(&mut enc), &mut out, Some(&mut pf), &mut stats);
            assert_eq!(got, Err(want), "{}", what);
            let mut mirror = MirrorTable::new(2, 4096);
            let got = transcode(&bytes, &mut mirror, None, &mut out, None, &mut stats);
            assert_eq!(got, Err(want), "{} (mirror-only)", what);
        }
    }

    /// `enc == None`: table sync + policy only, nothing written; a later
    /// block with an encoder must still round-trip (misses, then hits).
    #[test]
    fn mirror_only_walk() {
        let mut client = Encoder::new(4096, 0);
        let mut backend = Decoder::new(4096);
        let mut mirror = MirrorTable::new(2, 4096);
        let mut enc = EncoderTable::new(3, 4096);
        let mut stats = TranscodeStats::default();
        let mut pf = PolicyFields::default();

        let (hdrs, expect) = request(0, "srv-search:8082");
        let blk = encode(&mut client, hdrs);
        let mut out = BytesMut::new();
        transcode(&blk, &mut mirror, None, &mut out, Some(&mut pf), &mut stats).unwrap();
        assert!(out.is_empty());
        assert_eq!(stats.out_bytes, 0);
        assert_eq!(stats.lit_insert + stats.lit_noindex + stats.never_indexed, 0);
        check_policy(&pf, &expect);
        assert_eq!(mirror.len(), 5); // authority, content-type, user-agent, te, uber-trace-id

        for r in 1..4 {
            let (hdrs, expect) = request(r, "srv-search:8082");
            let blk = encode(&mut client, hdrs);
            let mut out = BytesMut::new();
            transcode(&blk, &mut mirror, Some(&mut enc), &mut out, Some(&mut pf), &mut stats).unwrap();
            assert_eq!(decode_pairs(&mut backend, &out).unwrap(), expect, "r={}", r);
            check_policy(&pf, &expect);
        }
        println!("mirror_only_walk: {:?}", stats);
        assert_eq!(stats.dyn_miss, 4, "the 4 entries the backend never saw");
        assert_eq!(stats.dyn_hit, 8);
    }

    /// Policy values from static indices, literal pseudo-header names, and
    /// mirror entries; unrelated fields leave `have` untouched.
    #[test]
    fn policy_extraction_paths() {
        let mut mirror = MirrorTable::new(2, 4096);
        let mut stats = TranscodeStats::default();
        let mut pf = PolicyFields::default();
        let mut out = Vec::new();

        transcode(&[0x82, 0x84, 0x81], &mut mirror, None, &mut out, Some(&mut pf), &mut stats).unwrap();
        assert_eq!(pf.method(), Some(&b"GET"[..]));
        assert_eq!(pf.path(), Some(&b"/"[..]));
        assert_eq!(pf.authority(), Some(&b""[..]));
        assert_eq!(pf.have, 7);

        let blk: &[u8] = &[0x40, 0x05, b':', b'p', b'a', b't', b'h', 0x03, b'/', b'a', b'b'];
        transcode(blk, &mut mirror, None, &mut out, Some(&mut pf), &mut stats).unwrap();
        assert_eq!(pf.have, Kind::Path.bit());
        assert_eq!(pf.path(), Some(&b"/ab"[..]));
        assert_eq!(mirror.get(62).unwrap().kind(), Kind::Path);
        transcode(&[0xbe], &mut mirror, None, &mut out, Some(&mut pf), &mut stats).unwrap();
        assert_eq!(pf.path(), Some(&b"/ab"[..]));
        assert_eq!(pf.method(), None);

        let mut enc_val = BytesMut::new();
        huffman::encode(b"example.com:443", &mut enc_val);
        let mut blk = vec![0x11, 0x80 | enc_val.len() as u8];
        blk.extend_from_slice(&enc_val);
        transcode(&blk, &mut mirror, None, &mut out, Some(&mut pf), &mut stats).unwrap();
        assert_eq!(pf.authority(), Some(&b"example.com:443"[..]));
        assert_eq!(mirror.len(), 1, "never-indexed does not insert");

        transcode(&[0x0f, 0x2b, 0x01, b'x'], &mut mirror, None, &mut out, Some(&mut pf), &mut stats).unwrap();
        assert_eq!(pf.have, 0);

        transcode(&[0x0f, 0x2f, 0x02, b'/', b'z'], &mut mirror, None, &mut out, Some(&mut pf), &mut stats).unwrap();
        assert_eq!(pf.path(), Some(&b"/z"[..]));
        assert_eq!(pf.have, Kind::Path.bit());
    }

    /// Literal with a dynamic *name* index whose name is a literal (the
    /// `uber-trace-id` case) and a Huffman-decoded needed value, both
    /// inserted: the rep must carry the mirror name bytes even though our own
    /// insert shifts the referenced entry.
    #[test]
    fn dynamic_name_ref_and_huffman_needed_value() {
        use crate::hpack::selective::NeededSet;
        let mut client = Encoder::new(4096, 0);
        let mut backend = Decoder::new(4096);
        let mut mirror = MirrorTable::new(2, 4096);
        mirror.set_needed(NeededSet::new([HeaderName::from_static("x-trace")]));
        let mut enc = EncoderTable::new(3, 4096);
        let mut stats = TranscodeStats::default();
        let mut buf = RepBuf::default();
        for r in 0..5 {
            let v = format!("trace-{}", r);
            let hdrs = vec![field("x-trace", v.as_bytes(), false), field("x-other", b"same", false)];
            let blk = encode(&mut client, hdrs);
            let mut out = BytesMut::new();
            transcode_with(&mut buf, &blk, &mut mirror, Some(&mut enc), &mut out, None, &mut stats).unwrap();
            let rep = &buf.reps[0];
            assert!(rep.has_dec);
            assert_eq!(rep.dec(&buf.bytes), Some(v.as_bytes()));
            assert_eq!(
                decode_pairs(&mut backend, &out).unwrap(),
                vec![(b"x-trace".to_vec(), v.into_bytes()), (b"x-other".to_vec(), b"same".to_vec())]
            );
        }
        assert_eq!(stats.dyn_miss, 0);
    }

    /// A queued encoder resize is signalled at the start of the next block
    /// and accepted by the peer decoder; the table shrinks accordingly.
    #[test]
    fn encoder_resize_emits_size_update() {
        let mut client = Encoder::new(4096, 0);
        let mut backend = Decoder::new(4096);
        let mut mirror = MirrorTable::new(2, 4096);
        let mut enc = EncoderTable::new(3, 4096);
        let mut stats = TranscodeStats::default();

        for r in 0..3 {
            let (hdrs, expect) = request(r, "srv-search:8082");
            let blk = encode(&mut client, hdrs);
            let mut out = BytesMut::new();
            if r == 1 {
                enc.resize(64);
                enc.resize(1024);
            }
            transcode(&blk, &mut mirror, Some(&mut enc), &mut out, None, &mut stats).unwrap();
            if r == 1 {
                assert_eq!(&out[..5], &[0x20 | 0x1f, 33, 0x3f, 0xe1, 0x07]);
                assert_eq!(enc.max(), 1024);
                assert_eq!(stats.dyn_miss, 4);
                assert_eq!(enc.len(), 5);
            }
            if r == 2 {
                assert_eq!(stats.dyn_miss, 4, "no further misses");
            }
            assert_eq!(decode_pairs(&mut backend, &out).unwrap(), expect, "r={}", r);
        }
        assert_eq!(stats.size_updates, 0, "counts received updates only");
    }

    #[test]
    fn table_index_of_and_eviction() {
        use crate::hpack::mirror::Table;
        let mut t = Table::new(1, 100); // fits two 50-byte entries, not three
        fn mk(v: &[u8]) -> Insert<'_> {
            Insert::plain(31, &[], false, 12, v, false, v.len())
        }
        let (a0, s0) = t.insert(mk(b"aaaaaa")); // 12+6+32 = 50
        let (a1, s1) = t.insert(mk(b"bbbbbb"));
        assert!(s0 && s1);
        assert_eq!(t.index_of(a0), Some(63));
        assert_eq!(t.index_of(a1), Some(62));
        let (a2, s2) = t.insert(mk(b"cccccc"));
        assert!(s2);
        assert_eq!(t.index_of(a0), None, "evicted");
        assert_eq!(t.index_of(a1), Some(63));
        assert_eq!(t.index_of(a2), Some(62));
        assert_eq!(t.get(63).unwrap().val_enc(), b"bbbbbb");
        assert_eq!(t.get(62).unwrap().val_enc(), b"cccccc");
        assert_eq!(t.index_of(99), None);
        assert!(t.get(61).is_none());
        assert!(t.get(64).is_none());
        let big = vec![b'x'; 200];
        let (a3, s3) = t.insert(mk(&big));
        assert!(!s3);
        assert!(t.is_empty());
        assert_eq!(t.size(), 0);
        assert_eq!(t.index_of(a3), None);
        assert_eq!(t.next_abs(), 4);
        assert_eq!(t.arena_len(), 0, "arena reclaimed when empty");
    }

    /// The arena stays bounded under churn and entries keep their bytes.
    #[test]
    fn arena_reclaims_and_preserves_bytes() {
        use crate::hpack::mirror::Table;
        let mut t = Table::new(1, 4096);
        for k in 0..5000u32 {
            let v = format!("value-{:06}", k);
            t.insert(Insert::plain(0, b"x-name", false, 6, v.as_bytes(), false, v.len()));
            // newest must read back
            assert_eq!(t.get(62).unwrap().val_enc(), v.as_bytes());
            assert_eq!(t.get(62).unwrap().name_enc(), b"x-name");
            assert!(t.arena_len() <= 3 * 4096, "arena {}", t.arena_len());
        }
        // oldest live entry reads back too
        let n = t.len() as u32;
        let oldest = t.get(62 + n - 1).unwrap();
        assert_eq!(oldest.val_enc(), format!("value-{:06}", 5000 - n).as_bytes());
    }

    /// Steady state allocates nothing: after warm-up, walking + emitting a
    /// block through `transcode_with` (pooled `RepBuf`, arena tables) does
    /// not touch the allocator. Counted per thread with a wrapping global
    /// allocator (test binary only).
    #[test]
    fn no_alloc_per_block_after_warmup() {
        let mut client = Encoder::new(4096, 0);
        let mut mirror = MirrorTable::new(2, 4096);
        let mut enc = EncoderTable::new(3, 4096);
        let mut stats = TranscodeStats::default();
        let mut buf = RepBuf::with_capacity(32, 4096);
        let mut out = BytesMut::with_capacity(4096);
        let mut total = 0u64;
        // nginx-like: every block inserts fresh entries (churn) plus refs
        for r in 0..600 {
            let v = format!("Wed, 10 Sep 2026 00:00:{:02} GMT", r % 60);
            let hdrs = vec![
                Header::Status(http::StatusCode::OK),
                field("server", b"nginx/1.24.0", false),
                field("date", v.as_bytes(), false),
                field("content-type", b"text/plain", false),
                field("content-length", b"3", false),
                field("x-churn", format!("c{}", r).as_bytes(), false),
            ];
            let blk = encode(&mut client, hdrs);
            out.clear();
            alloc_count::reset();
            transcode_with(&mut buf, &blk, &mut mirror, Some(&mut enc), &mut out, None, &mut stats).unwrap();
            let n = alloc_count::get();
            if r >= 300 {
                total += n;
            }
        }
        assert_eq!(total, 0, "allocations in 300 steady-state blocks: {}", total);
    }

    /// Provenance cache: hit → evict → miss → re-insert → hit again.
    #[test]
    fn provenance_cache_tracks_eviction() {
        use crate::hpack::mirror::Table;
        let mut t = Table::new(5, 100);
        fn mk(v: &[u8], serial: u64) -> Insert<'_> {
            let mut i = Insert::plain(31, &[], false, 12, v, false, v.len());
            i.src_tid = 9;
            i.src_serial = serial;
            i
        }
        t.insert(mk(b"aaaaaa", 1));
        assert_eq!(t.lookup_provenance(9, 1), Some(62));
        t.insert(mk(b"bbbbbb", 2));
        assert_eq!(t.lookup_provenance(9, 1), Some(63));
        t.insert(mk(b"cccccc", 3)); // evicts serial 1
        assert_eq!(t.lookup_provenance(9, 1), None);
        assert_eq!(t.lookup_provenance(9, 2), Some(63));
        assert_eq!(t.lookup_provenance(8, 2), None, "other mirror");
        t.insert(mk(b"aaaaaa", 1)); // re-insert, evicts serial 2
        assert_eq!(t.lookup_provenance(9, 1), Some(62));
        assert_eq!(t.lookup_provenance(9, 2), None);
    }

    mod alloc_count {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::cell::Cell;

        thread_local! {
            static COUNT: Cell<u64> = const { Cell::new(0) };
        }

        pub struct Counting;

        // SAFETY: delegates to `System`; the thread-local counter never
        // allocates (const-initialized `Cell`).
        unsafe impl GlobalAlloc for Counting {
            unsafe fn alloc(&self, l: Layout) -> *mut u8 {
                let _ = COUNT.try_with(|c| c.set(c.get() + 1));
                System.alloc(l)
            }
            unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
                System.dealloc(p, l)
            }
            unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
                let _ = COUNT.try_with(|c| c.set(c.get() + 1));
                System.realloc(p, l, n)
            }
        }

        #[global_allocator]
        static GLOBAL: Counting = Counting;

        pub fn reset() {
            COUNT.with(|c| c.set(0));
        }

        pub fn get() -> u64 {
            COUNT.with(|c| c.get())
        }
    }
}
