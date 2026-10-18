// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Experiment: alternative patch (exception) layouts for FastLanes bit-packing.
//!
//! Every dataset is compressed with the default BtrBlocks compressor, and every array that the
//! compressor chose to bit-pack is harvested (decoded back to its unsigned input, nulls zeroed).
//! Each harvested array is then re-encoded with several layouts and measured for size, encode
//! time, full decode time and random access time. Every layout is checked to round-trip.
//!
//! Layouts:
//!
//! - `vortex-v1` / `vortex-v2`: the real `BitPacked` encoding (global / per-block width), with
//!   `compress_patches` narrowing, decoded with `execute` and accessed with `take`.
//! - `legacy`: a mirror of `vortex-v1` (global u32/u16 indices, full-width values, chunk offsets,
//!   scatter after unpacking) implemented with the same raw kernels as the new layouts.
//! - `u16-full`: per-1024 chunk offsets + u16 in-chunk index + full value (overwrite).
//! - `u16-high`: u16 index + only the bits above the packed width (`v >> w`) in the narrowest
//!   byte type, applied with `out |= high << w`.
//! - `u16-highbp`: as `u16-high`, but the high parts are bit-packed at exactly `max_bits - w`.
//! - `skip-highbp`: per chunk, positions are a u16 list or, once more than 64 exceptions land in
//!   the chunk, a 1024-bit skip set (bitmap); high parts bit-packed.
//! - `blk-legacy`: per-block width with the legacy cost model and legacy patches (mirror of v2).
//! - `blk-skip-highbp`: per-block width chosen with the new cost model, skip-set positions and
//!   per-block bit-packed high parts.
//!
//! Run with `cargo run --release -p vortex-bench --example bitpack_patches -- <datasets...>`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines, clippy::print_stdout, clippy::print_stderr)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::hint::black_box;
use std::time::Duration;
use std::time::Instant;

use fastlanes::BitPacking;
use num_traits::PrimInt;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use vortex::array::ArrayRef;
use vortex::array::ExecutionCtx;
use vortex::array::IntoArray;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::Chunked;
use vortex::array::arrays::ChunkedArray;
use vortex::array::arrays::PrimitiveArray;
use vortex::array::arrays::StructArray;
use vortex::array::arrays::chunked::ChunkedArrayExt;
use vortex::array::arrays::primitive::PrimitiveArrayExt;
use vortex::array::arrays::struct_::StructArrayExt;
use vortex::array::validity::Validity;
use vortex::buffer::Buffer;
use vortex::compressor::BtrBlocksCompressorBuilder;
use vortex::dtype::DType;
use vortex::dtype::NativePType;
use vortex::dtype::PType;
use vortex::encodings::fastlanes::BitPacked;
use vortex::encodings::fastlanes::bitpack_compress::bitpack_blocked_to_best_bit_widths;
use vortex::encodings::fastlanes::bitpack_compress::bitpack_to_best_bit_width;
use vortex_bench::SESSION;
use vortex_bench::datasets::Dataset;
use vortex_bench::datasets::struct_list_of_ints::StructListOfInts;
use vortex_bench::datasets::taxi_data::TaxiData;
use vortex_bench::datasets::tpch_l_comment::TPCHLCommentChunked;
use vortex_bench::downloadable_dataset::DownloadableDataset;
use vortex_bench::public_bi::PBI_DATASETS;
use vortex_bench::public_bi::PBIDataset;

const N: usize = 1024;
/// Rows compressed together, roughly the size of a Vortex file's column chunk.
const ROWS_PER_CHUNK: usize = 128 * 1024;
const NUM_RANDOM: usize = 4096;

// ------------------------------------------------------------------------------------------------
// Integer abstraction
// ------------------------------------------------------------------------------------------------

pub trait Int: BitPacking + PrimInt + NativePType + Default + Debug + Send + Sync + 'static {
    const BITS: usize;
    fn tow(self) -> u64;
    fn fromw(v: u64) -> Self;
    #[inline(always)]
    fn width(self) -> usize {
        Self::BITS - self.leading_zeros() as usize
    }
}

macro_rules! impl_int {
    ($($t:ty),*) => {$(
        impl Int for $t {
            const BITS: usize = <$t>::BITS as usize;
            #[inline(always)]
            fn tow(self) -> u64 { self as u64 }
            #[inline(always)]
            fn fromw(v: u64) -> Self { v as $t }
        }
    )*};
}
impl_int!(u8, u16, u32, u64);

/// Narrowest byte width (1, 2, 4, 8) holding `bits` bits.
fn bytes_for_bits(bits: usize) -> usize {
    match bits {
        0..=8 => 1,
        9..=16 => 2,
        17..=32 => 4,
        _ => 8,
    }
}

fn bytes_for_value(max: u64) -> usize {
    bytes_for_bits(64 - max.leading_zeros() as usize)
}

#[inline(always)]
fn packed_len<T: Int>(w: usize) -> usize {
    N * w / T::BITS
}

// ------------------------------------------------------------------------------------------------
// Storage helpers
// ------------------------------------------------------------------------------------------------

/// High parts of exceptions.
enum HighStore {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
    /// Bit-packed at a fixed width over the whole array.
    Bp(BitVec),
    /// Bit-packed with a width per block, `starts[b]` is the bit offset of block `b`.
    BpBlocked { bits: BitVec, widths: Vec<u8>, starts: Vec<u64> },
}

impl HighStore {
    fn nbytes(&self) -> usize {
        match self {
            HighStore::U8(v) => v.len(),
            HighStore::U16(v) => v.len() * 2,
            HighStore::U32(v) => v.len() * 4,
            HighStore::U64(v) => v.len() * 8,
            HighStore::Bp(b) => b.nbytes(),
            // One width byte per block, the bit starts are derivable from widths and counts.
            HighStore::BpBlocked { bits, widths, .. } => bits.nbytes() + widths.len(),
        }
    }
}

/// A simple LSB-first bit vector with one word of padding so reads never go out of bounds.
struct BitVec {
    words: Vec<u64>,
    len_bits: u64,
    width: u32,
}

impl BitVec {
    fn new(width: u32) -> Self {
        Self { words: vec![0], len_bits: 0, width }
    }

    fn push_bits(&mut self, v: u64, width: u32) {
        if width == 0 {
            return;
        }
        let off = (self.len_bits % 64) as u32;
        let word = (self.len_bits / 64) as usize;
        while self.words.len() < word + 2 {
            self.words.push(0);
        }
        self.words[word] |= v << off;
        if off + width > 64 {
            self.words[word + 1] |= v >> (64 - off);
        }
        self.len_bits += u64::from(width);
    }

    fn push(&mut self, v: u64) {
        self.push_bits(v, self.width);
    }

    #[inline(always)]
    fn get_at(&self, bit: u64, width: u32) -> u64 {
        if width == 0 {
            return 0;
        }
        let word = (bit / 64) as usize;
        let off = (bit % 64) as u32;
        // SAFETY: there is always one word of padding past the last written bit.
        let (lo, hi) = unsafe {
            (
                *self.words.get_unchecked(word),
                *self.words.get_unchecked(word + 1),
            )
        };
        let v = if off == 0 {
            lo
        } else {
            (lo >> off) | (hi << (64 - off))
        };
        if width == 64 {
            v
        } else {
            v & ((1u64 << width) - 1)
        }
    }

    #[inline(always)]
    fn get(&self, k: usize) -> u64 {
        self.get_at(k as u64 * u64::from(self.width), self.width)
    }

    fn nbytes(&self) -> usize {
        self.len_bits.div_ceil(8) as usize
    }
}

enum Positions {
    /// Absolute u32 index into the array (sorted).
    Global(Vec<u32>, usize),
    /// In-chunk u16 index.
    Local(Vec<u16>),
    /// u16 list for sparse chunks, a 1024-bit set for chunks with more than 64 exceptions.
    Skip {
        sparse: Vec<u16>,
        bitmaps: Vec<u64>,
        /// Per chunk start into `sparse` (in u16) or `bitmaps` (in words); derivable on read.
        starts: Vec<u32>,
    },
}

const DENSE_THRESHOLD: usize = 64;

impl Positions {
    fn nbytes(&self) -> usize {
        match self {
            Positions::Global(v, w) => v.len() * w,
            Positions::Local(v) => v.len() * 2,
            Positions::Skip {
                sparse, bitmaps, ..
            } => sparse.len() * 2 + bitmaps.len() * 8,
        }
    }
}

enum Values<T> {
    /// Full value, overwrites the packed value.
    Full(Vec<T>),
    /// Bits above the block width, OR-ed in.
    High(HighStore),
}

struct PatchSet<T> {
    /// `nchunks + 1` cumulative counts.
    chunk_off: Vec<u32>,
    pos: Positions,
    values: Values<T>,
    /// Apply all patches after unpacking everything (legacy) instead of chunk by chunk.
    scatter_after: bool,
}

impl<T> PatchSet<T> {
    fn nbytes(&self) -> usize {
        let n = *self.chunk_off.last().unwrap() as u64;
        // Like Vortex, store one offset per chunk, narrowed.
        let off = (self.chunk_off.len() - 1) * bytes_for_value(n);
        let vals = match &self.values {
            Values::Full(v) => v.len() * size_of::<T>(),
            Values::High(h) => h.nbytes(),
        };
        off + self.pos.nbytes() + vals
    }
}

enum Widths {
    Global(usize),
    Blocked {
        widths: Vec<u8>,
        /// Start of each block in `packed`, in units of T; derivable from widths.
        starts: Vec<u32>,
    },
}

struct Encoded<T> {
    len: usize,
    widths: Widths,
    packed: Vec<T>,
    patches: Option<PatchSet<T>>,
}

impl<T: Int> Encoded<T> {
    fn nbytes(&self) -> usize {
        let w = match &self.widths {
            Widths::Global(_) => 0,
            Widths::Blocked { widths, .. } => widths.len(),
        };
        self.packed.len() * size_of::<T>() + w + self.patches.as_ref().map_or(0, |p| p.nbytes())
    }

    #[inline(always)]
    fn block(&self, b: usize) -> (usize, &[T]) {
        match &self.widths {
            Widths::Global(w) => {
                let l = packed_len::<T>(*w);
                (*w, &self.packed[b * l..][..l])
            }
            Widths::Blocked { widths, starts } => {
                let w = widths[b] as usize;
                (w, &self.packed[starts[b] as usize..][..packed_len::<T>(w)])
            }
        }
    }

    /// Decode into `out`, which must be `len` rounded up to a multiple of 1024.
    fn decode(&self, out: &mut [T]) {
        let nchunks = self.len.div_ceil(N);
        debug_assert_eq!(out.len(), nchunks * N);
        let chunk_patches = self.patches.as_ref().filter(|p| !p.scatter_after);
        let mut sparse_cursor = 0usize;
        let mut bitmap_cursor = 0usize;
        for b in 0..nchunks {
            let (w, packed) = self.block(b);
            let dst = &mut out[b * N..][..N];
            if w == 0 {
                dst.fill(T::zero());
            } else {
                unsafe { BitPacking::unchecked_unpack(w, packed, dst) };
            }
            if let Some(p) = chunk_patches {
                let lo = p.chunk_off[b] as usize;
                let hi = p.chunk_off[b + 1] as usize;
                if lo == hi {
                    continue;
                }
                apply_chunk(p, dst, w, b, lo, hi, &mut sparse_cursor, &mut bitmap_cursor);
            }
        }
        if let Some(p) = self.patches.as_ref().filter(|p| p.scatter_after) {
            let (Positions::Global(idx, _), Values::Full(vals)) = (&p.pos, &p.values) else {
                unreachable!()
            };
            for (&i, &v) in idx.iter().zip(vals) {
                unsafe { *out.get_unchecked_mut(i as usize) = v };
            }
        }
    }

    fn get(&self, i: usize) -> T {
        let b = i / N;
        let j = i % N;
        let (w, packed) = self.block(b);
        let low = if w == 0 {
            T::zero()
        } else {
            unsafe { BitPacking::unchecked_unpack_single(w, packed, j) }
        };
        let Some(p) = &self.patches else {
            return low;
        };
        let lo = p.chunk_off[b] as usize;
        let hi = p.chunk_off[b + 1] as usize;
        if lo == hi {
            return low;
        }
        let k = match &p.pos {
            Positions::Global(idx, _) => idx[lo..hi].binary_search(&(i as u32)).ok().map(|k| lo + k),
            Positions::Local(idx) => idx[lo..hi].binary_search(&(j as u16)).ok().map(|k| lo + k),
            Positions::Skip {
                sparse,
                bitmaps,
                starts,
            } => {
                let start = starts[b] as usize;
                if hi - lo > DENSE_THRESHOLD {
                    let bm = &bitmaps[start..][..N / 64];
                    let word = bm[j / 64];
                    if word >> (j % 64) & 1 == 0 {
                        None
                    } else {
                        let rank: u32 = bm[..j / 64].iter().map(|w| w.count_ones()).sum::<u32>()
                            + (word & ((1u64 << (j % 64)) - 1)).count_ones();
                        Some(lo + rank as usize)
                    }
                } else {
                    sparse[start..][..hi - lo]
                        .binary_search(&(j as u16))
                        .ok()
                        .map(|k| lo + k)
                }
            }
        };
        let Some(k) = k else {
            return low;
        };
        match &p.values {
            Values::Full(v) => v[k],
            Values::High(h) => low | (T::fromw(high_get(h, k, b, lo)) << w),
        }
    }
}

#[inline(always)]
fn high_get(h: &HighStore, k: usize, b: usize, block_lo: usize) -> u64 {
    match h {
        HighStore::U8(v) => u64::from(v[k]),
        HighStore::U16(v) => u64::from(v[k]),
        HighStore::U32(v) => u64::from(v[k]),
        HighStore::U64(v) => v[k],
        HighStore::Bp(bits) => bits.get(k),
        HighStore::BpBlocked {
            bits,
            widths,
            starts,
        } => {
            let w = u32::from(widths[b]);
            bits.get_at(starts[b] + (k - block_lo) as u64 * u64::from(w), w)
        }
    }
}

/// Apply the patches `lo..hi` of chunk `b` (packed at width `w`) to `dst`.
#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn apply_chunk<T: Int>(
    p: &PatchSet<T>,
    dst: &mut [T],
    w: usize,
    b: usize,
    lo: usize,
    hi: usize,
    sparse_cursor: &mut usize,
    bitmap_cursor: &mut usize,
) {
    // Visit (in-chunk position, patch ordinal) pairs and apply `f`.
    macro_rules! for_each_pos {
        (|$j:ident, $k:ident| $body:expr) => {
            match &p.pos {
                Positions::Global(idx, _) => {
                    let base = (b * N) as u32;
                    for $k in lo..hi {
                        let $j = (unsafe { *idx.get_unchecked($k) } - base) as usize;
                        $body;
                    }
                }
                Positions::Local(idx) => {
                    for $k in lo..hi {
                        let $j = unsafe { *idx.get_unchecked($k) } as usize;
                        $body;
                    }
                }
                Positions::Skip {
                    sparse, bitmaps, ..
                } => {
                    if hi - lo > DENSE_THRESHOLD {
                        let bm = &bitmaps[*bitmap_cursor..][..N / 64];
                        *bitmap_cursor += N / 64;
                        let mut $k = lo;
                        for (wi, &word) in bm.iter().enumerate() {
                            let mut word = word;
                            while word != 0 {
                                let $j = wi * 64 + word.trailing_zeros() as usize;
                                word &= word - 1;
                                $body;
                                $k += 1;
                            }
                        }
                    } else {
                        let s = &sparse[*sparse_cursor..][..hi - lo];
                        *sparse_cursor += hi - lo;
                        for (o, &j) in s.iter().enumerate() {
                            let $j = j as usize;
                            let $k = lo + o;
                            $body;
                        }
                    }
                }
            }
        };
    }

    match &p.values {
        Values::Full(v) => for_each_pos!(|j, k| unsafe {
            *dst.get_unchecked_mut(j) = *v.get_unchecked(k)
        }),
        Values::High(h) => match h {
            HighStore::U8(v) => for_each_pos!(|j, k| unsafe {
                *dst.get_unchecked_mut(j) =
                    *dst.get_unchecked(j) | (T::fromw(u64::from(*v.get_unchecked(k))) << w)
            }),
            HighStore::U16(v) => for_each_pos!(|j, k| unsafe {
                *dst.get_unchecked_mut(j) =
                    *dst.get_unchecked(j) | (T::fromw(u64::from(*v.get_unchecked(k))) << w)
            }),
            HighStore::U32(v) => for_each_pos!(|j, k| unsafe {
                *dst.get_unchecked_mut(j) =
                    *dst.get_unchecked(j) | (T::fromw(u64::from(*v.get_unchecked(k))) << w)
            }),
            HighStore::U64(v) => for_each_pos!(|j, k| unsafe {
                *dst.get_unchecked_mut(j) =
                    *dst.get_unchecked(j) | (T::fromw(*v.get_unchecked(k)) << w)
            }),
            HighStore::Bp(bits) => for_each_pos!(|j, k| unsafe {
                *dst.get_unchecked_mut(j) = *dst.get_unchecked(j) | (T::fromw(bits.get(k)) << w)
            }),
            HighStore::BpBlocked {
                bits,
                widths,
                starts,
            } => {
                let hw = u32::from(widths[b]);
                let base = starts[b];
                for_each_pos!(|j, k| unsafe {
                    *dst.get_unchecked_mut(j) = *dst.get_unchecked(j)
                        | (T::fromw(bits.get_at(base + (k - lo) as u64 * u64::from(hw), hw))
                            << w)
                })
            }
        },
    }
}

// ------------------------------------------------------------------------------------------------
// Encoders
// ------------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Layout {
    Legacy,
    U16Full,
    U16High,
    U16HighBp,
    SkipHighBp,
    BlkLegacy,
    BlkU16HighBp,
    BlkSkipHighBp,
}


/// A layout plus an extra cost, in bits, charged per exception when choosing widths. The penalty
/// trades size for decode speed by discouraging widths that leave many exceptions.
#[derive(Clone, Copy, Debug)]
struct Cfg {
    layout: Layout,
    penalty: usize,
    name: &'static str,
}

const fn cfg(layout: Layout, penalty: usize, name: &'static str) -> Cfg {
    Cfg {
        layout,
        penalty,
        name,
    }
}

const CONFIGS: &[Cfg] = &[
    cfg(Layout::Legacy, 0, "legacy"),
    cfg(Layout::U16Full, 0, "u16-full"),
    cfg(Layout::U16High, 0, "u16-high"),
    cfg(Layout::U16HighBp, 0, "u16-highbp"),
    cfg(Layout::SkipHighBp, 0, "skip-highbp"),
    cfg(Layout::SkipHighBp, 32, "skip-highbp+p32"),
    cfg(Layout::BlkLegacy, 0, "blk-legacy"),
    cfg(Layout::BlkU16HighBp, 0, "blk-u16-highbp"),
    cfg(Layout::BlkU16HighBp, 32, "blk-u16-highbp+p32"),
    cfg(Layout::BlkSkipHighBp, 0, "blk-skip-highbp"),
    cfg(Layout::BlkSkipHighBp, 16, "blk-skip-highbp+p16"),
    cfg(Layout::BlkSkipHighBp, 32, "blk-skip-highbp+p32"),
    cfg(Layout::BlkSkipHighBp, 64, "blk-skip-highbp+p64"),
];

/// Per-block width histograms (`BITS + 1` entries per block), padding counted as zero width.
fn histograms<T: Int>(v: &[T]) -> Vec<Vec<u32>> {
    v.chunks(N)
        .map(|c| {
            let mut h = vec![0u32; T::BITS + 1];
            h[0] = (N - c.len()) as u32;
            for &x in c {
                h[x.width()] += 1;
            }
            h
        })
        .collect()
}

/// Number of values wider than `w` in histogram `h`.
fn exceptions(h: &[u32], w: usize) -> usize {
    h[w + 1..].iter().map(|&c| c as usize).sum()
}

fn max_width(h: &[u32]) -> usize {
    h.iter().rposition(|&c| c > 0).unwrap_or(0)
}

/// Vortex's cost model: exceptions cost a full value plus a 4 byte index.
fn legacy_width<T: Int>(h: &[u32]) -> usize {
    let len: usize = h.iter().map(|&c| c as usize).sum();
    let bpe = size_of::<T>() + 4;
    let mut num_packed = 0;
    let mut best_cost = len * bpe;
    let mut best = 0;
    for (w, &f) in h.iter().enumerate() {
        let packed_cost = (w * len).div_ceil(8);
        num_packed += f as usize;
        let cost = (len - num_packed) * bpe + packed_cost;
        if cost < best_cost {
            best_cost = cost;
            best = w;
        }
    }
    best
}

fn sum_hist(hs: &[Vec<u32>], bits: usize) -> Vec<u32> {
    let mut total = vec![0u32; bits + 1];
    for h in hs {
        for (t, c) in total.iter_mut().zip(h) {
            *t += c;
        }
    }
    total
}

/// Exact byte cost of a global width `w` for the new layouts.
fn global_cost<T: Int>(
    layout: Layout,
    penalty: usize,
    hs: &[Vec<u32>],
    total: &[u32],
    w: usize,
) -> usize {
    let nchunks = hs.len();
    let packed = nchunks * 128 * w;
    let e = exceptions(total, w);
    if e == 0 {
        return 8 * packed;
    }
    let hb = max_width(total) - w;
    let pos = match layout {
        Layout::SkipHighBp => hs
            .iter()
            .map(|h| (2 * exceptions(h, w)).min(128))
            .sum::<usize>(),
        _ => 2 * e,
    };
    let vals = match layout {
        Layout::U16Full => e * size_of::<T>(),
        Layout::U16High => e * bytes_for_bits(hb),
        Layout::U16HighBp | Layout::SkipHighBp => (e * hb).div_ceil(8),
        _ => unreachable!(),
    };
    8 * (packed + pos + vals + nchunks * bytes_for_value(e as u64)) + penalty * e
}

/// Best width for block `h` under the blocked bit-packed high cost model, in bits.
fn blocked_new_width<T: Int>(h: &[u32], skip_set: bool, penalty: usize) -> usize {
    let m = max_width(h);
    (0..=m.min(T::BITS))
        .min_by_key(|&w| {
            let e = exceptions(h, w);
            let hb = m - w;
            let pos = if skip_set { (16 * e).min(1024) } else { 16 * e };
            1024 * w + pos + e * hb + penalty * e
        })
        .unwrap()
}

fn encode<T: Int>(v: &[T], cfg: Cfg) -> Encoded<T> {
    let layout = cfg.layout;
    let nchunks = v.len().div_ceil(N);
    let hs = histograms(v);

    // 1. Widths.
    let block_widths: Vec<usize> = match layout {
        Layout::BlkLegacy => hs.iter().map(|h| legacy_width::<T>(h)).collect(),
        Layout::BlkSkipHighBp | Layout::BlkU16HighBp => hs
            .iter()
            .map(|h| blocked_new_width::<T>(h, layout == Layout::BlkSkipHighBp, cfg.penalty))
            .collect(),
        _ => {
            let total = sum_hist(&hs, T::BITS);
            let w = match layout {
                Layout::Legacy => legacy_width::<T>(&total),
                _ => (0..=T::BITS)
                    .min_by_key(|&w| global_cost::<T>(layout, cfg.penalty, &hs, &total, w))
                    .unwrap(),
            };
            vec![w; nchunks]
        }
    };
    let blocked = matches!(
        layout,
        Layout::BlkLegacy | Layout::BlkU16HighBp | Layout::BlkSkipHighBp
    );
    let blocked_high = matches!(layout, Layout::BlkU16HighBp | Layout::BlkSkipHighBp);

    // 2. Pack.
    let total_packed: usize = block_widths.iter().map(|&w| packed_len::<T>(w)).sum();
    let mut packed = vec![T::zero(); total_packed];
    let mut starts = Vec::with_capacity(nchunks);
    let mut cursor = 0;
    let mut pad = [T::zero(); N];
    for (b, chunk) in v.chunks(N).enumerate() {
        let w = block_widths[b];
        let l = packed_len::<T>(w);
        starts.push(cursor as u32);
        if w > 0 {
            let input: &[T] = if chunk.len() == N {
                chunk
            } else {
                pad.fill(T::zero());
                pad[..chunk.len()].copy_from_slice(chunk);
                &pad
            };
            unsafe { BitPacking::unchecked_pack(w, input, &mut packed[cursor..][..l]) };
        }
        cursor += l;
    }

    // 3. Patches.
    let mut chunk_off = Vec::with_capacity(nchunks + 1);
    let mut gidx = Vec::new();
    let mut lidx: Vec<u16> = Vec::new();
    let mut full = Vec::new();
    let mut highs: Vec<u64> = Vec::new();
    let mut sparse = Vec::new();
    let mut bitmaps = Vec::new();
    let mut pos_starts = Vec::with_capacity(nchunks);
    let mut hbits = BitVec::new(0);
    let mut hwidths = Vec::new();
    let mut hstarts = Vec::new();
    let global_hb = {
        let total = sum_hist(&hs, T::BITS);
        max_width(&total).saturating_sub(block_widths.first().copied().unwrap_or(0)) as u32
    };
    if !blocked {
        hbits = BitVec::new(global_hb);
    }
    chunk_off.push(0u32);
    for (b, chunk) in v.chunks(N).enumerate() {
        let w = block_widths[b];
        let lo = full.len().max(highs.len());
        let hw = (max_width(&hs[b]).saturating_sub(w)) as u32;
        if blocked_high {
            hwidths.push(hw as u8);
            hstarts.push(hbits.len_bits);
        }
        let mut bm = [0u64; N / 64];
        for (j, &x) in chunk.iter().enumerate() {
            if x.width() > w {
                match layout {
                    Layout::Legacy | Layout::BlkLegacy => {
                        gidx.push((b * N + j) as u32);
                        full.push(x);
                    }
                    Layout::U16Full => {
                        lidx.push(j as u16);
                        full.push(x);
                    }
                    _ => {
                        lidx.push(j as u16);
                        bm[j / 64] |= 1 << (j % 64);
                        let h = (x >> w).tow();
                        highs.push(h);
                        if blocked_high {
                            hbits.push_bits(h, hw);
                        } else {
                            hbits.push(h);
                        }
                    }
                }
            }
        }
        let count = full.len().max(highs.len()) - lo;
        if matches!(layout, Layout::SkipHighBp | Layout::BlkSkipHighBp) {
            if count > DENSE_THRESHOLD {
                pos_starts.push(bitmaps.len() as u32);
                bitmaps.extend_from_slice(&bm);
            } else {
                pos_starts.push(sparse.len() as u32);
                sparse.extend_from_slice(&lidx[lidx.len() - count..]);
            }
        }
        chunk_off.push((lo + count) as u32);
    }

    let num_patches = *chunk_off.last().unwrap() as usize;
    let patches = (num_patches > 0).then(|| {
        let pos = match layout {
            Layout::Legacy | Layout::BlkLegacy => {
                Positions::Global(gidx, bytes_for_value(v.len().saturating_sub(1) as u64))
            }
            Layout::U16Full | Layout::U16High | Layout::U16HighBp | Layout::BlkU16HighBp => {
                Positions::Local(lidx)
            }
            Layout::SkipHighBp | Layout::BlkSkipHighBp => Positions::Skip {
                sparse,
                bitmaps,
                starts: pos_starts,
            },
        };
        let values = match layout {
            Layout::Legacy | Layout::BlkLegacy | Layout::U16Full => Values::Full(full),
            Layout::U16High => Values::High(match bytes_for_bits(global_hb as usize) {
                1 => HighStore::U8(highs.iter().map(|&h| h as u8).collect()),
                2 => HighStore::U16(highs.iter().map(|&h| h as u16).collect()),
                4 => HighStore::U32(highs.iter().map(|&h| h as u32).collect()),
                _ => HighStore::U64(highs),
            }),
            Layout::U16HighBp | Layout::SkipHighBp => Values::High(HighStore::Bp(hbits)),
            Layout::BlkU16HighBp | Layout::BlkSkipHighBp => Values::High(HighStore::BpBlocked {
                bits: hbits,
                widths: hwidths,
                starts: hstarts,
            }),
        };
        PatchSet {
            chunk_off,
            pos,
            values,
            scatter_after: matches!(layout, Layout::Legacy | Layout::BlkLegacy),
        }
    });

    Encoded {
        len: v.len(),
        widths: if blocked {
            Widths::Blocked {
                widths: block_widths.iter().map(|&w| w as u8).collect(),
                starts,
            }
        } else {
            Widths::Global(block_widths.first().copied().unwrap_or(0))
        },
        packed,
        patches,
    }
}

// ------------------------------------------------------------------------------------------------
// Measurement
// ------------------------------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Stats {
    arrays: usize,
    values: usize,
    raw_bytes: usize,
    bytes: usize,
    patches: usize,
    encode: Duration,
    decode: Duration,
    take: Duration,
    takes: usize,
    failures: usize,
}

impl Stats {
    fn add(&mut self, o: &Stats) {
        self.arrays += o.arrays;
        self.values += o.values;
        self.raw_bytes += o.raw_bytes;
        self.bytes += o.bytes;
        self.patches += o.patches;
        self.encode += o.encode;
        self.decode += o.decode;
        self.take += o.take;
        self.takes += o.takes;
        self.failures += o.failures;
    }
}

/// Minimum time over `reps` runs of `f`.
fn time_min(reps: usize, mut f: impl FnMut()) -> Duration {
    (0..reps)
        .map(|_| {
            let t = Instant::now();
            f();
            t.elapsed()
        })
        .min()
        .unwrap()
}

fn reps_for(len: usize) -> usize {
    (4_000_000 / len.max(1)).clamp(3, 50)
}

fn measure_layout<T: Int>(v: &[T], layout: Cfg, idx: &[u32]) -> Stats {
    let reps = reps_for(v.len());
    let mut enc = None;
    let encode = time_min(reps.min(10), || enc = Some(encode(v, layout)));
    let enc = enc.unwrap();
    let mut out = vec![T::zero(); v.len().div_ceil(N) * N];
    let decode = time_min(reps, || enc.decode(black_box(&mut out)));
    let mut failures = usize::from(&out[..v.len()] != v);
    let mut got = vec![T::zero(); idx.len()];
    let take = time_min(reps, || {
        for (g, &i) in got.iter_mut().zip(idx) {
            *g = enc.get(i as usize);
        }
        black_box(&got);
    });
    failures += usize::from(got.iter().zip(idx).any(|(&g, &i)| g != v[i as usize]));
    Stats {
        arrays: 1,
        values: v.len(),
        raw_bytes: size_of_val(v),
        bytes: enc.nbytes(),
        patches: enc
            .patches
            .as_ref()
            .map_or(0, |p| *p.chunk_off.last().unwrap() as usize),
        encode,
        decode,
        take,
        takes: idx.len(),
        failures,
    }
}

fn measure_vortex<T: Int>(v: &[T], blocked: bool, idx: &[u32], ctx: &mut ExecutionCtx) -> Stats {
    let reps = reps_for(v.len());
    let prim = PrimitiveArray::new(Buffer::<T>::copy_from(v), Validity::NonNullable);
    let mut arr = None;
    let encode = time_min(reps.min(10), || {
        let bp = if blocked {
            bitpack_blocked_to_best_bit_widths(&prim, ctx)
        } else {
            bitpack_to_best_bit_width(&prim, ctx)
        };
        arr = Some(bp);
    });
    let Ok(bp) = arr.unwrap() else {
        // Vortex refuses to pack (e.g. every block at native width): store raw.
        return Stats {
            arrays: 1,
            values: v.len(),
            raw_bytes: size_of_val(v),
            bytes: size_of_val(v),
            ..Default::default()
        };
    };
    let mut parts = BitPacked::into_parts(bp.clone());
    let num_patches = parts.patches.as_ref().map_or(0, |p| p.num_patches());
    parts.patches = parts
        .patches
        .take()
        .map(|p| vortex_btrblocks::compress_patches(p, ctx).unwrap());
    let arr: ArrayRef = match parts.bit_widths {
        vortex::encodings::fastlanes::BitWidths::Global(bw) => BitPacked::try_new(
            parts.packed,
            T::PTYPE,
            parts.validity,
            parts.patches,
            bw,
            parts.len,
            parts.offset,
        )
        .unwrap()
        .into_array(),
        vortex::encodings::fastlanes::BitWidths::Blocked(offsets) => {
            BitPacked::try_new_with_block_offsets(
                parts.packed,
                T::PTYPE,
                parts.validity,
                parts.patches,
                offsets,
                parts.len,
                parts.offset,
            )
            .unwrap()
            .into_array()
        }
    };
    let bytes = arr.nbytes() as usize;
    let mut decoded = None;
    let decode = time_min(reps, || {
        decoded = Some(arr.clone().execute::<PrimitiveArray>(ctx).unwrap())
    });
    let mut failures = usize::from(decoded.unwrap().as_slice::<T>() != v);
    let indices = PrimitiveArray::from_iter(idx.iter().copied()).into_array();
    let mut taken = None;
    let take = time_min(reps, || {
        taken = Some(
            arr.take(indices.clone())
                .unwrap()
                .execute::<PrimitiveArray>(ctx)
                .unwrap(),
        )
    });
    let taken = taken.unwrap();
    failures += usize::from(
        taken
            .as_slice::<T>()
            .iter()
            .zip(idx)
            .any(|(&g, &i)| g != v[i as usize]),
    );
    Stats {
        arrays: 1,
        values: v.len(),
        raw_bytes: size_of_val(v),
        bytes,
        patches: num_patches,
        encode,
        decode,
        take,
        takes: idx.len(),
        failures,
    }
}

const VORTEX_V1: &str = "vortex-v1";
const VORTEX_V2: &str = "vortex-v2";

fn names() -> Vec<&'static str> {
    let mut n = vec![VORTEX_V1, VORTEX_V2];
    n.extend(CONFIGS.iter().map(|c| c.name));
    n
}

fn measure_all<T: Int>(v: &[T], ctx: &mut ExecutionCtx) -> BTreeMap<&'static str, Stats> {
    let mut rng = StdRng::seed_from_u64(v.len() as u64);
    let idx: Vec<u32> = (0..NUM_RANDOM)
        .map(|_| rng.random_range(0..v.len() as u32))
        .collect();
    let mut res = BTreeMap::new();
    res.insert(VORTEX_V1, measure_vortex(v, false, &idx, ctx));
    res.insert(VORTEX_V2, measure_vortex(v, true, &idx, ctx));
    for &c in CONFIGS {
        res.insert(c.name, measure_layout(v, c, &idx));
    }
    res
}

// ------------------------------------------------------------------------------------------------
// Harvesting bit-packed inputs from the real compressor
// ------------------------------------------------------------------------------------------------

enum Input {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
}

fn walk(a: &ArrayRef, out: &mut Vec<Input>, ctx: &mut ExecutionCtx) -> anyhow::Result<()> {
    if a.is::<BitPacked>() {
        let prim = a.clone().execute::<PrimitiveArray>(ctx)?;
        let mask = prim.validity()?.execute_mask(prim.len(), ctx)?;
        let prim = prim.reinterpret_cast(prim.ptype().to_unsigned());
        macro_rules! harvest {
            ($t:ty, $variant:ident) => {{
                let mut v = prim.as_slice::<$t>().to_vec();
                if !mask.all_true() {
                    for (x, valid) in v.iter_mut().zip(mask.iter()) {
                        if !valid {
                            *x = 0;
                        }
                    }
                }
                out.push(Input::$variant(v));
            }};
        }
        match prim.ptype() {
            PType::U8 => harvest!(u8, U8),
            PType::U16 => harvest!(u16, U16),
            PType::U32 => harvest!(u32, U32),
            PType::U64 => harvest!(u64, U64),
            p => anyhow::bail!("unexpected ptype {p}"),
        }
        return Ok(());
    }
    for child in a.children() {
        walk(&child, out, ctx)?;
    }
    Ok(())
}

async fn harvest(
    dataset: &dyn Dataset,
    ctx: &mut ExecutionCtx,
    sink: &mut dyn FnMut(Vec<Input>, &mut ExecutionCtx),
) -> anyhow::Result<u64> {
    let mut total_compressed = 0u64;
    let array = dataset.to_vortex_array(ctx).await?;
    let chunks = match array.as_opt::<Chunked>() {
        Some(c) => c.iter_chunks().cloned().collect(),
        None => vec![array.clone()],
    };
    // Split into columns so each is compressed in roughly file-sized chunks.
    let mut columns: Vec<Vec<ArrayRef>> = Vec::new();
    let mut column_dtypes: Vec<DType> = Vec::new();
    for chunk in chunks {
        if chunk.dtype().is_struct() {
            let s = chunk.execute::<StructArray>(ctx)?;
            for (i, f) in s.iter_unmasked_fields().enumerate() {
                if columns.len() <= i {
                    columns.push(Vec::new());
                    column_dtypes.push(f.dtype().clone());
                }
                columns[i].push(f.clone());
            }
        } else {
            if columns.is_empty() {
                columns.push(Vec::new());
                column_dtypes.push(chunk.dtype().clone());
            }
            columns[0].push(chunk);
        }
    }
    let compressor = BtrBlocksCompressorBuilder::from_session(&SESSION).build();
    for (col, dtype) in columns.into_iter().zip(column_dtypes) {
        let mut out = Vec::new();
        let col = ChunkedArray::try_new(col, dtype)?.into_array();
        let len = col.len();
        for start in (0..len).step_by(ROWS_PER_CHUNK) {
            let slice = col.slice(start..(start + ROWS_PER_CHUNK).min(len))?;
            let compressed = compressor.compress(&slice, ctx)?;
            total_compressed += compressed.nbytes();
            walk(&compressed, &mut out, ctx)?;
        }
        sink(out, ctx);
    }
    Ok(total_compressed)
}

// ------------------------------------------------------------------------------------------------
// Main
// ------------------------------------------------------------------------------------------------

fn dataset_by_name(name: &str) -> Option<&'static dyn Dataset> {
    if let Some(rest) = name.strip_prefix("pbi:") {
        let ds = <PBIDataset as std::str::FromStr>::from_str(rest).ok()?;
        return Some(PBI_DATASETS.get(ds));
    }
    Some(match name {
        "taxi" => &TaxiData,
        "rplace" => &DownloadableDataset::RPlace,
        "airquality" => &DownloadableDataset::AirQuality,
        "tpch-l-comment" => &TPCHLCommentChunked,
        "structlistofints" => Box::leak(Box::new(StructListOfInts::new(1000, 1000, 50))),
        _ => return None,
    })
}

fn print_table(title: &str, stats: &BTreeMap<&'static str, Stats>) {
    let base = &stats[VORTEX_V1];
    println!("\n### {title}");
    println!(
        "arrays={} values={} raw={:.1}MB vortex-v1={:.2}MB",
        base.arrays,
        base.values,
        base.raw_bytes as f64 / 1e6,
        base.bytes as f64 / 1e6
    );
    println!(
        "| layout | size MB | vs v1 | ratio | patches | enc MB/s | dec GB/s | take ns/val | fails |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|");
    for name in names() {
        let s = &stats[name];
        println!(
            "| {} | {:.3} | {:+.2}% | {:.2}x | {} | {:.0} | {:.2} | {:.1} | {} |",
            name,
            s.bytes as f64 / 1e6,
            100.0 * (s.bytes as f64 / base.bytes as f64 - 1.0),
            s.raw_bytes as f64 / s.bytes as f64,
            s.patches,
            s.raw_bytes as f64 / 1e6 / s.encode.as_secs_f64(),
            s.raw_bytes as f64 / 1e9 / s.decode.as_secs_f64(),
            s.take.as_nanos() as f64 / s.takes.max(1) as f64,
            s.failures
        );
    }
}

fn main() -> anyhow::Result<()> {
    vortex_bench::configure_allocator();
    let names_arg: Vec<String> = std::env::args().skip(1).collect();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let mut ctx = SESSION.create_execution_ctx();
    let mut overall: BTreeMap<&'static str, Stats> = BTreeMap::new();
    let mut summary = Vec::new();
    for name in &names_arg {
        let Some(dataset) = dataset_by_name(name) else {
            eprintln!("unknown dataset {name}");
            continue;
        };
        let mut per: BTreeMap<&'static str, Stats> = BTreeMap::new();
        let started = Instant::now();
        let mut sink = |inputs: Vec<Input>, ctx: &mut ExecutionCtx| {
            for input in &inputs {
                let r = match input {
                    Input::U8(v) => measure_all(v, ctx),
                    Input::U16(v) => measure_all(v, ctx),
                    Input::U32(v) => measure_all(v, ctx),
                    Input::U64(v) => measure_all(v, ctx),
                };
                for (k, s) in r {
                    per.entry(k).or_default().add(&s);
                }
            }
        };
        match runtime.block_on(harvest(dataset, &mut ctx, &mut sink)) {
            Ok(total) => println!("TOTAL\t{name}\t{total}"),
            Err(e) => {
                eprintln!("{name}: failed: {e:#}");
                continue;
            }
        }
        eprintln!("{name}: done in {:?}", started.elapsed());
        if per.is_empty() {
            println!("\n### {name}: no bit-packed arrays");
            continue;
        }
        print_table(name, &per);
        for (k, s) in &per {
            println!(
                "RAW\t{name}\t{k}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                s.arrays,
                s.values,
                s.raw_bytes,
                s.bytes,
                s.patches,
                s.encode.as_nanos(),
                s.decode.as_nanos(),
                s.take.as_nanos(),
                s.takes,
                s.failures,
                0
            );
        }
        for (k, s) in &per {
            overall.entry(k).or_default().add(s);
        }
        summary.push((name.clone(), per));
    }

    if !overall.is_empty() {
        print_table("ALL", &overall);
        println!("\n### Size vs vortex-v1 per dataset (%)");
        let ns = names();
        println!("| dataset | {} |", ns.join(" | "));
        println!("|---|{}", "---:|".repeat(ns.len()));
        for (name, per) in &summary {
            let base = per[VORTEX_V1].bytes as f64;
            let cells: Vec<String> = ns
                .iter()
                .map(|n| format!("{:+.2}", 100.0 * (per[n].bytes as f64 / base - 1.0)))
                .collect();
            println!("| {name} | {} |", cells.join(" | "));
        }
    }
    Ok(())
}
