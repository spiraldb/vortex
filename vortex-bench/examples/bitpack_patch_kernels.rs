// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Experiment: patch-apply kernels and cost models for blocked FastLanes bit-packing with
//! high-bit patches.
//!
//! Two modes:
//!
//! - `harvest <cache-dir> <dataset>...`: compress each dataset with the default BtrBlocks
//!   compressor and write every array it bit-packs (decoded, nulls zeroed) to
//!   `<cache-dir>/<dataset>.zst`, plus its total compressed size to `<dataset>.total`.
//! - `measure <cache files>...`: re-encode every cached array with each configuration and measure
//!   size, encode, decode (per patch-apply kernel), unpack-only and random access, checking that
//!   everything round-trips.
//!
//! Every configuration uses per-1024-block widths and stores only the bits of an exception above
//! its block's width (`v >> w`), applied with `out |= high << w`. Configurations differ in:
//!
//! - positions: a sorted u16 in-chunk `list`, `lanes` (transposed: grouped by `pos % L` with
//!   per-lane counts, like the `Patched` array), or `adaptive` (per chunk, a list or a 1024-bit
//!   skip set, whichever the cost model prefers),
//! - high parts: `byte` (narrowest byte width per block) or `bp` (bit-packed per block),
//! - the width chooser: `pN` (size + N bits per exception) or `tL` (size + L bits per estimated
//!   nanosecond of decode, from a startup calibration of the kernels below).
//!
//! Kernels: `scalar` (one patch at a time, reading each high part in place), `opt` (widen the
//! chunk's high parts into a scratch buffer, then a tight unrolled OR loop) and `simd` (AVX-512:
//! gather/OR/scatter 16 or 8 list patches at a time, masked expand-load of high parts for skip-set
//! chunks, round-wise masked gather/scatter across lanes for the transposed layout).
//!
//! Run with `cargo run --release -p vortex-bench --example bitpack_patch_kernels -- <mode> ...`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::cast_possible_truncation)]
#![allow(clippy::too_many_lines, clippy::print_stdout, clippy::print_stderr)]
#![allow(unsafe_op_in_unsafe_fn, clippy::missing_safety_doc)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::File;
use std::hint::black_box;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
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

// ------------------------------------------------------------------------------------------------
// Harvest cache
// ------------------------------------------------------------------------------------------------

enum Input {
    U8(Vec<u8>),
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
}

impl Input {
    fn byte_width(&self) -> u8 {
        match self {
            Input::U8(_) => 1,
            Input::U16(_) => 2,
            Input::U32(_) => 4,
            Input::U64(_) => 8,
        }
    }

    fn len(&self) -> usize {
        match self {
            Input::U8(v) => v.len(),
            Input::U16(v) => v.len(),
            Input::U32(v) => v.len(),
            Input::U64(v) => v.len(),
        }
    }

    fn write(&self, w: &mut impl Write) -> std::io::Result<()> {
        w.write_all(&[self.byte_width()])?;
        w.write_all(&(self.len() as u64).to_le_bytes())?;
        macro_rules! body {
            ($v:expr) => {
                for x in $v {
                    w.write_all(&x.to_le_bytes())?;
                }
            };
        }
        match self {
            Input::U8(v) => w.write_all(v)?,
            Input::U16(v) => body!(v),
            Input::U32(v) => body!(v),
            Input::U64(v) => body!(v),
        }
        Ok(())
    }

    fn read(r: &mut impl Read) -> std::io::Result<Option<Input>> {
        let mut tag = [0u8; 1];
        if r.read(&mut tag)? == 0 {
            return Ok(None);
        }
        let mut len = [0u8; 8];
        r.read_exact(&mut len)?;
        let len = u64::from_le_bytes(len) as usize;
        let mut bytes = vec![0u8; len * tag[0] as usize];
        r.read_exact(&mut bytes)?;
        macro_rules! decode {
            ($t:ty, $variant:ident) => {
                Input::$variant(
                    bytes
                        .chunks_exact(size_of::<$t>())
                        .map(|c| <$t>::from_le_bytes(c.try_into().unwrap()))
                        .collect(),
                )
            };
        }
        Ok(Some(match tag[0] {
            1 => Input::U8(bytes),
            2 => decode!(u16, U16),
            4 => decode!(u32, U32),
            8 => decode!(u64, U64),
            t => panic!("bad tag {t}"),
        }))
    }
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
    sink: &mut dyn FnMut(Vec<Input>) -> anyhow::Result<()>,
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
    drop(array);
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
        sink(out)?;
    }
    Ok(total_compressed)
}

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

fn harvest_main(cache_dir: &Path, names: &[String]) -> anyhow::Result<()> {
    std::fs::create_dir_all(cache_dir)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let mut ctx = SESSION.create_execution_ctx();
    for name in names {
        let Some(dataset) = dataset_by_name(name) else {
            eprintln!("unknown dataset {name}");
            continue;
        };
        let file_name = name.replace(':', "_");
        let path = cache_dir.join(format!("{file_name}.zst"));
        let tmp = cache_dir.join(format!("{file_name}.zst.tmp"));
        let mut writer = zstd::Encoder::new(BufWriter::new(File::create(&tmp)?), 3)?;
        let mut arrays = 0usize;
        let started = Instant::now();
        let mut sink = |inputs: Vec<Input>| -> anyhow::Result<()> {
            for input in &inputs {
                input.write(&mut writer)?;
                arrays += 1;
            }
            Ok(())
        };
        match runtime.block_on(harvest(dataset, &mut ctx, &mut sink)) {
            Ok(total) => {
                writer.finish()?.flush()?;
                std::fs::rename(&tmp, &path)?;
                std::fs::write(cache_dir.join(format!("{file_name}.total")), total.to_string())?;
                eprintln!("{name}: {arrays} arrays in {:?}", started.elapsed());
            }
            Err(e) => {
                drop(writer);
                std::fs::remove_file(&tmp).ok();
                eprintln!("{name}: failed: {e:#}");
            }
        }
    }
    Ok(())
}

fn read_cache(path: &Path, mut f: impl FnMut(Input)) -> anyhow::Result<()> {
    let mut reader = BufReader::new(zstd::Decoder::new(File::open(path)?)?);
    while let Some(input) = Input::read(&mut reader)? {
        f(input);
    }
    Ok(())
}

// ------------------------------------------------------------------------------------------------
// Format
// ------------------------------------------------------------------------------------------------

/// How a chunk stores the positions of its patches.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Pos {
    /// Sorted u16 in-chunk positions.
    List,
    /// A 1024-bit skip set; the k-th set bit owns the k-th high part.
    Bitmap,
    /// u16 positions grouped by lane (`pos % L`), with one u8 count per lane.
    Lanes,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum HighFmt {
    /// Narrowest byte width holding the block's high parts.
    Byte,
    /// Bit-packed at the block's high width.
    Bp,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PosPolicy {
    List,
    Lanes,
    /// List or skip set per chunk, whichever the chooser prefers.
    Adaptive,
}

#[derive(Clone, Copy, Debug)]
enum Chooser {
    /// Minimize size plus this many bits per exception.
    Penalty(usize),
    /// Minimize size plus this many bits per estimated nanosecond of decode.
    Time(f64),
    /// Every block at this width and position encoding (calibration only).
    Fixed(usize, Pos),
}

#[derive(Clone, Copy, Debug)]
struct Cfg {
    name: &'static str,
    pos: PosPolicy,
    high: HighFmt,
    chooser: Chooser,
}

const fn cfg(name: &'static str, pos: PosPolicy, high: HighFmt, chooser: Chooser) -> Cfg {
    Cfg {
        name,
        pos,
        high,
        chooser,
    }
}

const CONFIGS: &[Cfg] = &[
    cfg(
        "nopatch",
        PosPolicy::List,
        HighFmt::Bp,
        Chooser::Penalty(1 << 40),
    ),
    cfg("p32-list-bp", PosPolicy::List, HighFmt::Bp, Chooser::Penalty(32)),
    cfg(
        "p32-list-byte",
        PosPolicy::List,
        HighFmt::Byte,
        Chooser::Penalty(32),
    ),
    cfg(
        "p32-lanes-bp",
        PosPolicy::Lanes,
        HighFmt::Bp,
        Chooser::Penalty(32),
    ),
    cfg("p0-list-bp", PosPolicy::List, HighFmt::Bp, Chooser::Penalty(0)),
    cfg(
        "p0-adapt-bp",
        PosPolicy::Adaptive,
        HighFmt::Bp,
        Chooser::Penalty(0),
    ),
    cfg("t1-adapt-bp", PosPolicy::Adaptive, HighFmt::Bp, Chooser::Time(1.0)),
    cfg("t4-adapt-bp", PosPolicy::Adaptive, HighFmt::Bp, Chooser::Time(4.0)),
    cfg(
        "t16-adapt-bp",
        PosPolicy::Adaptive,
        HighFmt::Bp,
        Chooser::Time(16.0),
    ),
    cfg(
        "t64-adapt-bp",
        PosPolicy::Adaptive,
        HighFmt::Bp,
        Chooser::Time(64.0),
    ),
    cfg(
        "t16-adapt-byte",
        PosPolicy::Adaptive,
        HighFmt::Byte,
        Chooser::Time(16.0),
    ),
    cfg("t16-list-bp", PosPolicy::List, HighFmt::Bp, Chooser::Time(16.0)),
];

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Kernel {
    UnpackOnly,
    /// One patch at a time, reading each high part in place.
    Scalar,
    /// Widen the chunk's high parts (BMI2 `pdep` when bit-packed), then an unrolled OR loop.
    Opt,
    /// AVX-512 gather/OR/scatter for lists, round-wise lane scatter for the transposed layout,
    /// expand for skip sets.
    Scatter,
    /// Dense chunks (skip sets, or lists of at least `DENSIFY_MIN` patches turned into one)
    /// through the AVX-512 expand kernel, sparse chunks through `Opt`.
    Simd,
}

const KERNELS: [Kernel; 4] = [Kernel::Scalar, Kernel::Opt, Kernel::Scatter, Kernel::Simd];

impl Kernel {
    fn name(self) -> &'static str {
        match self {
            Kernel::UnpackOnly => "unpack",
            Kernel::Scalar => "scalar",
            Kernel::Opt => "opt",
            Kernel::Scatter => "scatter",
            Kernel::Simd => "simd",
        }
    }
}

/// Number of patch lanes for the transposed layout: one per AVX-512 element.
const fn lanes_for(bits: usize) -> usize {
    if bits <= 32 { 16 } else { 8 }
}

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

/// Read `width` bits starting at bit `bit`. `bytes` must have 9 bytes of padding past the data.
#[inline(always)]
fn read_bits(bytes: &[u8], bit: u64, width: u32) -> u64 {
    if width == 0 {
        return 0;
    }
    let byte = (bit / 8) as usize;
    let sh = (bit % 8) as u32;
    debug_assert!(byte + 9 <= bytes.len());
    // SAFETY: the encoder pads the buffer by 16 bytes.
    let lo = unsafe { (bytes.as_ptr().add(byte) as *const u64).read_unaligned() } >> sh;
    let v = if sh + width > 64 {
        lo | (u64::from(unsafe { *bytes.get_unchecked(byte + 8) }) << (64 - sh))
    } else {
        lo
    };
    if width == 64 {
        v
    } else {
        v & ((1u64 << width) - 1)
    }
}

struct BitWriter {
    words: Vec<u64>,
    len: u64,
}

impl BitWriter {
    fn push(&mut self, v: u64, width: u32) {
        if width == 0 {
            return;
        }
        let off = (self.len % 64) as u32;
        let word = (self.len / 64) as usize;
        while self.words.len() < word + 2 {
            self.words.push(0);
        }
        self.words[word] |= v << off;
        if off != 0 && off + width > 64 {
            self.words[word + 1] |= v >> (64 - off);
        }
        self.len += u64::from(width);
    }

    fn into_bytes(self) -> Vec<u8> {
        let mut bytes: Vec<u8> = self.words.iter().flat_map(|w| w.to_le_bytes()).collect();
        bytes.truncate(self.len.div_ceil(8) as usize);
        bytes.extend_from_slice(&[0; 16]);
        bytes
    }
}

/// Blocked bit-packing with high-bit patches.
struct Enc<T> {
    /// Width of each block. The position encoding of a block fits in the spare bits of this byte.
    widths: Vec<u8>,
    pos_kind: Vec<Pos>,
    packed: Vec<T>,
    /// Cumulative patch counts, `nblocks + 1` entries.
    off: Vec<u32>,
    /// High width of each block.
    hb: Vec<u8>,
    /// List and lane positions, plus one padding entry.
    pos16: Vec<u16>,
    bitmaps: Vec<u64>,
    lane_counts: Vec<u8>,
    high: HighFmt,
    /// High parts, byte-aligned or bit-packed, with 16 bytes of padding.
    hbytes: Vec<u8>,
    high_data_bytes: usize,
    // Offsets below are derivable from the stored data in one pass over the blocks.
    pstart: Vec<u32>,
    pos_start: Vec<u32>,
    bm_start: Vec<u32>,
    lc_start: Vec<u32>,
    /// Byte offset (Byte) or bit offset (Bp) of each block's high parts.
    hstart: Vec<u64>,
}

impl<T: Int> Enc<T> {
    fn nblocks(&self) -> usize {
        self.widths.len()
    }

    fn num_patches(&self) -> usize {
        *self.off.last().unwrap() as usize
    }

    fn nbytes(&self) -> usize {
        let mut n = self.packed.len() * size_of::<T>() + self.widths.len();
        let p = self.num_patches();
        if p > 0 {
            n += self.nblocks() * bytes_for_value(p as u64)
                + self.hb.len()
                + (self.pos16.len() - 1) * 2
                + self.bitmaps.len() * 8
                + self.lane_counts.len()
                + self.high_data_bytes;
        }
        n
    }

    fn dense_chunks(&self) -> usize {
        (0..self.nblocks())
            .filter(|&b| self.off[b] != self.off[b + 1] && self.pos_kind[b] == Pos::Bitmap)
            .count()
    }

    fn patched_chunks(&self) -> usize {
        (0..self.nblocks())
            .filter(|&b| self.off[b] != self.off[b + 1])
            .count()
    }

    #[inline(always)]
    fn high(&self, b: usize, i: usize) -> u64 {
        let hb = u32::from(self.hb[b]);
        match self.high {
            HighFmt::Byte => {
                let nb = bytes_for_bits(hb as usize) as u64;
                read_bits(&self.hbytes, 8 * (self.hstart[b] + i as u64 * nb), 8 * nb as u32)
            }
            HighFmt::Bp => read_bits(&self.hbytes, self.hstart[b] + i as u64 * u64::from(hb), hb),
        }
    }

    /// Widen the high parts of block `b` into `out`.
    #[inline(always)]
    fn widen<U: Int>(&self, b: usize, out: &mut [U]) {
        let hb = self.hb[b] as usize;
        match self.high {
            HighFmt::Byte => {
                let base = self.hstart[b] as usize;
                let c = out.len();
                macro_rules! widen {
                    ($t:ty) => {{
                        let src = &self.hbytes[base..base + c * size_of::<$t>()];
                        for (o, s) in out.iter_mut().zip(src.chunks_exact(size_of::<$t>())) {
                            *o = U::fromw(<$t>::from_le_bytes(s.try_into().unwrap()) as u64);
                        }
                    }};
                }
                match bytes_for_bits(hb) {
                    1 => {
                        for (o, &s) in out.iter_mut().zip(&self.hbytes[base..base + c]) {
                            *o = U::fromw(u64::from(s));
                        }
                    }
                    2 => widen!(u16),
                    4 => widen!(u32),
                    _ => widen!(u64),
                }
            }
            HighFmt::Bp => {
                let base = self.hstart[b];
                let ptr = self.hbytes.as_ptr();
                if hb > 0 && hb <= 32 && has_bmi2() {
                    // SAFETY: BMI2 is available and the buffer is padded by 16 bytes.
                    unsafe { widen_pdep(&self.hbytes, base, hb, out) };
                } else if hb <= 56 {
                    let mask = (1u64 << hb) - 1;
                    let mut bit = base as usize;
                    for o in out.iter_mut() {
                        // SAFETY: the encoder pads the buffer by 16 bytes.
                        let word = unsafe { (ptr.add(bit >> 3) as *const u64).read_unaligned() };
                        *o = U::fromw((word >> (bit & 7)) & mask);
                        bit += hb;
                    }
                } else {
                    for (i, o) in out.iter_mut().enumerate() {
                        *o = U::fromw(read_bits(&self.hbytes, base + (i * hb) as u64, hb as u32));
                    }
                }
            }
        }
    }

    fn decode(&self, out: &mut [T], kernel: Kernel) {
        debug_assert_eq!(out.len(), self.nblocks() * N);
        let mut tmp = [T::zero(); N + 16];
        let mut tmp32 = [0u32; N + 16];
        for b in 0..self.nblocks() {
            let w = self.widths[b] as usize;
            let dst = &mut out[b * N..][..N];
            if w == 0 {
                dst.fill(T::zero());
            } else {
                let l = packed_len::<T>(w);
                unsafe {
                    BitPacking::unchecked_unpack(w, &self.packed[self.pstart[b] as usize..][..l], dst)
                };
            }
            if kernel == Kernel::UnpackOnly || self.off[b] == self.off[b + 1] {
                continue;
            }
            match kernel {
                Kernel::Scalar => self.apply_scalar(b, dst, w),
                Kernel::Opt => self.apply_opt(b, dst, w, &mut tmp),
                Kernel::Scatter => self.apply_scatter(b, dst, w, &mut tmp, &mut tmp32),
                Kernel::Simd => self.apply_simd(b, dst, w, &mut tmp, &mut tmp32),
                Kernel::UnpackOnly => unreachable!(),
            }
        }
    }

    fn count(&self, b: usize) -> usize {
        (self.off[b + 1] - self.off[b]) as usize
    }

    fn bitmap(&self, b: usize) -> &[u64] {
        &self.bitmaps[self.bm_start[b] as usize..][..N / 64]
    }

    fn positions(&self, b: usize) -> &[u16] {
        &self.pos16[self.pos_start[b] as usize..][..self.count(b)]
    }

    /// One patch at a time, reading each high part in place.
    fn apply_scalar(&self, b: usize, dst: &mut [T], w: usize) {
        match self.pos_kind[b] {
            Pos::List | Pos::Lanes => {
                for (i, &j) in self.positions(b).iter().enumerate() {
                    dst[j as usize] = dst[j as usize] | (T::fromw(self.high(b, i)) << w);
                }
            }
            Pos::Bitmap => {
                let mut i = 0;
                for (wi, &word) in self.bitmap(b).iter().enumerate() {
                    let mut word = word;
                    while word != 0 {
                        let j = wi * 64 + word.trailing_zeros() as usize;
                        word &= word - 1;
                        dst[j] = dst[j] | (T::fromw(self.high(b, i)) << w);
                        i += 1;
                    }
                }
            }
        }
    }

    /// Widen the high parts first, then a tight OR loop.
    fn apply_opt(&self, b: usize, dst: &mut [T], w: usize, tmp: &mut [T; N + 16]) {
        let c = self.count(b);
        self.widen(b, &mut tmp[..c]);
        self.or_widened(b, dst, w, &tmp[..c]);
    }

    #[inline(always)]
    fn or_widened(&self, b: usize, dst: &mut [T], w: usize, highs: &[T]) {
        match self.pos_kind[b] {
            Pos::List | Pos::Lanes => {
                let pos = self.positions(b);
                let mut pc = pos.chunks_exact(4);
                let mut hc = highs.chunks_exact(4);
                for (p, h) in (&mut pc).zip(&mut hc) {
                    // SAFETY: positions are below 1024.
                    unsafe {
                        let (p0, p1, p2, p3) =
                            (p[0] as usize, p[1] as usize, p[2] as usize, p[3] as usize);
                        *dst.get_unchecked_mut(p0) = *dst.get_unchecked(p0) | (h[0] << w);
                        *dst.get_unchecked_mut(p1) = *dst.get_unchecked(p1) | (h[1] << w);
                        *dst.get_unchecked_mut(p2) = *dst.get_unchecked(p2) | (h[2] << w);
                        *dst.get_unchecked_mut(p3) = *dst.get_unchecked(p3) | (h[3] << w);
                    }
                }
                for (&p, &h) in pc.remainder().iter().zip(hc.remainder()) {
                    unsafe {
                        *dst.get_unchecked_mut(p as usize) = *dst.get_unchecked(p as usize) | (h << w)
                    };
                }
            }
            Pos::Bitmap => {
                let mut i = 0;
                for (wi, &word) in self.bitmap(b).iter().enumerate() {
                    let mut word = word;
                    while word != 0 {
                        let j = wi * 64 + word.trailing_zeros() as usize;
                        word &= word - 1;
                        unsafe {
                            *dst.get_unchecked_mut(j) =
                                *dst.get_unchecked(j) | (*highs.get_unchecked(i) << w)
                        };
                        i += 1;
                    }
                }
            }
        }
    }

    fn apply_simd(
        &self,
        b: usize,
        dst: &mut [T],
        w: usize,
        tmp: &mut [T; N + 16],
        tmp32: &mut [u32; N + 16],
    ) {
        if !has_avx512() {
            return self.apply_opt(b, dst, w, tmp);
        }
        match self.pos_kind[b] {
            Pos::Bitmap => {
                let bm: &[u64; N / 64] = self.bitmap(b).try_into().unwrap();
                self.apply_dense(b, dst, w, bm, tmp, tmp32)
            }
            Pos::List if self.count(b) >= DENSIFY_MIN => {
                // Sorted list positions match skip-set rank order, so a skip set built here
                // lets the expand kernel apply the chunk.
                let mut bm = [0u64; N / 64];
                for &j in self.positions(b) {
                    bm[j as usize / 64] |= 1 << (j % 64);
                }
                self.apply_dense(b, dst, w, &bm, tmp, tmp32)
            }
            _ => self.apply_opt(b, dst, w, tmp),
        }
    }

    fn apply_scatter(
        &self,
        b: usize,
        dst: &mut [T],
        w: usize,
        tmp: &mut [T; N + 16],
        tmp32: &mut [u32; N + 16],
    ) {
        if !has_avx512() {
            return self.apply_opt(b, dst, w, tmp);
        }
        let kind = self.pos_kind[b];
        if kind == Pos::Bitmap {
            let bm: &[u64; N / 64] = self.bitmap(b).try_into().unwrap();
            return self.apply_dense(b, dst, w, bm, tmp, tmp32);
        }
        if T::BITS < 32 {
            // No 8- or 16-bit gather/scatter in AVX-512F/BW.
            return self.apply_opt(b, dst, w, tmp);
        }
        let c = self.count(b);
        self.widen(b, &mut tmp[..c]);
        let d = dst.as_mut_ptr();
        let h = tmp.as_ptr();
        // SAFETY: AVX-512 is available; `dst` holds 1024 values and `h` holds `c` highs.
        unsafe {
            match (T::BITS, kind) {
                (32, Pos::List) => simd::scatter32(d.cast(), self.positions(b), h.cast(), w),
                (64, Pos::List) => simd::scatter64(d.cast(), self.positions(b), h.cast(), w),
                (32, Pos::Lanes) => simd::lanes32(
                    d.cast(),
                    &self.pos16[self.pos_start[b] as usize..],
                    self.lane_slice(b),
                    h.cast(),
                    w,
                ),
                (64, Pos::Lanes) => simd::lanes64(
                    d.cast(),
                    &self.pos16[self.pos_start[b] as usize..],
                    self.lane_slice(b),
                    h.cast(),
                    w,
                ),
                _ => unreachable!(),
            }
        }
    }

    /// Expand the chunk's high parts into the set lanes of `bm` with AVX-512.
    fn apply_dense(
        &self,
        b: usize,
        dst: &mut [T],
        w: usize,
        bm: &[u64; N / 64],
        tmp: &mut [T; N + 16],
        tmp32: &mut [u32; N + 16],
    ) {
        let c = self.count(b);
        // SAFETY: AVX-512 is available, `dst` holds 1024 values and the scratch buffers are
        // padded so the kernels may read 16 elements past the last high part.
        unsafe {
            match T::BITS {
                32 => {
                    self.widen(b, &mut tmp[..c]);
                    simd::expand32(dst.as_mut_ptr().cast(), bm, tmp.as_ptr().cast(), w)
                }
                64 => {
                    self.widen(b, &mut tmp[..c]);
                    simd::expand64(dst.as_mut_ptr().cast(), bm, tmp.as_ptr().cast(), w)
                }
                16 => {
                    self.widen(b, &mut tmp32[..c]);
                    simd::expand16(dst.as_mut_ptr().cast(), bm, tmp32.as_ptr(), w)
                }
                _ => {
                    self.widen(b, &mut tmp32[..c]);
                    simd::expand8(dst.as_mut_ptr().cast(), bm, tmp32.as_ptr(), w)
                }
            }
        }
    }

    fn lane_slice(&self, b: usize) -> &[u8] {
        &self.lane_counts[self.lc_start[b] as usize..][..lanes_for(T::BITS)]
    }

    fn get(&self, i: usize) -> T {
        let b = i / N;
        let j = i % N;
        let w = self.widths[b] as usize;
        let low = if w == 0 {
            T::zero()
        } else {
            let l = packed_len::<T>(w);
            unsafe {
                BitPacking::unchecked_unpack_single(w, &self.packed[self.pstart[b] as usize..][..l], j)
            }
        };
        if self.off[b] == self.off[b + 1] {
            return low;
        }
        let k = match self.pos_kind[b] {
            Pos::List => self.positions(b).binary_search(&(j as u16)).ok(),
            Pos::Bitmap => {
                let bm = self.bitmap(b);
                let word = bm[j / 64];
                (word >> (j % 64) & 1 == 1).then(|| {
                    (bm[..j / 64].iter().map(|w| w.count_ones()).sum::<u32>()
                        + (word & ((1u64 << (j % 64)) - 1)).count_ones()) as usize
                })
            }
            Pos::Lanes => {
                let lanes = lanes_for(T::BITS);
                let counts = self.lane_slice(b);
                let lane = j % lanes;
                let start: usize = counts[..lane].iter().map(|&c| c as usize).sum();
                let n = counts[lane] as usize;
                self.pos16[self.pos_start[b] as usize + start..][..n]
                    .binary_search(&(j as u16))
                    .ok()
                    .map(|k| start + k)
            }
        };
        match k {
            Some(k) => low | (T::fromw(self.high(b, k)) << w),
            None => low,
        }
    }
}

fn has_bmi2() -> bool {
    static HAS: std::sync::LazyLock<bool> =
        std::sync::LazyLock::new(|| is_x86_feature_detected!("bmi2"));
    *HAS
}

/// Unpack `out.len()` values of `hb <= 32` bits starting at bit `base`, depositing 64 / L of them
/// at a time into L-bit lanes (L = 8, 16 or 32) with one `pdep`.
#[target_feature(enable = "bmi2")]
unsafe fn widen_pdep<U: Int>(bytes: &[u8], base: u64, hb: usize, out: &mut [U]) {
    if hb <= 8 {
        widen_pdep_lanes::<U, 8>(bytes, base, hb, out)
    } else if hb <= 16 {
        widen_pdep_lanes::<U, 16>(bytes, base, hb, out)
    } else {
        widen_pdep_lanes::<U, 32>(bytes, base, hb, out)
    }
}

#[inline]
#[target_feature(enable = "bmi2")]
unsafe fn widen_pdep_lanes<U: Int, const LANE: usize>(
    bytes: &[u8],
    base: u64,
    hb: usize,
    out: &mut [U],
) {
    use std::arch::x86_64::_pdep_u64;
    let per = 64 / LANE;
    let field = (1u64 << hb) - 1;
    let mut mask = 0u64;
    for i in 0..per {
        mask |= field << (i * LANE);
    }
    let lane_mask = (1u64 << LANE) - 1;
    let ptr = bytes.as_ptr();
    let mut bit = base as usize;
    let step = per * hb;
    let mut chunks = out.chunks_exact_mut(64 / LANE);
    for group in &mut chunks {
        let byte = bit >> 3;
        let sh = (bit & 7) as u32;
        let lo = (ptr.add(byte) as *const u64).read_unaligned();
        let hi = u64::from(*ptr.add(byte + 8));
        // `step + sh` can exceed 64 bits; take the rest from the next byte. `<< 1 << (63 - sh)`
        // is a branch-free `<< (64 - sh)` that yields 0 when `sh == 0`.
        let v = (lo >> sh) | ((hi << 1) << (63 - sh));
        let d = _pdep_u64(v, mask);
        for (i, o) in group.iter_mut().enumerate() {
            *o = U::fromw((d >> (i * LANE)) & lane_mask);
        }
        bit += step;
    }
    for o in chunks.into_remainder() {
        *o = U::fromw(read_bits(bytes, bit as u64, hb as u32));
        bit += hb;
    }
}

/// From this many list patches in a chunk, building a skip set and expanding beats scattering.
const DENSIFY_MIN: usize = 384;

fn has_avx512() -> bool {
    static HAS: std::sync::LazyLock<bool> = std::sync::LazyLock::new(|| {
        is_x86_feature_detected!("avx512f")
            && is_x86_feature_detected!("avx512bw")
            && is_x86_feature_detected!("avx512vl")
    });
    *HAS
}

mod simd {
    use std::arch::asm;
    use std::arch::x86_64::*;

    /// Register-form `vpexpandd`. LLVM folds a preceding load into the memory form, which is
    /// microcoded and several times slower on Intel cores, so this is written in asm.
    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn expand_epi32(m: u16, a: __m512i) -> __m512i {
        let out: __m512i;
        asm!(
            "vpexpandd {out}{{{k}}}{{z}}, {a}",
            out = lateout(zmm_reg) out,
            k = in(kreg) m,
            a = in(zmm_reg) a,
            options(pure, nomem, nostack),
        );
        out
    }

    #[inline]
    #[target_feature(enable = "avx512f")]
    unsafe fn expand_epi64(m: u8, a: __m512i) -> __m512i {
        let out: __m512i;
        asm!(
            "vpexpandq {out}{{{k}}}{{z}}, {a}",
            out = lateout(zmm_reg) out,
            k = in(kreg) m,
            a = in(zmm_reg) a,
            options(pure, nomem, nostack),
        );
        out
    }

    /// Gather, OR and scatter 16 list patches at a time. Positions are unique, so no conflicts.
    #[target_feature(enable = "avx512f,avx512bw,avx512vl,popcnt")]
    pub unsafe fn scatter32(dst: *mut u32, pos: &[u16], highs: *const u32, w: usize) {
        let sh = _mm_cvtsi32_si128(w as i32);
        let c = pos.len();
        let mut k = 0;
        while k < c {
            let rem = c - k;
            let m: __mmask16 = if rem >= 16 { 0xFFFF } else { ((1u32 << rem) - 1) as u16 };
            let idx = _mm512_cvtepu16_epi32(_mm256_maskz_loadu_epi16(m, pos.as_ptr().add(k).cast()));
            let h = _mm512_maskz_loadu_epi32(m, highs.add(k).cast());
            let cur = _mm512_mask_i32gather_epi32::<4>(_mm512_setzero_si512(), m, idx, dst.cast());
            let v = _mm512_or_si512(cur, _mm512_sll_epi32(h, sh));
            _mm512_mask_i32scatter_epi32::<4>(dst.cast(), m, idx, v);
            k += 16;
        }
    }

    #[target_feature(enable = "avx512f,avx512bw,avx512vl,popcnt")]
    pub unsafe fn scatter64(dst: *mut u64, pos: &[u16], highs: *const u64, w: usize) {
        let sh = _mm_cvtsi32_si128(w as i32);
        let c = pos.len();
        let mut k = 0;
        while k < c {
            let rem = c - k;
            let m: __mmask8 = if rem >= 8 { 0xFF } else { ((1u32 << rem) - 1) as u8 };
            let idx = _mm256_cvtepu16_epi32(_mm_maskz_loadu_epi16(m, pos.as_ptr().add(k).cast()));
            let h = _mm512_maskz_loadu_epi64(m, highs.add(k).cast());
            let cur = _mm512_mask_i32gather_epi64::<8>(_mm512_setzero_si512(), m, idx, dst.cast());
            let v = _mm512_or_si512(cur, _mm512_sll_epi64(h, sh));
            _mm512_mask_i32scatter_epi64::<8>(dst.cast(), m, idx, v);
            k += 8;
        }
    }

    /// For each group of 16 outputs, load the next 16 high parts, expand them into the set lanes
    /// of the skip set and OR them in. Branch-free. The register form of `vpexpandd` is used
    /// because the memory form is microcoded and several times slower on Intel cores. `highs`
    /// must be readable 16 elements past the last high part.
    #[target_feature(enable = "avx512f,avx512bw,avx512vl,popcnt")]
    pub unsafe fn expand32(dst: *mut u32, bm: &[u64], highs: *const u32, w: usize) {
        let sh = _mm_cvtsi32_si128(w as i32);
        let mut p = highs;
        for g in 0..64 {
            let m = (bm[g >> 2] >> ((g & 3) * 16)) as u16;
            let h = expand_epi32(m, _mm512_loadu_si512(p.cast()));
            let o = dst.add(16 * g).cast::<__m512i>();
            _mm512_storeu_si512(o, _mm512_or_si512(_mm512_loadu_si512(o), _mm512_sll_epi32(h, sh)));
            p = p.add(m.count_ones() as usize);
        }
    }

    #[target_feature(enable = "avx512f,avx512bw,avx512vl,popcnt")]
    pub unsafe fn expand64(dst: *mut u64, bm: &[u64], highs: *const u64, w: usize) {
        let sh = _mm_cvtsi32_si128(w as i32);
        let mut p = highs;
        for g in 0..128 {
            let m = (bm[g >> 3] >> ((g & 7) * 8)) as u8;
            let h = expand_epi64(m, _mm512_loadu_si512(p.cast()));
            let o = dst.add(8 * g).cast::<__m512i>();
            _mm512_storeu_si512(o, _mm512_or_si512(_mm512_loadu_si512(o), _mm512_sll_epi64(h, sh)));
            p = p.add(m.count_ones() as usize);
        }
    }

    /// 16-bit outputs: expand 32-bit high parts, shift, narrow and OR.
    #[target_feature(enable = "avx512f,avx512bw,avx512vl,popcnt")]
    pub unsafe fn expand16(dst: *mut u16, bm: &[u64], highs: *const u32, w: usize) {
        let sh = _mm_cvtsi32_si128(w as i32);
        let mut p = highs;
        for g in 0..64 {
            let m = (bm[g >> 2] >> ((g & 3) * 16)) as u16;
            let h = _mm512_cvtepi32_epi16(_mm512_sll_epi32(expand_epi32(m, _mm512_loadu_si512(p.cast())), sh));
            let o = dst.add(16 * g).cast::<__m256i>();
            _mm256_storeu_si256(o, _mm256_or_si256(_mm256_loadu_si256(o), h));
            p = p.add(m.count_ones() as usize);
        }
    }

    #[target_feature(enable = "avx512f,avx512bw,avx512vl,popcnt")]
    pub unsafe fn expand8(dst: *mut u8, bm: &[u64], highs: *const u32, w: usize) {
        let sh = _mm_cvtsi32_si128(w as i32);
        let mut p = highs;
        for g in 0..64 {
            let m = (bm[g >> 2] >> ((g & 3) * 16)) as u16;
            let h = _mm512_cvtepi32_epi8(_mm512_sll_epi32(expand_epi32(m, _mm512_loadu_si512(p.cast())), sh));
            let o = dst.add(16 * g).cast::<__m128i>();
            _mm_storeu_si128(o, _mm_or_si128(_mm_loadu_si128(o), h));
            p = p.add(m.count_ones() as usize);
        }
    }

    /// Transposed patches: round `r` applies the `r`-th patch of every lane that has one, with
    /// one masked gather of positions, highs and outputs and one masked scatter per round.
    #[target_feature(enable = "avx512f,avx512bw,avx512vl,popcnt")]
    pub unsafe fn lanes32(dst: *mut u32, pos: &[u16], counts: &[u8], highs: *const u32, w: usize) {
        let sh = _mm_cvtsi32_si128(w as i32);
        let mut starts = [0u32; 16];
        let mut acc = 0u32;
        let mut rounds = 0u32;
        for l in 0..16 {
            starts[l] = acc;
            acc += u32::from(counts[l]);
            rounds = rounds.max(u32::from(counts[l]));
        }
        let cnt = _mm512_cvtepu8_epi32(_mm_loadu_si128(counts.as_ptr().cast()));
        let base = _mm512_loadu_si512(starts.as_ptr().cast());
        let low16 = _mm512_set1_epi32(0xFFFF);
        for r in 0..rounds {
            let rv = _mm512_set1_epi32(r as i32);
            let m = _mm512_cmpgt_epu32_mask(cnt, rv);
            let k = _mm512_add_epi32(base, rv);
            // Two-byte stride: each 32-bit load holds the position in its low half.
            let idx = _mm512_and_si512(
                _mm512_mask_i32gather_epi32::<2>(_mm512_setzero_si512(), m, k, pos.as_ptr().cast()),
                low16,
            );
            let h = _mm512_mask_i32gather_epi32::<4>(_mm512_setzero_si512(), m, k, highs.cast());
            let cur = _mm512_mask_i32gather_epi32::<4>(_mm512_setzero_si512(), m, idx, dst.cast());
            _mm512_mask_i32scatter_epi32::<4>(dst.cast(), m, idx, _mm512_or_si512(cur, _mm512_sll_epi32(h, sh)));
        }
    }

    #[target_feature(enable = "avx512f,avx512bw,avx512vl,popcnt")]
    pub unsafe fn lanes64(dst: *mut u64, pos: &[u16], counts: &[u8], highs: *const u64, w: usize) {
        let sh = _mm_cvtsi32_si128(w as i32);
        let mut starts = [0u32; 8];
        let mut cnts = [0u32; 8];
        let mut acc = 0u32;
        let mut rounds = 0u32;
        for l in 0..8 {
            starts[l] = acc;
            cnts[l] = u32::from(counts[l]);
            acc += cnts[l];
            rounds = rounds.max(cnts[l]);
        }
        let cnt = _mm256_loadu_si256(cnts.as_ptr().cast());
        let base = _mm256_loadu_si256(starts.as_ptr().cast());
        let low16 = _mm256_set1_epi32(0xFFFF);
        for r in 0..rounds {
            let rv = _mm256_set1_epi32(r as i32);
            let m = _mm256_cmpgt_epu32_mask(cnt, rv);
            let k = _mm256_add_epi32(base, rv);
            let idx = _mm256_and_si256(
                _mm256_mmask_i32gather_epi32::<2>(_mm256_setzero_si256(), m, k, pos.as_ptr().cast()),
                low16,
            );
            let h = _mm512_mask_i32gather_epi64::<8>(_mm512_setzero_si512(), m, k, highs.cast());
            let cur = _mm512_mask_i32gather_epi64::<8>(_mm512_setzero_si512(), m, idx, dst.cast());
            _mm512_mask_i32scatter_epi64::<8>(dst.cast(), m, idx, _mm512_or_si512(cur, _mm512_sll_epi64(h, sh)));
        }
    }
}

// ------------------------------------------------------------------------------------------------
// Encoder and choosers
// ------------------------------------------------------------------------------------------------

/// Estimated decode time of one block, from a startup calibration of the `simd` kernels.
#[derive(Clone, Debug)]
struct TimeModel {
    /// ns to unpack a block, by width.
    unpack: Vec<f64>,
    /// Fixed ns per patched chunk and ns per patch, by [high format][position encoding].
    chunk: [[f64; 3]; 2],
    per: [[f64; 3]; 2],
}

fn pos_idx(p: Pos) -> usize {
    match p {
        Pos::List => 0,
        Pos::Bitmap => 1,
        Pos::Lanes => 2,
    }
}

fn high_idx(h: HighFmt) -> usize {
    match h {
        HighFmt::Byte => 0,
        HighFmt::Bp => 1,
    }
}

impl TimeModel {
    fn time(&self, w: usize, p: Pos, e: usize, h: HighFmt) -> f64 {
        let mut t = self.unpack[w];
        if e > 0 {
            t += self.chunk[high_idx(h)][pos_idx(p)] + e as f64 * self.per[high_idx(h)][pos_idx(p)];
        }
        t
    }
}

fn exceptions(h: &[u32], w: usize) -> usize {
    h[w + 1..].iter().map(|&c| c as usize).sum()
}

fn choose<T: Int>(h: &[u32], m: usize, cfg: &Cfg, tm: &TimeModel) -> (usize, Pos) {
    if let Chooser::Fixed(w, p) = cfg.chooser {
        return (w, p);
    }
    let lanes = lanes_for(T::BITS);
    let mut best = (f64::INFINITY, 0, Pos::List);
    for w in 0..=m {
        let e = exceptions(h, w);
        let hb = m - w;
        let hbits = match cfg.high {
            HighFmt::Byte => 8 * bytes_for_bits(hb),
            HighFmt::Bp => hb,
        };
        let cands: &[Pos] = if e == 0 {
            &[Pos::List]
        } else {
            match cfg.pos {
                PosPolicy::List => &[Pos::List],
                PosPolicy::Lanes => &[Pos::Lanes],
                PosPolicy::Adaptive => &[Pos::List, Pos::Bitmap],
            }
        };
        for &p in cands {
            let pos_bits = match (e, p) {
                (0, _) => 0,
                (_, Pos::List) => 16 * e,
                (_, Pos::Lanes) => 16 * e + 8 * lanes,
                (_, Pos::Bitmap) => N,
            };
            let size = (N * w + pos_bits + e * hbits + if e > 0 { 8 } else { 0 }) as f64;
            let cost = match cfg.chooser {
                Chooser::Penalty(pn) => size + (pn * e) as f64,
                Chooser::Time(l) => size + l * tm.time(w, p, e, cfg.high),
                Chooser::Fixed(..) => unreachable!(),
            };
            if cost < best.0 {
                best = (cost, w, p);
            }
        }
    }
    (best.1, best.2)
}

fn encode<T: Int>(v: &[T], cfg: &Cfg, tm: &TimeModel) -> Enc<T> {
    let nblocks = v.len().div_ceil(N);
    let lanes = lanes_for(T::BITS);
    let mut e = Enc {
        widths: Vec::with_capacity(nblocks),
        pos_kind: Vec::with_capacity(nblocks),
        packed: Vec::new(),
        off: vec![0u32],
        hb: Vec::with_capacity(nblocks),
        pos16: Vec::new(),
        bitmaps: Vec::new(),
        lane_counts: Vec::new(),
        high: cfg.high,
        hbytes: Vec::new(),
        high_data_bytes: 0,
        pstart: Vec::with_capacity(nblocks),
        pos_start: Vec::with_capacity(nblocks),
        bm_start: Vec::with_capacity(nblocks),
        lc_start: Vec::with_capacity(nblocks),
        hstart: Vec::with_capacity(nblocks),
    };
    let mut hbytes = Vec::new();
    let mut hbits = BitWriter {
        words: Vec::new(),
        len: 0,
    };
    let mut pad = [T::zero(); N];
    let mut hist = vec![0u32; T::BITS + 1];
    let mut exc: Vec<(u16, u64)> = Vec::with_capacity(N);
    for chunk in v.chunks(N) {
        hist.fill(0);
        hist[0] = (N - chunk.len()) as u32;
        for &x in chunk {
            hist[x.width()] += 1;
        }
        let m = hist.iter().rposition(|&c| c > 0).unwrap_or(0);
        let (w, p) = choose::<T>(&hist, m, cfg, tm);

        // Pack.
        let l = packed_len::<T>(w);
        e.pstart.push(e.packed.len() as u32);
        if w > 0 {
            let input: &[T] = if chunk.len() == N {
                chunk
            } else {
                pad.fill(T::zero());
                pad[..chunk.len()].copy_from_slice(chunk);
                &pad
            };
            let start = e.packed.len();
            e.packed.resize(start + l, T::zero());
            unsafe { BitPacking::unchecked_pack(w, input, &mut e.packed[start..]) };
        }

        // Patches.
        exc.clear();
        for (j, &x) in chunk.iter().enumerate() {
            if x.width() > w {
                exc.push((j as u16, (x >> w).tow()));
            }
        }
        let hb = if exc.is_empty() { 0 } else { m - w };
        if p == Pos::Lanes {
            // Stable sort by lane keeps positions ascending within each lane.
            exc.sort_by_key(|&(j, _)| j as usize % lanes);
        }
        e.widths.push(w as u8);
        e.pos_kind.push(p);
        e.hb.push(hb as u8);
        e.pos_start.push(e.pos16.len() as u32);
        e.bm_start.push(e.bitmaps.len() as u32);
        e.lc_start.push(e.lane_counts.len() as u32);
        e.hstart.push(match cfg.high {
            HighFmt::Byte => hbytes.len() as u64,
            HighFmt::Bp => hbits.len,
        });
        if !exc.is_empty() {
            match p {
                Pos::List => e.pos16.extend(exc.iter().map(|&(j, _)| j)),
                Pos::Lanes => {
                    let mut counts = vec![0u8; lanes];
                    for &(j, _) in &exc {
                        counts[j as usize % lanes] += 1;
                    }
                    e.lane_counts.extend_from_slice(&counts);
                    e.pos16.extend(exc.iter().map(|&(j, _)| j));
                }
                Pos::Bitmap => {
                    let mut bm = [0u64; N / 64];
                    for &(j, _) in &exc {
                        bm[j as usize / 64] |= 1 << (j % 64);
                    }
                    e.bitmaps.extend_from_slice(&bm);
                }
            }
            for &(_, h) in &exc {
                match cfg.high {
                    HighFmt::Byte => hbytes.extend_from_slice(&h.to_le_bytes()[..bytes_for_bits(hb)]),
                    HighFmt::Bp => hbits.push(h, hb as u32),
                }
            }
        }
        e.off.push(e.off.last().unwrap() + exc.len() as u32);
    }
    e.pos16.push(0);
    e.high_data_bytes = match cfg.high {
        HighFmt::Byte => hbytes.len(),
        HighFmt::Bp => hbits.len.div_ceil(8) as usize,
    };
    e.hbytes = match cfg.high {
        HighFmt::Byte => {
            hbytes.extend_from_slice(&[0; 16]);
            hbytes
        }
        HighFmt::Bp => hbits.into_bytes(),
    };
    e
}

// ------------------------------------------------------------------------------------------------
// Calibration
// ------------------------------------------------------------------------------------------------

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

const CAL_BLOCKS: usize = 64;

fn cal_decode_ns<T: Int>(enc: &Enc<T>, kernel: Kernel) -> f64 {
    let mut out = vec![T::zero(); enc.nblocks() * N];
    time_min(30, || enc.decode(black_box(&mut out), kernel)).as_nanos() as f64 / CAL_BLOCKS as f64
}

fn calibrate<T: Int>() -> TimeModel {
    let mut rng = StdRng::seed_from_u64(T::BITS as u64);
    let dummy = TimeModel {
        unpack: vec![0.0; T::BITS + 1],
        chunk: [[0.0; 3]; 2],
        per: [[0.0; 3]; 2],
    };
    let mut unpack = vec![0.0; T::BITS + 1];
    for (w, u) in unpack.iter_mut().enumerate() {
        let v: Vec<T> = (0..CAL_BLOCKS * N)
            .map(|_| T::fromw(rng.random::<u64>() & ((1u128 << w) - 1) as u64))
            .collect();
        let c = cfg("cal", PosPolicy::List, HighFmt::Bp, Chooser::Fixed(w, Pos::List));
        *u = cal_decode_ns(&encode(&v, &c, &dummy), Kernel::UnpackOnly);
    }

    // Patched blocks at a base width with `n` exceptions of `hb` extra bits each.
    let w = (T::BITS / 2).min(8);
    let hb = (T::BITS - w).min(8);
    let mut chunk = [[0.0; 3]; 2];
    let mut per = [[0.0; 3]; 2];
    for high in [HighFmt::Byte, HighFmt::Bp] {
        for p in [Pos::List, Pos::Bitmap, Pos::Lanes] {
            let (n1, n2) = if p == Pos::Bitmap { (64, 512) } else { (8, 128) };
            let mut t = [0.0; 2];
            for (ti, n) in [n1, n2].into_iter().enumerate() {
                let mut v: Vec<T> = (0..CAL_BLOCKS * N)
                    .map(|_| T::fromw(rng.random::<u64>() & ((1u64 << w) - 1)))
                    .collect();
                for blk in v.chunks_mut(N) {
                    let mut placed = 0;
                    while placed < n {
                        let j = rng.random_range(0..N);
                        if blk[j].width() <= w {
                            let high = (rng.random::<u64>() & ((1u64 << hb) - 1)) | (1u64 << (hb - 1));
                            blk[j] = T::fromw((high << w) | blk[j].tow());
                            placed += 1;
                        }
                    }
                }
                let c = cfg("cal", PosPolicy::List, high, Chooser::Fixed(w, p));
                let enc = encode(&v, &c, &dummy);
                t[ti] = (cal_decode_ns(&enc, Kernel::Simd) - cal_decode_ns(&enc, Kernel::UnpackOnly)).max(0.0);
            }
            let slope = ((t[1] - t[0]) / (n2 - n1) as f64).max(0.0);
            chunk[high_idx(high)][pos_idx(p)] = (t[0] - slope * n1 as f64).max(0.0);
            per[high_idx(high)][pos_idx(p)] = slope;
        }
    }
    TimeModel {
        unpack,
        chunk,
        per,
    }
}

struct TimeModels {
    u8: TimeModel,
    u16: TimeModel,
    u32: TimeModel,
    u64: TimeModel,
}

trait PickModel {
    fn pick(tms: &TimeModels) -> &TimeModel;
}

macro_rules! pick_model {
    ($($t:ident),*) => {$(
        impl PickModel for $t {
            fn pick(tms: &TimeModels) -> &TimeModel {
                &tms.$t
            }
        }
    )*};
}
pick_model!(u8, u16, u32, u64);

// ------------------------------------------------------------------------------------------------
// Measurement
// ------------------------------------------------------------------------------------------------

#[derive(Default, Clone)]
struct Stats {
    arrays: usize,
    raw_bytes: usize,
    bytes: usize,
    patches: usize,
    patched_chunks: usize,
    dense_chunks: usize,
    encode: Duration,
    decode: Duration,
    unpack: Duration,
    take: Duration,
    takes: usize,
    failures: usize,
}

impl Stats {
    fn add(&mut self, o: &Stats) {
        self.arrays += o.arrays;
        self.raw_bytes += o.raw_bytes;
        self.bytes += o.bytes;
        self.patches += o.patches;
        self.patched_chunks += o.patched_chunks;
        self.dense_chunks += o.dense_chunks;
        self.encode += o.encode;
        self.decode += o.decode;
        self.unpack += o.unpack;
        self.take += o.take;
        self.takes += o.takes;
        self.failures += o.failures;
    }
}

fn reps_for(len: usize) -> usize {
    (2_000_000 / len.max(1)).clamp(3, 20)
}

type Results = BTreeMap<String, Stats>;

fn measure_array<T: Int + PickModel>(v: &[T], tms: &TimeModels, res: &mut Results) {
    let tm = T::pick(tms);
    let reps = reps_for(v.len());
    let mut rng = StdRng::seed_from_u64(v.len() as u64);
    let idx: Vec<u32> = (0..NUM_RANDOM).map(|_| rng.random_range(0..v.len() as u32)).collect();
    let mut out = vec![T::zero(); v.len().div_ceil(N) * N];
    for c in CONFIGS {
        let mut enc = None;
        let encode_t = time_min(3, || enc = Some(encode(v, c, tm)));
        let enc = enc.unwrap();
        let unpack = time_min(reps, || enc.decode(black_box(&mut out), Kernel::UnpackOnly));
        let mut got = vec![T::zero(); idx.len()];
        let take = time_min(reps, || {
            for (g, &i) in got.iter_mut().zip(&idx) {
                *g = enc.get(i as usize);
            }
            black_box(&got);
        });
        let take_fail = usize::from(got.iter().zip(&idx).any(|(&g, &i)| g != v[i as usize]));
        for k in KERNELS {
            let decode = time_min(reps, || enc.decode(black_box(&mut out), k));
            let fail = usize::from(&out[..v.len()] != v) + take_fail;
            res.entry(format!("{}/{}", c.name, k.name())).or_default().add(&Stats {
                arrays: 1,
                raw_bytes: size_of_val(v),
                bytes: enc.nbytes(),
                patches: enc.num_patches(),
                patched_chunks: enc.patched_chunks(),
                dense_chunks: enc.dense_chunks(),
                encode: encode_t,
                decode,
                unpack,
                take,
                takes: idx.len(),
                failures: fail,
            });
        }
    }
    let s = measure_vortex(v, &idx, reps);
    res.entry("vortex-v1".to_string()).or_default().add(&s);
}

fn measure_vortex<T: Int>(v: &[T], idx: &[u32], reps: usize) -> Stats {
    let mut ctx = SESSION.create_execution_ctx();
    let prim = PrimitiveArray::new(Buffer::<T>::copy_from(v), Validity::NonNullable);
    let mut arr = None;
    let encode = time_min(3, || arr = Some(bitpack_to_best_bit_width(&prim, &mut ctx)));
    let Ok(bp) = arr.unwrap() else {
        return Stats {
            arrays: 1,
            raw_bytes: size_of_val(v),
            bytes: size_of_val(v),
            ..Default::default()
        };
    };
    let mut parts = BitPacked::into_parts(bp);
    let num_patches = parts.patches.as_ref().map_or(0, |p| p.num_patches());
    parts.patches = parts
        .patches
        .take()
        .map(|p| vortex_btrblocks::compress_patches(p, &mut ctx).unwrap());
    let vortex::encodings::fastlanes::BitWidths::Global(bw) = parts.bit_widths else {
        unreachable!()
    };
    let arr = BitPacked::try_new(
        parts.packed,
        T::PTYPE,
        parts.validity,
        parts.patches,
        bw,
        parts.len,
        parts.offset,
    )
    .unwrap()
    .into_array();
    let mut decoded = None;
    let decode = time_min(reps, || decoded = Some(arr.clone().execute::<PrimitiveArray>(&mut ctx).unwrap()));
    let failures = usize::from(decoded.unwrap().as_slice::<T>() != v);
    let indices = PrimitiveArray::from_iter(idx.iter().copied()).into_array();
    let take = time_min(reps, || {
        black_box(arr.take(indices.clone()).unwrap().execute::<PrimitiveArray>(&mut ctx).unwrap());
    });
    Stats {
        arrays: 1,
        raw_bytes: size_of_val(v),
        bytes: arr.nbytes() as usize,
        patches: num_patches,
        encode,
        decode,
        take,
        takes: idx.len(),
        failures,
        ..Default::default()
    }
}

fn print_table(title: &str, res: &Results) {
    let base = &res["vortex-v1"];
    println!("\n### {title}");
    println!(
        "arrays={} raw={:.1}MB vortex-v1={:.2}MB",
        base.arrays,
        base.raw_bytes as f64 / 1e6,
        base.bytes as f64 / 1e6
    );
    println!(
        "| config/kernel | size vs v1 | patches | dense chunks | enc MB/s | dec GB/s | unpack GB/s | patch cost | take ns | fails |"
    );
    println!("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|");
    for (name, s) in res {
        let dec = s.raw_bytes as f64 / 1e9 / s.decode.as_secs_f64();
        let unpack = if s.unpack.is_zero() {
            "-".to_string()
        } else {
            format!("{:.2}", s.raw_bytes as f64 / 1e9 / s.unpack.as_secs_f64())
        };
        let cost = if s.unpack.is_zero() {
            "-".to_string()
        } else {
            format!("{:+.1}%", 100.0 * (s.decode.as_secs_f64() / s.unpack.as_secs_f64() - 1.0))
        };
        println!(
            "| {name} | {:+.2}% | {} | {:.1}% | {:.0} | {dec:.2} | {unpack} | {cost} | {:.1} | {} |",
            100.0 * (s.bytes as f64 / base.bytes as f64 - 1.0),
            s.patches,
            100.0 * s.dense_chunks as f64 / s.patched_chunks.max(1) as f64,
            s.raw_bytes as f64 / 1e6 / s.encode.as_secs_f64(),
            s.take.as_nanos() as f64 / s.takes.max(1) as f64,
            s.failures,
        );
    }
}

fn measure_main(files: &[PathBuf]) -> anyhow::Result<()> {
    let tms = TimeModels {
        u8: calibrate::<u8>(),
        u16: calibrate::<u16>(),
        u32: calibrate::<u32>(),
        u64: calibrate::<u64>(),
    };
    for (name, tm) in [("u8", &tms.u8), ("u16", &tms.u16), ("u32", &tms.u32), ("u64", &tms.u64)] {
        eprintln!(
            "calibration {name}: unpack ns/block w=1 {:.0}, w=mid {:.0}, w=max {:.0}; chunk/per ns [byte|bp] list {:.1}/{:.2}|{:.1}/{:.2} bitmap {:.1}/{:.2}|{:.1}/{:.2} lanes {:.1}/{:.2}|{:.1}/{:.2}",
            tm.unpack[1],
            tm.unpack[tm.unpack.len() / 2],
            tm.unpack[tm.unpack.len() - 1],
            tm.chunk[0][0], tm.per[0][0], tm.chunk[1][0], tm.per[1][0],
            tm.chunk[0][1], tm.per[0][1], tm.chunk[1][1], tm.per[1][1],
            tm.chunk[0][2], tm.per[0][2], tm.chunk[1][2], tm.per[1][2],
        );
    }
    let mut overall = Results::new();
    for path in files {
        let name = path.file_stem().unwrap().to_string_lossy().replace('_', ":");
        let started = Instant::now();
        let mut res = Results::new();
        read_cache(path, |input| match &input {
            Input::U8(v) => measure_array(v, &tms, &mut res),
            Input::U16(v) => measure_array(v, &tms, &mut res),
            Input::U32(v) => measure_array(v, &tms, &mut res),
            Input::U64(v) => measure_array(v, &tms, &mut res),
        })?;
        if res.is_empty() {
            println!("\n### {name}: no bit-packed arrays");
            continue;
        }
        print_table(&name, &res);
        if let Ok(total) = std::fs::read_to_string(path.with_extension("total")) {
            println!("TOTAL\t{name}\t{}", total.trim());
        }
        for (k, s) in &res {
            println!(
                "RAW\t{name}\t{k}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                s.arrays,
                s.raw_bytes,
                s.bytes,
                s.patches,
                s.patched_chunks,
                s.dense_chunks,
                s.encode.as_nanos(),
                s.decode.as_nanos(),
                s.unpack.as_nanos(),
                s.take.as_nanos(),
                s.takes,
                s.failures
            );
            overall.entry(k.clone()).or_default().add(s);
        }
        eprintln!("{name}: measured in {:?}", started.elapsed());
    }
    if files.len() > 1 && !overall.is_empty() {
        print_table("ALL", &overall);
    }
    Ok(())
}

/// Isolated timings of the dense-chunk pieces on real encoded blocks.
fn micro() {
    let mut rng = StdRng::seed_from_u64(1);
    let dummy = TimeModel {
        unpack: vec![0.0; 33],
        chunk: [[0.0; 3]; 2],
        per: [[0.0; 3]; 2],
    };
    println!("bmi2={} avx512={}", has_bmi2(), has_avx512());
    for n in [64usize, 256, 512, 1000] {
        let w = 8;
        let hb = 9;
        let mut v: Vec<u32> = (0..N).map(|_| rng.random_range(0..256u32)).collect();
        let mut placed = 0;
        while placed < n {
            let j = rng.random_range(0..N);
            if v[j] < 256 {
                v[j] |= (rng.random_range(0..512u32) | 256) << w;
                placed += 1;
            }
        }
        for high in [HighFmt::Byte, HighFmt::Bp] {
            let c = cfg("m", PosPolicy::List, high, Chooser::Fixed(w, Pos::Bitmap));
            let enc = encode(&v, &c, &dummy);
            assert_eq!(enc.hb[0] as usize, hb);
            let mut tmp = [0u32; N + 16];
            let reps = 200_000;
            let t = Instant::now();
            for _ in 0..reps {
                black_box(&enc).widen(0, &mut tmp[..n]);
                black_box(&tmp);
            }
            let widen = t.elapsed().as_nanos() as f64 / reps as f64;
            let mut out = vec![0u32; N];
            let t = Instant::now();
            for _ in 0..reps {
                unsafe { simd::expand32(black_box(out.as_mut_ptr()), black_box(enc.bitmap(0)), tmp.as_ptr(), w) };
            }
            let expand = t.elapsed().as_nanos() as f64 / reps as f64;
            let t = Instant::now();
            for _ in 0..reps {
                black_box(&enc).or_widened(0, black_box(&mut out), w, &tmp[..n]);
            }
            let scalar_or = t.elapsed().as_nanos() as f64 / reps as f64;
            println!("n={n} {high:?}: widen {widen:.1} ns ({:.2}/patch), expand {expand:.1} ns, scalar OR {scalar_or:.1} ns", widen / n as f64);
        }
    }
}

fn main() -> anyhow::Result<()> {
    vortex_bench::configure_allocator();
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("harvest") => harvest_main(Path::new(&args[1]), &args[2..]),
        Some("micro") => {
            micro();
            Ok(())
        }
        Some("measure") => measure_main(&args[1..].iter().map(PathBuf::from).collect::<Vec<_>>()),
        _ => anyhow::bail!("usage: harvest <cache-dir> <datasets...> | measure <cache files...>"),
    }
}
