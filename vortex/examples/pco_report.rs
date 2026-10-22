// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Compares Pco v1 and v2 against BtrBlocks and FastLanes on synthetic columns and on numeric
//! columns of Parquet files.
//!
//! ```text
//! cargo run --release -p vortex --example pco_report -- [FILE.parquet[:COL,COL,...]]...
//! ```
//!
//! `FILE.parquet:?` lists a file's columns instead. Every encoding compresses each file-sized
//! chunk on its own, as the Vortex file writer does: 1 MiB of values, rounded up to a multiple of
//! 8,192 rows. The encodings are:
//!
//! - `btrblocks`: the default BtrBlocks compressor, which Vortex files use today.
//! - `fastlanes`: BtrBlocks restricted to FoR, ZigZag and bit-packing, plus ALP for floats.
//! - `fastlanes+delta`: the same with FastLanes delta, which BtrBlocks picks when it wins, and
//!   with FoR and bit-packing free to use per-block references and widths. No edition includes
//!   these yet, so Vortex files cannot hold them today.
//! - `btrblocks+pco`: the compact BtrBlocks compressor with the `pco` edition enabled, which
//!   samples every scheme including Pco v2.
//! - `pco v1 8k`: Pco v1 with 8,192-value pages, as the compact compressor writes without the
//!   `pco` edition.
//! - `pco v2`: Pco v2 with one page per chunk, checkpoints every 1,024 values and block stats.
//!
//! Each encoding is timed on a full decode and on every compute function FastLanes bit-packing has
//! a kernel for: `scalar_at`, slice, take, filter, the six comparisons against a constant,
//! between, a widening cast, and `is_constant`. `is_constant` runs with cached statistics cleared,
//! so it is computed every time.
//!
//! By default the report prints one table per column. Set `PCO_VERDICT=1` to instead tally how
//! `pco v2` fares against every other encoding on the ratio and on every operation, or
//! `PCO_SIZES=1` to print the compression ratio of more Pco page sizes and checkpoint intervals.
//! Set `PCO_SKIP_SYNTHETIC=1` to leave out the synthetic columns, and `PCO_TREE=1` to print the
//! encoding tree of every encoding's first chunk.
//!
//! Every operation runs through the same session, so each encoding is measured with the kernels
//! Vortex has for it. Results of every operation are checked against the uncompressed column.

#![expect(clippy::cast_possible_truncation)]
#![expect(clippy::cast_precision_loss)]
#![expect(clippy::print_stdout)]

use std::cmp::Reverse;
use std::fs::File;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use arrow_array::Array as _;
use arrow_array::ArrowPrimitiveType;
use arrow_array::cast::AsArray;
use arrow_array::types::Date32Type;
use arrow_array::types::Float32Type;
use arrow_array::types::Float64Type;
use arrow_array::types::Int16Type;
use arrow_array::types::Int32Type;
use arrow_array::types::Int64Type;
use arrow_array::types::TimestampMicrosecondType;
use arrow_array::types::TimestampMillisecondType;
use arrow_array::types::TimestampNanosecondType;
use arrow_array::types::TimestampSecondType;
use arrow_array::types::UInt16Type;
use arrow_array::types::UInt32Type;
use arrow_array::types::UInt64Type;
use arrow_schema::DataType;
use arrow_schema::TimeUnit;
use mimalloc::MiMalloc;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use vortex::VortexSessionDefault;
use vortex::compressor::BtrBlocksCompressor;
use vortex::compressor::BtrBlocksCompressorBuilder;
use vortex::compressor::CompressionSessionExt;
use vortex::editions::EditionSessionExt;
use vortex_array::ArrayContext;
use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::fns::is_constant::is_constant;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::assert_arrays_eq;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::DType;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::PType;
use vortex_array::expr::stats::Stat;
use vortex_array::match_each_native_ptype;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::between::BetweenOptions;
use vortex_array::scalar_fn::fns::between::StrictComparison;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::serde::SerializeOptions;
use vortex_array::validity::Validity;
use vortex_btrblocks::SchemeExt;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;
use vortex_pco::Pco;
use vortex_pco::PcoOptions;
use vortex_pco::editions::PCO_2026_10;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const LEVEL: usize = PcoOptions::v2().level;
const SCALAR_PROBES: usize = 200;
const TAKE_ROWS: usize = 100;

/// The bytes of values the file writer coalesces into one chunk before compressing it.
const CHUNK_BYTES: usize = 1 << 20;

/// File chunks hold a multiple of this many rows.
const CHUNK_ROW_MULTIPLE: usize = 8192;

/// The FastLanes-family schemes the `fastlanes` encoding may use.
const FASTLANES_SCHEMES: [&str; 4] = [
    "vortex.int.for",
    "vortex.int.zigzag",
    "vortex.int.bitpacking",
    "vortex.float.alp",
];

#[derive(Clone, Copy)]
enum Encoding {
    BtrBlocks,
    FastLanes,
    FastLanesDelta,
    BtrBlocksPco,
    Pco(PcoOptions),
}

/// The encodings every report compares, with `pco v2` last.
const ENCODINGS: [(&str, Encoding); 6] = [
    ("btrblocks", Encoding::BtrBlocks),
    ("fastlanes", Encoding::FastLanes),
    ("fastlanes+delta", Encoding::FastLanesDelta),
    ("btrblocks+pco", Encoding::BtrBlocksPco),
    ("pco v1 8k", Encoding::Pco(PcoOptions::v1(LEVEL, 8192))),
    ("pco v2", Encoding::Pco(PcoOptions::v2())),
];

/// Pco page sizes and checkpoint intervals compared by the `PCO_SIZES` sweep.
const SWEEP: [(&str, PcoOptions); 9] = [
    ("v1 1k", PcoOptions::v1(LEVEL, 1024)),
    ("v1 8k", PcoOptions::v1(LEVEL, 8192)),
    ("v1 chunk", PcoOptions::v1(LEVEL, 0)),
    ("v2 1k fixed", with_interval(1024, 1024, true)),
    ("v2 1k fixed no stats", with_interval(1024, 1024, false)),
    ("v2 1k-8k", with_interval(1024, 8192, true)),
    ("v2 1k-64k", with_interval(1024, 65_536, true)),
    ("v2 4k fixed", with_interval(4096, 4096, true)),
    ("v2 8k fixed", with_interval(8192, 8192, true)),
];

const fn with_interval(
    checkpoint_interval: usize,
    max_checkpoint_interval: usize,
    block_stats: bool,
) -> PcoOptions {
    PcoOptions {
        checkpoint_interval: Some(checkpoint_interval),
        max_checkpoint_interval,
        block_stats,
        ..PcoOptions::v2()
    }
}

/// The compressors behind the BtrBlocks-based encodings.
struct Compressors {
    btrblocks: BtrBlocksCompressor,
    fastlanes: BtrBlocksCompressor,
    fastlanes_delta: BtrBlocksCompressor,
    btrblocks_pco: BtrBlocksCompressor,
}

impl Compressors {
    fn new(session: &VortexSession) -> Self {
        let restricted = |keep: &[&str]| {
            let excluded = session
                .compression()
                .schemes()
                .iter()
                .filter(|scheme| !keep.contains(&scheme.scheme_name()))
                .map(|scheme| scheme.id())
                .collect::<Vec<_>>();
            BtrBlocksCompressorBuilder::from_session(session).exclude_schemes(excluded)
        };
        let mut with_delta = FASTLANES_SCHEMES.to_vec();
        with_delta.push("vortex.int.delta");
        Self {
            btrblocks: BtrBlocksCompressor::from_session(session),
            fastlanes: restricted(&FASTLANES_SCHEMES).build(),
            // No edition has FastLanes delta yet, nor FoR with per-block references or
            // bit-packing with per-block widths, so the session's editions would drop them.
            fastlanes_delta: restricted(&with_delta).disable_editions().build(),
            btrblocks_pco: BtrBlocksCompressorBuilder::from_session(session)
                .with_compact()
                .build(),
        }
    }
}

impl Encoding {
    fn compress_chunk(
        self,
        chunk: &PrimitiveArray,
        compressors: &Compressors,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<ArrayRef> {
        let compressor = match self {
            Self::BtrBlocks => &compressors.btrblocks,
            Self::FastLanes => &compressors.fastlanes,
            Self::FastLanesDelta => &compressors.fastlanes_delta,
            Self::BtrBlocksPco => &compressors.btrblocks_pco,
            Self::Pco(options) => {
                return Ok(Pco::compress(chunk.as_view(), &options, ctx)?.into_array());
            }
        };
        compressor.compress(&chunk.clone().into_array(), ctx)
    }
}

/// An encoded column: its chunks and the chunked array over them.
struct Encoded {
    chunks: Vec<ArrayRef>,
    array: ArrayRef,
}

fn rows_per_chunk(array: &PrimitiveArray) -> usize {
    (CHUNK_BYTES / array.ptype().byte_width()).next_multiple_of(CHUNK_ROW_MULTIPLE)
}

fn compress(
    array: &PrimitiveArray,
    encoding: Encoding,
    compressors: &Compressors,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Encoded> {
    let n = array.len();
    let rows = rows_per_chunk(array);
    let whole = array.clone().into_array();
    let chunks = (0..n)
        .step_by(rows)
        .map(|start| {
            let chunk = whole
                .slice(start..(start + rows).min(n))?
                .execute::<PrimitiveArray>(ctx)?;
            encoding.compress_chunk(&chunk, compressors, ctx)
        })
        .collect::<VortexResult<Vec<_>>>()?;
    let array = ChunkedArray::try_new(chunks.clone(), array.dtype().clone())?.into_array();
    Ok(Encoded { chunks, array })
}

/// Counts the top-level encodings of `encoded`'s chunks, telling Pco v1 and v2 apart.
fn chunk_encodings(encoded: &Encoded) -> String {
    let mut counts: Vec<(String, usize)> = Vec::new();
    for chunk in &encoded.chunks {
        let name = match chunk.as_opt::<Pco>() {
            Some(pco) if pco.data().has_index() => "pco.v2".to_string(),
            Some(_) => "pco".to_string(),
            None => chunk
                .encoding_id()
                .to_string()
                .trim_start_matches("vortex.")
                .to_string(),
        };
        match counts.iter_mut().find(|(seen, _)| *seen == name) {
            Some((_, count)) => *count += 1,
            None => counts.push((name, 1)),
        }
    }
    counts.sort_by_key(|(_, count)| Reverse(*count));
    counts
        .iter()
        .map(|(name, count)| format!("{name}×{count}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Bytes the `pco v2` index takes in `encoded`: checkpoint records and block stats.
fn index_nbytes(encoded: &Encoded) -> (usize, usize) {
    encoded
        .chunks
        .iter()
        .filter_map(|chunk| chunk.as_opt::<Pco>())
        .map(|pco| pco.data().index_nbytes())
        .fold((0, 0), |(c, s), (checkpoints, stats)| {
            (c + checkpoints, s + stats)
        })
}

struct Column {
    name: String,
    array: PrimitiveArray,
}

/// SplitMix64, so the report needs no RNG dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn synthetic(n: usize) -> Vec<Column> {
    let mut rng = Rng(42);
    let mut t = 1_700_000_000_000_000i64;
    let timestamps = PrimitiveArray::from_iter((0..n).map(|_| {
        t += 1_000_000 + rng.below(1000) as i64;
        t
    }));
    let prices = PrimitiveArray::from_iter((0..n).map(|_| rng.below(1_000_000) as f64 / 100.0));
    let mut level = 20.0f64;
    let sensor = PrimitiveArray::from_iter((0..n).map(|_| {
        level += (rng.below(201) as f64 - 100.0) / 1000.0;
        (level * 100.0).round() / 100.0
    }));
    let ids = PrimitiveArray::from_iter((0..n).map(|_| rng.below(1_000_000) as u32));
    vec![
        Column {
            name: "synthetic timestamps i64 (sorted, jitter)".to_string(),
            array: timestamps,
        },
        Column {
            name: "synthetic prices f64 (random cents)".to_string(),
            array: prices,
        },
        Column {
            name: "synthetic sensor f64 (random walk)".to_string(),
            array: sensor,
        },
        Column {
            name: "synthetic ids u32 (uniform)".to_string(),
            array: ids,
        },
    ]
}

fn convert<A: ArrowPrimitiveType>(chunks: &[arrow_array::ArrayRef]) -> PrimitiveArray
where
    A::Native: NativePType,
{
    let values = chunks
        .iter()
        .flat_map(|chunk| chunk.as_primitive::<A>().values().iter().copied())
        .collect::<Buffer<A::Native>>();
    if chunks.iter().all(|chunk| chunk.null_count() == 0) {
        return PrimitiveArray::new(values, Validity::NonNullable);
    }
    let validity = chunks
        .iter()
        .flat_map(|chunk| (0..chunk.len()).map(|i| chunk.is_valid(i)))
        .collect::<BoolArray>();
    PrimitiveArray::new(values, Validity::Array(validity.into_array()))
}

fn load_parquet(spec: &str) -> VortexResult<Vec<Column>> {
    let (path, wanted) = match spec.split_once(':') {
        Some((path, columns)) => (path, Some(columns.split(',').collect::<Vec<_>>())),
        None => (spec, None),
    };
    let file_name = path.rsplit('/').next().unwrap_or(path);
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
        .map_err(|err| vortex_err!("{err}"))?;
    let schema = Arc::clone(builder.schema());
    if wanted.as_deref() == Some(&["?"]) {
        for field in schema.fields() {
            println!("{file_name}:{} {}", field.name(), field.data_type());
        }
        return Ok(Vec::new());
    }
    let indices = schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, field)| {
            wanted
                .as_ref()
                .is_none_or(|w| w.contains(&field.name().as_str()))
        })
        .map(|(i, _)| i)
        .collect::<Vec<_>>();
    let projection = ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
    let reader = builder
        .with_projection(projection)
        .with_batch_size(1 << 16)
        .build()
        .map_err(|err| vortex_err!("{err}"))?;
    let mut chunks = vec![Vec::new(); indices.len()];
    for batch in reader {
        let batch = batch.map_err(|err| vortex_err!("{err}"))?;
        for (column, chunk) in chunks.iter_mut().enumerate() {
            chunk.push(Arc::clone(batch.column(column)));
        }
    }

    let mut columns = Vec::new();
    for (&field_idx, chunks) in indices.iter().zip(&chunks) {
        let field = schema.field(field_idx);
        let array = match field.data_type() {
            DataType::Int16 => convert::<Int16Type>(chunks),
            DataType::Int32 => convert::<Int32Type>(chunks),
            DataType::Int64 => convert::<Int64Type>(chunks),
            DataType::UInt16 => convert::<UInt16Type>(chunks),
            DataType::UInt32 => convert::<UInt32Type>(chunks),
            DataType::UInt64 => convert::<UInt64Type>(chunks),
            DataType::Float32 => convert::<Float32Type>(chunks),
            DataType::Float64 => convert::<Float64Type>(chunks),
            DataType::Date32 => convert::<Date32Type>(chunks),
            DataType::Timestamp(TimeUnit::Second, _) => convert::<TimestampSecondType>(chunks),
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                convert::<TimestampMillisecondType>(chunks)
            }
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                convert::<TimestampMicrosecondType>(chunks)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                convert::<TimestampNanosecondType>(chunks)
            }
            other => {
                println!("skipping {file_name}:{} ({other})", field.name());
                continue;
            }
        };
        columns.push(Column {
            name: format!("{file_name}:{} {}", field.name(), array.dtype()),
            array,
        });
    }
    Ok(columns)
}

fn serialized_bytes(encoded: &Encoded, session: &VortexSession) -> VortexResult<usize> {
    let mut bytes = 0;
    for chunk in &encoded.chunks {
        bytes += chunk
            .serialize(
                &ArrayContext::empty(),
                session,
                &SerializeOptions {
                    offset: 0,
                    include_padding: true,
                },
            )?
            .iter()
            .map(|buffer| buffer.len())
            .sum::<usize>();
    }
    Ok(bytes)
}

fn micros(duration: Duration) -> String {
    let micros = duration.as_secs_f64() * 1e6;
    if micros >= 100.0 {
        format!("{micros:.0}")
    } else {
        format!("{micros:.1}")
    }
}

/// Positions at which slices and clustered filters are timed, so no single position's alignment
/// with page or block boundaries decides the result.
const POSITIONS: usize = 16;

/// The operations each encoding is timed on, built once per column.
struct Workload {
    probe_indices: Vec<usize>,
    take_indices: ArrayRef,
    slices: Vec<Range<usize>>,
    clustered: Vec<Mask>,
    scattered: Mask,
    /// The 1st, 50th, 51st and 99th percentiles of the valid values.
    p1: Scalar,
    p50: Scalar,
    p51: Scalar,
    p99: Scalar,
    /// A stored value.
    existing: Scalar,
    /// The 64-bit type of the same kind, if the column is narrower.
    widened: Option<DType>,
}

fn workload(array: &PrimitiveArray, ctx: &mut ExecutionCtx) -> VortexResult<Workload> {
    let n = array.len();
    let mut rng = Rng(7);
    let probe_indices = (0..SCALAR_PROBES).map(|_| rng.below(n)).collect();
    let take_indices =
        PrimitiveArray::from_iter((0..TAKE_ROWS).map(|_| rng.below(n) as u64)).into_array();
    let slices = (0..POSITIONS)
        .map(|_| {
            let start = rng.below(n - 1024);
            start..start + 1024
        })
        .collect();
    let clustered = (0..POSITIONS)
        .map(|_| {
            let start = rng.below(n - n / 1000);
            Mask::from_slices(n, vec![(start, start + n / 1000)])
        })
        .collect();
    let mut scattered_indices = (0..n / 10_000).map(|_| rng.below(n)).collect::<Vec<_>>();
    scattered_indices.sort_unstable();
    scattered_indices.dedup();
    let scattered = Mask::from_indices(n, scattered_indices);

    let mask = array.validity()?.execute_mask(n, ctx)?;
    let nullability = array.dtype().nullability();
    let ([p1, p50, p51, p99], existing) = match_each_native_ptype!(array.ptype(), |T| {
        let mut valid = array
            .as_slice::<T>()
            .iter()
            .enumerate()
            .filter(|(i, _)| mask.value(*i))
            .map(|(_, v)| *v)
            .collect::<Vec<T>>();
        let existing = valid[rng.below(valid.len())];
        valid.sort_unstable_by(|a, b| a.total_compare(*b));
        (
            [1, 50, 51, 99]
                .map(|percent| Scalar::primitive(valid[valid.len() * percent / 100], nullability)),
            Scalar::primitive(existing, nullability),
        )
    });
    let ptype = array.ptype();
    let widened = match ptype {
        PType::I8 | PType::I16 | PType::I32 => Some(PType::I64),
        PType::U8 | PType::U16 | PType::U32 => Some(PType::U64),
        PType::F16 | PType::F32 => Some(PType::F64),
        PType::I64 | PType::U64 | PType::F64 => None,
    }
    .map(|ptype| DType::Primitive(ptype, nullability));
    Ok(Workload {
        probe_indices,
        take_indices,
        slices,
        clustered,
        scattered,
        p1,
        p50,
        p51,
        p99,
        existing,
        widened,
    })
}

fn constant(scalar: &Scalar, len: usize) -> ArrayRef {
    ConstantArray::new(scalar.clone(), len).into_array()
}

/// The result of one operation of the workload.
enum Output {
    Array(ArrayRef),
    Scalar(Scalar),
    Flag(bool),
}

/// The timed operations: every compute function FastLanes bit-packing has a kernel for, plus a
/// full decode. Every comparison is against a constant.
const OPS: [&str; 15] = [
    "decode",
    "scalar_at",
    "slice 1k",
    "take 100",
    "filter clustered",
    "filter scattered",
    "`< p1`",
    "`<= p1`",
    "`> p99`",
    "`>= p99`",
    "`== v`",
    "`!= v`",
    "between p50–p51",
    "cast to 64-bit",
    "is_constant",
];

const IS_CONSTANT: usize = OPS.len() - 1;

/// Latency of every one of [`OPS`], per single operation, or `None` where it does not apply.
type Metrics = [Option<Duration>; OPS.len()];

/// Clears the statistics `is_constant` reads and caches, throughout `array`'s tree, so that every
/// run computes it.
fn clear_stats(array: &ArrayRef) {
    for stat in [Stat::IsConstant, Stat::Min, Stat::Max] {
        array.statistics().clear(stat);
    }
    for child in array.children() {
        clear_stats(&child);
    }
}

/// Runs operation `op` of the workload once and returns its results, one per operation run.
fn run_op(
    op: usize,
    encoded: &ArrayRef,
    workload: &Workload,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Vec<Output>> {
    let n = encoded.len();
    let execute = |array: VortexResult<ArrayRef>, ctx: &mut ExecutionCtx| {
        VortexResult::Ok(Output::Array(
            array?.execute::<Canonical>(ctx)?.into_array(),
        ))
    };
    let compare = |scalar: &Scalar, operator: Operator, ctx: &mut ExecutionCtx| {
        execute(encoded.binary(constant(scalar, n), operator), ctx)
    };
    Ok(match op {
        0 => vec![execute(Ok(encoded.clone()), ctx)?],
        1 => workload
            .probe_indices
            .iter()
            .map(|&index| Ok(Output::Scalar(encoded.execute_scalar(index, ctx)?)))
            .collect::<VortexResult<_>>()?,
        2 => workload
            .slices
            .iter()
            .map(|range| execute(encoded.slice(range.clone()), ctx))
            .collect::<VortexResult<_>>()?,
        3 => vec![execute(encoded.take(workload.take_indices.clone()), ctx)?],
        4 => workload
            .clustered
            .iter()
            .map(|mask| execute(encoded.filter(mask.clone()), ctx))
            .collect::<VortexResult<_>>()?,
        5 => vec![execute(encoded.filter(workload.scattered.clone()), ctx)?],
        6 => vec![compare(&workload.p1, Operator::Lt, ctx)?],
        7 => vec![compare(&workload.p1, Operator::Lte, ctx)?],
        8 => vec![compare(&workload.p99, Operator::Gt, ctx)?],
        9 => vec![compare(&workload.p99, Operator::Gte, ctx)?],
        10 => vec![compare(&workload.existing, Operator::Eq, ctx)?],
        11 => vec![compare(&workload.existing, Operator::NotEq, ctx)?],
        12 => vec![execute(
            encoded.clone().between(
                constant(&workload.p50, n),
                constant(&workload.p51, n),
                BetweenOptions {
                    lower_strict: StrictComparison::NonStrict,
                    upper_strict: StrictComparison::NonStrict,
                },
            ),
            ctx,
        )?],
        13 => match &workload.widened {
            Some(dtype) => vec![execute(encoded.cast(dtype.clone()), ctx)?],
            None => Vec::new(),
        },
        _ => vec![Output::Flag(is_constant(encoded, ctx)?)],
    })
}

/// Runs every operation on `array` and checks each result against `expected`, if given.
fn run_all(
    array: &ArrayRef,
    workload: &Workload,
    expected: Option<&[Vec<Output>]>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Vec<Vec<Output>>> {
    let mut all = Vec::with_capacity(OPS.len());
    for op in 0..OPS.len() {
        if op == IS_CONSTANT {
            clear_stats(array);
        }
        let outputs = run_op(op, array, workload, ctx)?;
        if let Some(expected) = expected {
            assert_eq!(outputs.len(), expected[op].len(), "{}", OPS[op]);
            for (output, expected) in outputs.iter().zip(&expected[op]) {
                match (output, expected) {
                    (Output::Array(output), Output::Array(expected)) => {
                        assert_arrays_eq!(output, expected, ctx);
                    }
                    (Output::Scalar(output), Output::Scalar(expected)) => {
                        assert_eq!(output, expected, "{}", OPS[op]);
                    }
                    (Output::Flag(output), Output::Flag(expected)) => {
                        assert_eq!(output, expected, "{}", OPS[op]);
                    }
                    _ => unreachable!("{} returned a different kind of result", OPS[op]),
                }
            }
        }
        all.push(outputs);
    }
    Ok(all)
}

/// Times every operation on every encoding `reps` times and keeps the fastest run, the most
/// robust statistic on a noisy machine. Encodings take turns on each repetition of each
/// operation, so a machine slowing down mid-run penalizes them alike.
fn measure(
    encodings: &[&ArrayRef],
    workload: &Workload,
    reps: usize,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Vec<Metrics>> {
    let mut fastest: Vec<Metrics> = vec![[None; OPS.len()]; encodings.len()];
    for op in 0..OPS.len() {
        for _ in 0..reps {
            for (encoding, encoded) in encodings.iter().enumerate() {
                if op == IS_CONSTANT {
                    clear_stats(encoded);
                }
                let start = Instant::now();
                let outputs = run_op(op, encoded, workload, ctx)?;
                let elapsed = start.elapsed();
                // Operations that do not apply to the column return no results.
                let Ok(n_ops @ 1..) = u32::try_from(outputs.len()) else {
                    continue;
                };
                let per_op = elapsed / n_ops;
                let fastest = &mut fastest[encoding][op];
                *fastest = Some(fastest.map_or(per_op, |fastest| fastest.min(per_op)));
            }
        }
    }
    Ok(fastest)
}

/// One column compressed with every one of [`ENCODINGS`], each checked against the uncompressed
/// column and timed.
struct Measured {
    encoded: Vec<Encoded>,
    bytes: Vec<usize>,
    compress: Vec<Duration>,
    metrics: Vec<Metrics>,
}

fn measure_column(
    column: &Column,
    compressors: &Compressors,
    session: &VortexSession,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Measured> {
    let array = &column.array;
    let n = array.len();
    let workload = workload(array, ctx)?;
    let expected = run_all(&array.clone().into_array(), &workload, None, ctx)?;
    let mut encoded = Vec::new();
    let mut bytes = Vec::new();
    let mut compress_times = Vec::new();
    for (_, encoding) in ENCODINGS {
        let start = Instant::now();
        let result = compress(array, encoding, compressors, ctx)?;
        compress_times.push(start.elapsed());
        run_all(&result.array, &workload, Some(&expected), ctx)?;
        bytes.push(serialized_bytes(&result, session)?);
        encoded.push(result);
    }
    let reps = if n > 2_000_000 { 5 } else { 9 };
    let arrays = encoded
        .iter()
        .map(|encoded| &encoded.array)
        .collect::<Vec<_>>();
    let metrics = measure(&arrays, &workload, reps, ctx)?;
    Ok(Measured {
        encoded,
        bytes,
        compress: compress_times,
        metrics,
    })
}

fn report(column: &Column, measured: &Measured) {
    let n = column.array.len();
    let raw_bytes = n * column.array.ptype().byte_width();
    println!(
        "\n### {} — {n} rows, {} rows per chunk\n",
        column.name,
        rows_per_chunk(&column.array)
    );
    print!("| encoding | ratio | bits/value | chunk encodings | compress ms |");
    for op in OPS {
        print!(" {op} µs |");
    }
    println!("\n|---|---|---|---|---|{}", "---|".repeat(OPS.len()));
    for (index, (name, _)) in ENCODINGS.iter().enumerate() {
        let bytes = measured.bytes[index];
        print!(
            "| {name} | {:.2} | {:.2} | {} | {:.0} |",
            raw_bytes as f64 / bytes as f64,
            8.0 * bytes as f64 / n as f64,
            chunk_encodings(&measured.encoded[index]),
            measured.compress[index].as_secs_f64() * 1e3,
        );
        for metric in measured.metrics[index] {
            print!(" {} |", metric.map_or_else(|| "n/a".to_string(), micros));
        }
        println!();
    }
    if std::env::var_os("PCO_TREE").is_some() {
        for ((name, _), encoded) in ENCODINGS.iter().zip(&measured.encoded) {
            println!(
                "\n{name}, first chunk:\n```\n{}```",
                encoded.chunks[0].display_tree()
            );
        }
    }
    let (checkpoints, stats) = index_nbytes(&measured.encoded[ENCODINGS.len() - 1]);
    println!(
        "\nraw size {:.1} MB; the pco v2 index takes {:.3} bits/value of checkpoints and {:.3} of \
         block stats; scalar_at is per one-off probe.",
        raw_bytes as f64 / 1e6,
        8.0 * checkpoints as f64 / n as f64,
        8.0 * stats as f64 / n as f64,
    );
}

/// Prints the compression ratio (raw bytes over serialized bytes) of every [`SWEEP`] variant.
fn size_sweep(
    columns: &[Column],
    compressors: &Compressors,
    session: &VortexSession,
) -> VortexResult<()> {
    let mut ctx = session.create_execution_ctx();
    print!("| column | raw MB |");
    for (name, _) in SWEEP {
        print!(" {name} |");
    }
    println!("\n|---|---|{}", "---|".repeat(SWEEP.len()));
    for column in columns {
        let raw_bytes = column.array.len() * column.array.ptype().byte_width();
        print!("| {} | {:.1} |", column.name, raw_bytes as f64 / 1e6);
        for (_, options) in SWEEP {
            let encoded = compress(&column.array, Encoding::Pco(options), compressors, &mut ctx)?;
            let bytes = serialized_bytes(&encoded, session)?;
            print!(" {:.2} |", raw_bytes as f64 / bytes as f64);
        }
        println!();
    }
    Ok(())
}

/// Speedups within this factor of 1 count as ties, since wall-clock timings here are noisy.
const TIME_TIE: f64 = 1.15;

/// Relative size differences within this fraction count as ties.
const RATIO_TIE: f64 = 0.001;

/// The ratio and then each of [`OPS`].
const METRICS: usize = 1 + OPS.len();

/// How `pco v2` fares against one other encoding on every metric.
#[derive(Clone, Copy, Default)]
struct Tally {
    /// Win, tie, and loss counts.
    scores: [[usize; 3]; METRICS],
    /// Sum of the logarithms of the improvements, for the geometric mean.
    log_sum: [f64; METRICS],
    /// Columns each metric applies to.
    columns: [usize; METRICS],
}

impl Tally {
    fn add(&mut self, improvements: &[Option<f64>; METRICS]) {
        for (metric, &improvement) in improvements.iter().enumerate() {
            let Some(improvement) = improvement else {
                continue;
            };
            let tie = if metric == 0 {
                1.0 + RATIO_TIE
            } else {
                TIME_TIE
            };
            let score = if improvement > tie {
                0
            } else if improvement >= 1.0 / tie {
                1
            } else {
                2
            };
            self.scores[metric][score] += 1;
            self.log_sum[metric] += improvement.ln();
            self.columns[metric] += 1;
        }
    }

    fn geomean(&self, metric: usize) -> f64 {
        (self.log_sum[metric] / self.columns[metric] as f64).exp()
    }
}

/// The improvement of `pco v2` over encoding `other` on every metric that applies, higher is
/// better.
fn improvements(measured: &Measured, other: usize) -> [Option<f64>; METRICS] {
    let v2 = ENCODINGS.len() - 1;
    let mut out = [None; METRICS];
    out[0] = Some(measured.bytes[other] as f64 / measured.bytes[v2] as f64);
    for op in 0..OPS.len() {
        out[1 + op] = measured.metrics[other][op]
            .zip(measured.metrics[v2][op])
            .map(|(other, v2)| other.as_secs_f64() / v2.as_secs_f64().max(1e-9));
    }
    out
}

/// Compares `pco v2` against every other encoding on every column, checking every result against
/// the uncompressed column.
fn verdict(
    columns: &[Column],
    compressors: &Compressors,
    session: &VortexSession,
) -> VortexResult<()> {
    let mut ctx = session.create_execution_ctx();
    let others = ENCODINGS.len() - 1;
    let mut tallies = vec![Tally::default(); others];
    let mut rows = Vec::new();
    for column in columns {
        let array = &column.array;
        let n = array.len();
        let valid = array.validity()?.execute_mask(n, &mut ctx)?.true_count();
        if n < 4096 || valid < 4096 {
            println!("skipping {}: too few valid values", column.name);
            continue;
        }
        let measured = measure_column(column, compressors, session, &mut ctx)?;
        let raw_bytes = (n * array.ptype().byte_width()) as f64;
        let mut row = format!("| {} |", column.name);
        for bytes in &measured.bytes {
            row.push_str(&format!(" {:.2} |", raw_bytes / *bytes as f64));
        }
        for (other, tally) in tallies.iter_mut().enumerate() {
            let improvements = improvements(&measured, other);
            tally.add(&improvements);
            if other == 2 {
                for improvement in &improvements[1..] {
                    match improvement {
                        Some(improvement) => row.push_str(&format!(" {improvement:.1}× |")),
                        None => row.push_str(" n/a |"),
                    }
                }
            }
        }
        row.push_str(&format!(
            " {} |",
            chunk_encodings(&measured.encoded[ENCODINGS.len() - 3])
        ));
        rows.push(row);
        println!("checked {}", column.name);
    }

    println!(
        "\n### Ratio of every encoding, and speedup of pco v2 over {} (higher is better)\n",
        ENCODINGS[2].0
    );
    print!("| column |");
    for (name, _) in ENCODINGS {
        print!(" {name} ratio |");
    }
    for op in OPS {
        print!(" {op} |");
    }
    println!(" {} picks |", ENCODINGS[3].0);
    println!("|---|{}", "---|".repeat(ENCODINGS.len() + OPS.len() + 1));
    for row in &rows {
        println!("{row}");
    }

    println!(
        "\n### pco v2 against each encoding: wins / ties / losses, and geometric-mean improvement\n"
    );
    println!(
        "Ties: ratio within {:.1}%, time within {:.0}%.\n",
        100.0 * RATIO_TIE,
        100.0 * (TIME_TIE - 1.0)
    );
    print!("| metric |");
    for (name, _) in &ENCODINGS[..others] {
        print!(" vs {name} |");
    }
    println!("\n|---|{}", "---|".repeat(others));
    for (metric, name) in std::iter::once("ratio").chain(OPS).enumerate() {
        print!("| {name} |");
        for tally in &tallies {
            let [wins, ties, losses] = tally.scores[metric];
            print!(" {wins}/{ties}/{losses} ({:.2}×) |", tally.geomean(metric));
        }
        println!();
    }
    Ok(())
}

fn main() -> VortexResult<()> {
    let session = VortexSession::default();
    session.enable_edition(PCO_2026_10)?;
    let compressors = Compressors::new(&session);

    let mut columns = if std::env::var_os("PCO_SKIP_SYNTHETIC").is_some() {
        Vec::new()
    } else {
        synthetic(1 << 21)
    };
    for spec in std::env::args().skip(1) {
        columns.extend(load_parquet(&spec)?);
    }
    if std::env::var_os("PCO_SIZES").is_some() {
        return size_sweep(&columns, &compressors, &session);
    }
    if std::env::var_os("PCO_VERDICT").is_some() {
        return verdict(&columns, &compressors, &session);
    }
    let mut ctx = session.create_execution_ctx();
    for column in &columns {
        let measured = measure_column(column, &compressors, &session, &mut ctx)?;
        report(column, &measured);
    }
    Ok(())
}
