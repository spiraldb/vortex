// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Compares Pco at several page sizes against Pco2 (one page per chunk plus checkpoints every
//! 1,024 values) on synthetic columns and on numeric columns of Parquet files.
//!
//! ```text
//! cargo run --release -p vortex-pco --example pco2_report -- [FILE.parquet[:COL,COL,...]]...
//! ```
//!
//! `FILE.parquet:?` lists a file's columns instead. Set `PCO2_SIZES=1` to print only the
//! compression ratio of more Pco page sizes and Pco2 checkpoint intervals, or `PCO2_VERDICT=1`
//! to compare Pco and Pco2 at equal block sizes on every column and tally wins and losses.
//!
//! Every operation runs through the same session, so Pco measures what Vortex does today
//! (decompress, then operate) and Pco2 measures its block-pruning kernels. Results of every
//! operation are checked to match between encodings.

#![expect(clippy::cast_possible_truncation)]
#![expect(clippy::cast_precision_loss)]
#![expect(clippy::print_stdout)]

use std::fs::File;
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
use vortex_array::ArrayContext;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::assert_arrays_eq;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::NativePType;
use vortex_array::match_each_native_ptype;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::serde::SerializeOptions;
use vortex_array::session::ArraySessionExt;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;
use vortex_pco::Pco;
use vortex_pco::pco2::Pco2;
use vortex_pco::pco2::Pco2Options;
use vortex_session::VortexSession;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const LEVEL: usize = pco::DEFAULT_COMPRESSION_LEVEL;
const SCALAR_PROBES: usize = 200;
const TAKE_ROWS: usize = 100;

#[derive(Clone, Copy)]
enum Variant {
    Pco {
        values_per_page: usize,
    },
    Pco2 {
        checkpoint_interval: usize,
        block_stats: bool,
    },
}

const VARIANTS: [(&str, Variant); 4] = [
    ("pco 262k pages", Variant::Pco { values_per_page: 0 }),
    (
        "pco 8k pages",
        Variant::Pco {
            values_per_page: 8192,
        },
    ),
    (
        "pco 1k pages",
        Variant::Pco {
            values_per_page: 1024,
        },
    ),
    (
        "pco2 1k checkpoints",
        Variant::Pco2 {
            checkpoint_interval: 1024,
            block_stats: true,
        },
    ),
];

/// Pco page sizes and Pco2 checkpoint intervals compared by the `PCO2_SIZES` sweep.
const SWEEP: [(&str, Variant); 11] = [
    (
        "v1 256",
        Variant::Pco {
            values_per_page: 256,
        },
    ),
    (
        "v1 1k",
        Variant::Pco {
            values_per_page: 1024,
        },
    ),
    (
        "v1 4k",
        Variant::Pco {
            values_per_page: 4096,
        },
    ),
    (
        "v1 8k",
        Variant::Pco {
            values_per_page: 8192,
        },
    ),
    (
        "v1 64k",
        Variant::Pco {
            values_per_page: 65_536,
        },
    ),
    ("v1 262k", Variant::Pco { values_per_page: 0 }),
    (
        "v2 256",
        Variant::Pco2 {
            checkpoint_interval: 256,
            block_stats: true,
        },
    ),
    (
        "v2 1k",
        Variant::Pco2 {
            checkpoint_interval: 1024,
            block_stats: true,
        },
    ),
    (
        "v2 1k no stats",
        Variant::Pco2 {
            checkpoint_interval: 1024,
            block_stats: false,
        },
    ),
    (
        "v2 4k",
        Variant::Pco2 {
            checkpoint_interval: 4096,
            block_stats: true,
        },
    ),
    (
        "v2 8k",
        Variant::Pco2 {
            checkpoint_interval: 8192,
            block_stats: true,
        },
    ),
];

impl Variant {
    fn compress(self, array: &PrimitiveArray, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        Ok(match self {
            Self::Pco { values_per_page } => {
                Pco::from_primitive(array.as_view(), LEVEL, values_per_page, ctx)?.into_array()
            }
            Self::Pco2 {
                checkpoint_interval,
                block_stats,
            } => Pco2::from_primitive(
                array.as_view(),
                &Pco2Options {
                    checkpoint_interval,
                    block_stats,
                    ..Pco2Options::default()
                },
                ctx,
            )?
            .into_array(),
        })
    }
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

fn serialized_bytes(array: &ArrayRef, session: &VortexSession) -> VortexResult<usize> {
    Ok(array
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
        .sum())
}

/// How [`measure`] summarizes repeated runs.
#[derive(Clone, Copy)]
enum Stat {
    Median,
    /// The fastest run, which is the most robust statistic on a noisy machine.
    Fastest,
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
    slices: Vec<std::ops::Range<usize>>,
    clustered: Vec<Mask>,
    scattered: Mask,
    lt_constant: Scalar,
    eq_constant: Scalar,
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

    // Compare against the 1st percentile (1% selectivity) and an existing value.
    let mask = array.validity()?.execute_mask(n, ctx)?;
    let (lt_constant, eq_constant) = match_each_native_ptype!(array.ptype(), |T| {
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
            Scalar::primitive(valid[valid.len() / 100], array.dtype().nullability()),
            Scalar::primitive(existing, array.dtype().nullability()),
        )
    });
    Ok(Workload {
        probe_indices,
        take_indices,
        slices,
        clustered,
        scattered,
        lt_constant,
        eq_constant,
    })
}

struct Results {
    decoded: PrimitiveArray,
    taken: PrimitiveArray,
    sliced: Vec<PrimitiveArray>,
    clustered: Vec<PrimitiveArray>,
    scattered: PrimitiveArray,
    lt: BoolArray,
    eq: BoolArray,
}

fn run(array: &ArrayRef, workload: &Workload, ctx: &mut ExecutionCtx) -> VortexResult<Results> {
    let constant = |scalar: &Scalar| ConstantArray::new(scalar.clone(), array.len()).into_array();
    Ok(Results {
        decoded: array.clone().execute::<PrimitiveArray>(ctx)?,
        taken: array
            .take(workload.take_indices.clone())?
            .execute::<PrimitiveArray>(ctx)?,
        sliced: workload
            .slices
            .iter()
            .map(|range| array.slice(range.clone())?.execute::<PrimitiveArray>(ctx))
            .collect::<VortexResult<_>>()?,
        clustered: workload
            .clustered
            .iter()
            .map(|mask| array.filter(mask.clone())?.execute::<PrimitiveArray>(ctx))
            .collect::<VortexResult<_>>()?,
        scattered: array
            .filter(workload.scattered.clone())?
            .execute::<PrimitiveArray>(ctx)?,
        lt: array
            .binary(constant(&workload.lt_constant), Operator::Lt)?
            .execute::<BoolArray>(ctx)?,
        eq: array
            .binary(constant(&workload.eq_constant), Operator::Eq)?
            .execute::<BoolArray>(ctx)?,
    })
}

fn check(results: &Results, expected: &Results, ctx: &mut ExecutionCtx) {
    assert_arrays_eq!(results.decoded, expected.decoded, ctx);
    assert_arrays_eq!(results.taken, expected.taken, ctx);
    for (sliced, expected) in results.sliced.iter().zip(&expected.sliced) {
        assert_arrays_eq!(sliced, expected, ctx);
    }
    for (clustered, expected) in results.clustered.iter().zip(&expected.clustered) {
        assert_arrays_eq!(clustered, expected, ctx);
    }
    assert_arrays_eq!(results.scattered, expected.scattered, ctx);
    assert_arrays_eq!(results.lt, expected.lt, ctx);
    assert_arrays_eq!(results.eq, expected.eq, ctx);
}

/// The timed operations, in the order [`Metrics`] holds them.
const OPS: [&str; 8] = [
    "decode",
    "scalar_at",
    "slice 1k",
    "take 100",
    "filter clustered",
    "filter scattered",
    "`< p1`",
    "`== v`",
];

/// Latency of every one of [`OPS`], per single operation.
type Metrics = [Duration; OPS.len()];

/// Runs operation `op` of the workload once and returns how many operations that was.
fn run_op(
    op: usize,
    encoded: &ArrayRef,
    workload: &Workload,
    ctx: &mut ExecutionCtx,
) -> VortexResult<u32> {
    let constant = |scalar: &Scalar| ConstantArray::new(scalar.clone(), encoded.len()).into_array();
    match op {
        0 => {
            encoded.clone().execute::<PrimitiveArray>(ctx)?;
            Ok(1)
        }
        1 => {
            for &index in &workload.probe_indices {
                encoded.execute_scalar(index, ctx)?;
            }
            Ok(SCALAR_PROBES as u32)
        }
        2 => {
            for range in &workload.slices {
                encoded
                    .slice(range.clone())?
                    .execute::<PrimitiveArray>(ctx)?;
            }
            Ok(POSITIONS as u32)
        }
        3 => {
            encoded
                .take(workload.take_indices.clone())?
                .execute::<PrimitiveArray>(ctx)?;
            Ok(1)
        }
        4 => {
            for mask in &workload.clustered {
                encoded
                    .filter(mask.clone())?
                    .execute::<PrimitiveArray>(ctx)?;
            }
            Ok(POSITIONS as u32)
        }
        5 => {
            encoded
                .filter(workload.scattered.clone())?
                .execute::<PrimitiveArray>(ctx)?;
            Ok(1)
        }
        6 => {
            encoded
                .binary(constant(&workload.lt_constant), Operator::Lt)?
                .execute::<BoolArray>(ctx)?;
            Ok(1)
        }
        _ => {
            encoded
                .binary(constant(&workload.eq_constant), Operator::Eq)?
                .execute::<BoolArray>(ctx)?;
            Ok(1)
        }
    }
}

/// Times every operation on every encoding `reps` times. Encodings take turns on each repetition
/// of each operation, so a machine slowing down mid-run penalizes them alike.
fn measure(
    encodings: &[&ArrayRef],
    workload: &Workload,
    reps: usize,
    stat: Stat,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Vec<Metrics>> {
    let mut times = vec![vec![Vec::with_capacity(reps); OPS.len()]; encodings.len()];
    for op in 0..OPS.len() {
        for _ in 0..reps {
            for (encoding, encoded) in encodings.iter().enumerate() {
                let start = Instant::now();
                let n_ops = run_op(op, encoded, workload, ctx)?;
                times[encoding][op].push(start.elapsed() / n_ops);
            }
        }
    }
    Ok(times
        .into_iter()
        .map(|per_op| {
            let mut metrics = [Duration::ZERO; OPS.len()];
            for (metric, mut samples) in metrics.iter_mut().zip(per_op) {
                samples.sort();
                *metric = match stat {
                    Stat::Median => samples[samples.len() / 2],
                    Stat::Fastest => samples[0],
                };
            }
            metrics
        })
        .collect())
}

fn report(column: &Column, session: &VortexSession) -> VortexResult<()> {
    let mut ctx = session.create_execution_ctx();
    let array = &column.array;
    let n = array.len();
    let raw_bytes = n * array.ptype().byte_width();
    let workload = workload(array, &mut ctx)?;
    let reps = if n > 2_000_000 { 3 } else { 7 };

    println!("\n### {} — {n} rows\n", column.name);
    println!(
        "| encoding | bits/value | size vs 262k | index bits/value | compress ms | decode ms | \
         scalar_at µs | slice 1k µs | take {TAKE_ROWS} µs | filter 0.1% clustered µs | \
         filter 0.01% scattered µs | `< p1` µs | `== v` µs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|");

    let mut baseline_bytes = None;
    let mut reference: Option<Results> = None;
    for (name, variant) in VARIANTS {
        let start = Instant::now();
        let encoded = variant.compress(array, &mut ctx)?;
        let compress = start.elapsed();
        let bytes = serialized_bytes(&encoded, session)?;
        let index = match encoded.as_opt::<Pco2>() {
            Some(pco2) => {
                let (checkpoints, stats) = pco2.data().index_nbytes();
                format!(
                    "{:.3} + {:.3} stats",
                    8.0 * checkpoints as f64 / n as f64,
                    8.0 * stats as f64 / n as f64
                )
            }
            None => "-".to_string(),
        };
        let baseline = *baseline_bytes.get_or_insert(bytes);

        let results = run(&encoded, &workload, &mut ctx)?;
        match &reference {
            None => {
                assert_arrays_eq!(results.decoded, array, &mut ctx);
                reference = Some(results);
            }
            Some(expected) => check(&results, expected, &mut ctx),
        }
        let [decode, scalar_at, slice, take, clustered, scattered, lt, eq] =
            measure(&[&encoded], &workload, reps, Stat::Median, &mut ctx)?[0];

        println!(
            "| {name} | {:.2} | {:+.2}% | {index} | {:.0} | {:.2} | {} | {} | {} | {} | {} | {} | {} |",
            8.0 * bytes as f64 / n as f64,
            100.0 * (bytes as f64 / baseline as f64 - 1.0),
            compress.as_secs_f64() * 1e3,
            decode.as_secs_f64() * 1e3,
            micros(scalar_at),
            micros(slice),
            micros(take),
            micros(clustered),
            micros(scattered),
            micros(lt),
            micros(eq),
        );
    }
    println!(
        "\nraw size {:.1} MB; scalar_at is per one-off probe; filters select rows of every \
         variant identically.",
        raw_bytes as f64 / 1e6
    );
    Ok(())
}

/// Prints the compression ratio (raw bytes over serialized bytes) of every [`SWEEP`] variant.
fn size_sweep(columns: &[Column], session: &VortexSession) -> VortexResult<()> {
    let mut ctx = session.create_execution_ctx();
    print!("| column | raw MB |");
    for (name, _) in SWEEP {
        print!(" {name} |");
    }
    println!("\n|---|---|{}", "---|".repeat(SWEEP.len()));
    for column in columns {
        let raw_bytes = column.array.len() * column.array.ptype().byte_width();
        print!("| {} | {:.1} |", column.name, raw_bytes as f64 / 1e6);
        for (_, variant) in SWEEP {
            let encoded = variant.compress(&column.array, &mut ctx)?;
            let bytes = serialized_bytes(&encoded, session)?;
            print!(" {:.2} |", raw_bytes as f64 / bytes as f64);
        }
        println!();
    }
    Ok(())
}

/// Block sizes at which the `PCO2_VERDICT` mode compares Pco against Pco2.
const VERDICT_BLOCKS: [usize; 2] = [1024, 8192];

/// Speedups within this factor of 1 count as ties, since wall-clock timings here are noisy.
const TIME_TIE: f64 = 1.15;

/// Relative size differences within this fraction count as ties.
const RATIO_TIE: f64 = 0.001;

/// Win, tie, and loss counts of Pco2 against Pco for the ratio and then each of [`OPS`].
type Tally = [[usize; 3]; 1 + OPS.len()];

/// Scores `v2 / v1` for a higher-is-better quantity: 0 win, 1 tie, 2 loss.
fn score(improvement: f64, tie: f64) -> usize {
    if improvement > tie {
        0
    } else if improvement >= 1.0 / tie {
        1
    } else {
        2
    }
}

/// Compares Pco and Pco2 at equal block sizes on every column, checking every result against
/// the uncompressed array.
fn verdict(columns: &[Column], session: &VortexSession) -> VortexResult<()> {
    let mut ctx = session.create_execution_ctx();
    let mut tallies = [[[0usize; 3]; 1 + OPS.len()]; VERDICT_BLOCKS.len()];
    let mut rows = vec![Vec::new(); VERDICT_BLOCKS.len()];
    for column in columns {
        let array = &column.array;
        let n = array.len();
        let valid = array.validity()?.execute_mask(n, &mut ctx)?.true_count();
        if n < 4096 || valid < 4096 {
            println!("skipping {}: too few valid values", column.name);
            continue;
        }
        let raw_bytes = n * array.ptype().byte_width();
        let workload = workload(array, &mut ctx)?;
        let expected = run(&array.clone().into_array(), &workload, &mut ctx)?;
        let reps = if n > 2_000_000 { 5 } else { 9 };

        for (block_idx, &block) in VERDICT_BLOCKS.iter().enumerate() {
            let mut encodings = Vec::new();
            let mut ratios = Vec::new();
            for variant in [
                Variant::Pco {
                    values_per_page: block,
                },
                Variant::Pco2 {
                    checkpoint_interval: block,
                    block_stats: true,
                },
            ] {
                let encoded = variant.compress(array, &mut ctx)?;
                check(&run(&encoded, &workload, &mut ctx)?, &expected, &mut ctx);
                ratios.push(raw_bytes as f64 / serialized_bytes(&encoded, session)? as f64);
                encodings.push(encoded);
            }
            let (v1_ratio, v2_ratio) = (ratios[0], ratios[1]);
            let metrics = measure(
                &[&encodings[0], &encodings[1]],
                &workload,
                reps,
                Stat::Fastest,
                &mut ctx,
            )?;
            let improvements = std::iter::once(v2_ratio / v1_ratio)
                .chain(
                    metrics[0]
                        .iter()
                        .zip(&metrics[1])
                        .map(|(v1, v2)| v1.as_secs_f64() / v2.as_secs_f64()),
                )
                .collect::<Vec<_>>();
            let mut row = format!("| {} | {v1_ratio:.2} | {v2_ratio:.2} |", column.name);
            for (metric, &improvement) in improvements.iter().enumerate() {
                let tie = if metric == 0 {
                    1.0 + RATIO_TIE
                } else {
                    TIME_TIE
                };
                tallies[block_idx][metric][score(improvement, tie)] += 1;
                if metric > 0 {
                    row.push_str(&format!(" {improvement:.1}× |"));
                }
            }
            rows[block_idx].push(row);
        }
        println!("checked {}", column.name);
    }

    for (block_idx, block) in VERDICT_BLOCKS.iter().enumerate() {
        println!("\n### {block}-value blocks: speedup of v2 over v1 (higher is better)\n");
        print!("| column | v1 ratio | v2 ratio |");
        for op in OPS {
            print!(" {op} |");
        }
        println!("\n|---|---|---|{}", "---|".repeat(OPS.len()));
        for row in &rows[block_idx] {
            println!("{row}");
        }
        print_tally(*block, &tallies[block_idx]);
    }
    Ok(())
}

fn print_tally(block: usize, tally: &Tally) {
    println!(
        "\n{block}-value blocks, v2 against v1 (ties: ratio within {:.1}%, time within {:.0}%)\n",
        100.0 * RATIO_TIE,
        100.0 * (TIME_TIE - 1.0)
    );
    println!("| metric | v2 wins | tie | v2 loses |");
    println!("|---|---|---|---|");
    for (metric, [wins, ties, losses]) in std::iter::once("ratio").chain(OPS).zip(tally) {
        println!("| {metric} | {wins} | {ties} | {losses} |");
    }
}

fn main() -> VortexResult<()> {
    let session = vortex_array::array_session();
    session.arrays().register(Pco);
    vortex_pco::pco2::initialize(&session);

    let mut columns = if std::env::var_os("PCO2_SKIP_SYNTHETIC").is_some() {
        Vec::new()
    } else {
        synthetic(1 << 21)
    };
    for spec in std::env::args().skip(1) {
        columns.extend(load_parquet(&spec)?);
    }
    if std::env::var_os("PCO2_SIZES").is_some() {
        return size_sweep(&columns, &session);
    }
    if std::env::var_os("PCO2_VERDICT").is_some() {
        return verdict(&columns, &session);
    }
    for column in &columns {
        report(column, &session)?;
    }
    Ok(())
}
