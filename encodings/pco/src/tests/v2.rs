// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors
#![expect(clippy::cast_possible_truncation)]
#![expect(clippy::cast_precision_loss)]

use std::sync::LazyLock;

use rstest::rstest;
use vortex_array::ArrayContext;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::aggregate_fn::Accumulator;
use vortex_array::aggregate_fn::DynAccumulator;
use vortex_array::aggregate_fn::EmptyOptions;
use vortex_array::aggregate_fn::fns::is_constant::IsConstant;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::dict::TakeExecute;
use vortex_array::arrays::filter::FilterKernel;
use vortex_array::assert_arrays_eq;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::compute::conformance::consistency::test_array_consistency;
use vortex_array::dtype::NativePType;
use vortex_array::dtype::half::f16;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::between::BetweenKernel;
use vortex_array::scalar_fn::fns::between::BetweenOptions;
use vortex_array::scalar_fn::fns::between::StrictComparison;
use vortex_array::scalar_fn::fns::binary::CompareKernel;
use vortex_array::scalar_fn::fns::operators::CompareOperator;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::serde::SerializeOptions;
use vortex_array::serde::SerializedArray;
use vortex_array::session::ArraySessionExt;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;

use crate::Pco;
use crate::PcoArray;
use crate::PcoArrayExt;
use crate::PcoData;
use crate::PcoOptions;
use crate::array::BlockDecoder;
use crate::pco_v1_id;
use crate::pco_v2_id;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    crate::initialize(&session);
    session
});

/// SplitMix64, so tests need no RNG dependency.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const N: usize = 20_000;

fn timestamps() -> PrimitiveArray {
    let mut rng = Rng(1);
    let mut t = 1_700_000_000_000_000i64;
    PrimitiveArray::from_iter((0..N).map(|_| {
        t += 1_000 + rng.below(50) as i64;
        t
    }))
}

fn arithmetic() -> PrimitiveArray {
    PrimitiveArray::from_iter((0..N as i64).map(|i| 5 + 3 * i))
}

fn uniform_u32() -> PrimitiveArray {
    let mut rng = Rng(2);
    PrimitiveArray::from_iter((0..N).map(|_| rng.next() as u32))
}

fn small_i32() -> PrimitiveArray {
    let mut rng = Rng(3);
    PrimitiveArray::from_iter((0..N).map(|_| rng.below(201) as i32 - 100))
}

fn prices_f64() -> PrimitiveArray {
    let mut rng = Rng(4);
    PrimitiveArray::from_iter((0..N).map(|_| rng.below(100_000) as f64 / 100.0))
}

fn smooth_f64() -> PrimitiveArray {
    PrimitiveArray::from_iter((0..N).map(|i| (i as f64 / 300.0).sin() * 1000.0))
}

fn uniform_f32() -> PrimitiveArray {
    let mut rng = Rng(5);
    PrimitiveArray::from_iter((0..N).map(|_| (rng.next() >> 40) as f32 / 1e3 - 5e3))
}

fn low_cardinality_u16() -> PrimitiveArray {
    let mut rng = Rng(6);
    PrimitiveArray::from_iter((0..N).map(|_| [7u16, 300, 301, 9000][rng.below(4) as usize]))
}

fn multiples_u64() -> PrimitiveArray {
    let mut rng = Rng(7);
    PrimitiveArray::from_iter((0..N).map(|_| 1_000_003 * rng.below(1 << 20) + rng.below(3)))
}

fn sorted_i16() -> PrimitiveArray {
    PrimitiveArray::from_iter((0..N).map(|i| (i / 3) as i16 - 3000))
}

fn f16s() -> PrimitiveArray {
    let mut rng = Rng(8);
    PrimitiveArray::from_iter((0..N).map(|_| f16::from_f32(rng.below(4096) as f32 / 16.0)))
}

/// Repeats a random period, which pco's automatic delta selection prefers to encode with lookback.
fn repeating_u32() -> PrimitiveArray {
    let mut rng = Rng(9);
    let period = (0..100).map(|_| rng.next() as u32).collect::<Vec<_>>();
    PrimitiveArray::from_iter((0..N).map(|i| period[i % period.len()]))
}

fn with_nulls(array: PrimitiveArray, seed: u64) -> PrimitiveArray {
    let mut rng = Rng(seed);
    let validity = (0..array.len())
        .map(|_| rng.below(10) >= 3)
        .collect::<BoolArray>();
    PrimitiveArray::from_buffer_handle(
        array.buffer_handle().clone(),
        array.ptype(),
        Validity::Array(validity.into_array()),
    )
}

/// v2 with checkpoints every 1,024 values in every chunk, so the tests see small blocks.
fn options() -> PcoOptions {
    PcoOptions {
        max_checkpoint_interval: 1024,
        ..PcoOptions::v2()
    }
}

/// Compresses with small chunks, so every dataset spans several chunks and partial blocks.
fn compress_with(
    array: &PrimitiveArray,
    options: &PcoOptions,
    ctx: &mut ExecutionCtx,
) -> VortexResult<PcoArray> {
    let data = PcoData::compress_with_values_per_chunk(array.as_view(), options, 7_000, ctx)?;
    Pco::try_new(array.dtype().clone(), data, array.validity()?)
}

fn compress(array: &PrimitiveArray, ctx: &mut ExecutionCtx) -> VortexResult<PcoArray> {
    compress_with(array, &options(), ctx)
}

fn datasets() -> Vec<(&'static str, PrimitiveArray)> {
    vec![
        ("timestamps", timestamps()),
        ("arithmetic", arithmetic()),
        ("uniform_u32", uniform_u32()),
        ("small_i32", small_i32()),
        ("prices_f64", prices_f64()),
        ("smooth_f64", smooth_f64()),
        ("uniform_f32", uniform_f32()),
        ("low_cardinality_u16", low_cardinality_u16()),
        ("multiples_u64", multiples_u64()),
        ("sorted_i16", sorted_i16()),
        ("f16s", f16s()),
        ("repeating_u32", repeating_u32()),
    ]
}

/// Every checkpoint block, and every run of three blocks, decodes to the original values.
#[rstest]
fn every_checkpoint_resumes_decoding(
    #[values(256, 1024, 4096)] checkpoint_interval: usize,
) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let options = PcoOptions {
        checkpoint_interval: Some(checkpoint_interval),
        max_checkpoint_interval: checkpoint_interval,
        ..options()
    };
    for (name, array) in datasets() {
        let pco2 = compress_with(&array, &options, &mut ctx)?;
        let data = pco2.data();
        let mut n_checkpoints = 0;
        vortex_array::match_each_native_ptype!(array.ptype(), |T| {
            let expected = array.as_slice::<T>();
            let mut decoder = BlockDecoder::<T>::new(data, false)?;
            for (page, entry) in data.entries.iter().enumerate() {
                let n_blocks = entry.n_blocks();
                n_checkpoints += n_blocks - 1;
                for start in 0..n_blocks {
                    for run in [1, 3] {
                        let blocks = start..(start + run).min(n_blocks);
                        let page_values = entry.block_values(blocks.clone());
                        let values = entry.values.start + page_values.start
                            ..entry.values.start + page_values.end;
                        let mut decoded = BufferMut::<T>::empty();
                        decoder.append_page_blocks(page, blocks.clone(), &mut decoded)?;
                        assert!(
                            decoded
                                .iter()
                                .zip(&expected[values.clone()])
                                .all(|(a, b)| a.is_eq(*b)),
                            "{name}: page {page} blocks {blocks:?} decoded incorrectly"
                        );
                    }
                }
            }
        });
        if name == "repeating_u32" {
            // Lookback chunks fall back to pages of the longest block, which need no checkpoints.
            assert_eq!(n_checkpoints, 0, "{name} should not be checkpointed");
        } else {
            assert!(n_checkpoints > 0, "{name} should be checkpointed");
        }
    }
    Ok(())
}

/// Reads that start and stop anywhere in a page, including mid-block and mid-batch, decode only
/// the values asked for.
#[test]
fn partial_reads_match() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    for (name, array) in datasets() {
        let pco2 = compress(&array, &mut ctx)?;
        vortex_array::match_each_native_ptype!(array.ptype(), |T| {
            check_partial_reads::<T>(pco2.data(), array.as_slice::<T>(), name)?;
        });
    }
    Ok(())
}

fn check_partial_reads<T: pco::data_types::Number + NativePType>(
    data: &PcoData,
    expected: &[T],
    name: &str,
) -> VortexResult<()> {
    let mut decoder = BlockDecoder::<T>::new(data, true)?;
    for (page, entry) in data.entries.iter().enumerate() {
        let page_n = entry.values.len();
        for start in (0..page_n).step_by(997) {
            for len in [1, 255, 256, 257, 1500] {
                let local = start..(start + len).min(page_n);
                let mut decoded = BufferMut::<T>::empty();
                decoder.append_page_values(page, local.clone(), &mut decoded)?;
                let values = entry.values.start + local.start..entry.values.start + local.end;
                assert!(
                    decoded.len() == values.len()
                        && decoded
                            .iter()
                            .zip(&expected[values])
                            .all(|(a, b)| a.is_eq(*b)),
                    "{name}: page {page} values {local:?} decoded incorrectly"
                );
            }
        }
    }
    Ok(())
}

#[rstest]
#[case::non_null(false)]
#[case::nullable(true)]
fn round_trip_and_slices(#[case] nullable: bool) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let mut rng = Rng(10);
    for (seed, (_, array)) in datasets().into_iter().enumerate() {
        let array = if nullable {
            with_nulls(array, seed as u64)
        } else {
            array
        };
        let pco2 = compress(&array, &mut ctx)?;
        assert_arrays_eq!(pco2, array, &mut ctx);
        for _ in 0..8 {
            let start = rng.below(N as u64) as usize;
            let end = start + rng.below((N - start) as u64 + 1) as usize;
            assert_arrays_eq!(pco2.slice(start..end)?, array.slice(start..end)?, &mut ctx);
        }
        let nested = pco2.slice(1000..15_000)?.slice(2049..9000)?;
        assert_arrays_eq!(nested, array.slice(3049..10_000)?, &mut ctx);
    }
    Ok(())
}

#[rstest]
#[case::non_null(false)]
#[case::nullable(true)]
fn repeated_slice_decodes_match(#[case] nullable: bool) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    for (seed, (_, array)) in datasets().into_iter().enumerate() {
        let array = if nullable {
            with_nulls(array, seed as u64)
        } else {
            array
        };
        let pco2 = compress(&array, &mut ctx)?;
        for range in [500..9000, 500..9000, 7000..15_000, 500..9000, 0..N] {
            let decoded = pco2
                .slice(range.clone())?
                .execute::<PrimitiveArray>(&mut ctx)?;
            assert_arrays_eq!(decoded, array.slice(range)?, &mut ctx);
        }
    }
    Ok(())
}

#[rstest]
#[case::non_null(false)]
#[case::nullable(true)]
fn scalar_at_matches(#[case] nullable: bool) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let mut rng = Rng(11);
    for (seed, (_, array)) in datasets().into_iter().enumerate() {
        let array = if nullable {
            with_nulls(array, seed as u64)
        } else {
            array
        };
        let pco2 = compress(&array, &mut ctx)?.into_array();
        let sliced = pco2.slice(777..N)?;
        let expected_sliced = array.slice(777..N)?;
        for _ in 0..20 {
            let index = rng.below(N as u64 - 777) as usize;
            assert_eq!(
                pco2.execute_scalar(index, &mut ctx)?,
                array.execute_scalar(index, &mut ctx)?
            );
            assert_eq!(
                sliced.execute_scalar(index, &mut ctx)?,
                expected_sliced.execute_scalar(index, &mut ctx)?
            );
        }
    }
    Ok(())
}

fn random_indices(rng: &mut Rng, n: usize, len: usize) -> PrimitiveArray {
    PrimitiveArray::from_iter((0..n).map(|_| rng.below(len as u64) as u32))
}

#[rstest]
#[case::non_null(false)]
#[case::nullable(true)]
fn take_decodes_touched_blocks(#[case] nullable: bool) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let mut rng = Rng(12);
    for (seed, (_, array)) in datasets().into_iter().enumerate() {
        let array = if nullable {
            with_nulls(array, seed as u64)
        } else {
            array
        };
        let pco2 = compress(&array, &mut ctx)?;
        let sliced = pco2.slice(500..19_000)?;
        let expected_sliced = array.slice(500..19_000)?;
        for n in [0, 1, 17, 3000] {
            let indices = random_indices(&mut rng, n, sliced.len()).into_array();
            let taken = <Pco as TakeExecute>::take(sliced.as_::<Pco>(), &indices, &mut ctx)?
                .expect("Pco take kernel applies");
            assert_arrays_eq!(taken, expected_sliced.take(indices.clone())?, &mut ctx);
        }
        let nullable_indices = PrimitiveArray::from_option_iter([
            Some(3u64),
            None,
            Some(18_000),
            Some(0),
            Some(3),
            Some(524),
            Some(523),
        ])
        .into_array();
        assert_arrays_eq!(
            sliced.take(nullable_indices.clone())?,
            expected_sliced.take(nullable_indices)?,
            &mut ctx
        );
    }
    Ok(())
}

#[rstest]
#[case::non_null(false)]
#[case::nullable(true)]
fn filter_decodes_touched_blocks(#[case] nullable: bool) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let mut rng = Rng(13);
    for (seed, (_, array)) in datasets().into_iter().enumerate() {
        let array = if nullable {
            with_nulls(array, seed as u64)
        } else {
            array
        };
        let pco2 = compress(&array, &mut ctx)?;
        let sliced = pco2.slice(300..N)?;
        let expected_sliced = array.slice(300..N)?;
        let len = sliced.len();

        // Clustered runs touch few blocks, so the kernel decodes each run as a range.
        let clustered = Mask::from_iter((0..len).map(|i| (4000..4100).contains(&i) || i == 9000));
        let filtered = <Pco as FilterKernel>::filter(sliced.as_::<Pco>(), &clustered, &mut ctx)?
            .expect("Pco filter kernel applies to clustered masks");
        assert_arrays_eq!(filtered, expected_sliced.filter(clustered)?, &mut ctx);

        // Many short runs inside few blocks, so the kernel gathers value by value.
        let interleaved =
            Mask::from_iter((0..len).map(|i| (6000..6900).contains(&i) && i % 3 == 0));
        let filtered = <Pco as FilterKernel>::filter(sliced.as_::<Pco>(), &interleaved, &mut ctx)?
            .expect("Pco filter kernel applies to interleaved masks");
        assert_arrays_eq!(filtered, expected_sliced.filter(interleaved)?, &mut ctx);

        // A scattered selection touches every block, so the kernel declines.
        let scattered = Mask::from_iter((0..len).map(|_| rng.below(100) == 0));
        assert!(
            <Pco as FilterKernel>::filter(sliced.as_::<Pco>(), &scattered, &mut ctx)?.is_none()
        );
        assert_arrays_eq!(
            sliced.filter(scattered.clone())?,
            expected_sliced.filter(scattered)?,
            &mut ctx
        );
    }
    Ok(())
}

const OPERATORS: [CompareOperator; 6] = [
    CompareOperator::Eq,
    CompareOperator::NotEq,
    CompareOperator::Lt,
    CompareOperator::Lte,
    CompareOperator::Gt,
    CompareOperator::Gte,
];

#[rstest]
#[case::non_null(false)]
#[case::nullable(true)]
fn compare_prunes_blocks(#[case] nullable: bool) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let mut rng = Rng(14);
    for (seed, (name, array)) in datasets().into_iter().enumerate() {
        let array = if nullable {
            with_nulls(array, seed as u64)
        } else {
            array
        };
        let pco2 = compress(&array, &mut ctx)?;
        let sliced = pco2.slice(123..N - 77)?;
        let expected_sliced = array.slice(123..N - 77)?;
        let len = sliced.len();
        // Probe values at, between, and beyond the stored ones.
        let mut constants = (0..4)
            .map(|_| {
                let index = rng.below(N as u64) as usize;
                array.execute_scalar(index, &mut ctx)
            })
            .collect::<VortexResult<Vec<Scalar>>>()?;
        constants.retain(|constant| !constant.is_null());
        let ptype = array.ptype();
        vortex_array::match_each_native_ptype!(ptype, |T| {
            let values = array.as_slice::<T>();
            let min = values
                .iter()
                .copied()
                .fold(values[0], |a, b| if b.is_lt(a) { b } else { a });
            let max = values
                .iter()
                .copied()
                .fold(values[0], |a, b| if b.is_gt(a) { b } else { a });
            constants.push(Scalar::primitive(min, false.into()));
            constants.push(Scalar::primitive(max, false.into()));
        });

        let sorted = matches!(name, "timestamps" | "arithmetic" | "sorted_i16");
        for constant in constants {
            let rhs = ConstantArray::new(constant.cast(&ptype.into())?, len).into_array();
            for op in OPERATORS {
                let expected = expected_sliced.binary(rhs.clone(), Operator::from(op))?;
                let compared =
                    <Pco as CompareKernel>::compare(sliced.as_::<Pco>(), &rhs, op, &mut ctx)?;
                // Sorted data leaves at most a block or two straddling any constant.
                assert!(
                    compared.is_some() || !sorted,
                    "{name}: compare kernel declined"
                );
                if let Some(compared) = compared {
                    assert_arrays_eq!(compared, expected.clone(), &mut ctx);
                }
                assert_arrays_eq!(
                    sliced.binary(rhs.clone(), Operator::from(op))?,
                    expected,
                    &mut ctx
                );
            }
        }
    }
    Ok(())
}

const STRICTNESS: [StrictComparison; 2] = [StrictComparison::Strict, StrictComparison::NonStrict];

#[rstest]
#[case::non_null(false)]
#[case::nullable(true)]
fn between_prunes_blocks(#[case] nullable: bool) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let mut rng = Rng(15);
    for (seed, (name, array)) in datasets().into_iter().enumerate() {
        let array = if nullable {
            with_nulls(array, seed as u64)
        } else {
            array
        };
        let pco2 = compress(&array, &mut ctx)?;
        let sliced = pco2.slice(123..N - 77)?;
        let expected_sliced = array.slice(123..N - 77)?;
        let len = sliced.len();
        let ptype = array.ptype();
        let sorted = matches!(name, "timestamps" | "arithmetic" | "sorted_i16");
        for _ in 0..3 {
            let a = array.execute_scalar(rng.below(N as u64) as usize, &mut ctx)?;
            let b = array.execute_scalar(rng.below(N as u64) as usize, &mut ctx)?;
            if a.is_null() || b.is_null() {
                continue;
            }
            // Both orders, so one range is usually empty.
            for (lo, hi) in [(&a, &b), (&b, &a)] {
                let lower = ConstantArray::new(lo.cast(&ptype.into())?, len).into_array();
                let upper = ConstantArray::new(hi.cast(&ptype.into())?, len).into_array();
                for (lower_strict, upper_strict) in STRICTNESS
                    .into_iter()
                    .flat_map(|l| STRICTNESS.map(|u| (l, u)))
                {
                    let options = BetweenOptions {
                        lower_strict,
                        upper_strict,
                    };
                    let expected = expected_sliced.clone().between(
                        lower.clone(),
                        upper.clone(),
                        options.clone(),
                    )?;
                    let result = <Pco as BetweenKernel>::between(
                        sliced.as_::<Pco>(),
                        &lower,
                        &upper,
                        &options,
                        &mut ctx,
                    )?;
                    // Sorted data leaves at most a block or two straddling each bound.
                    assert!(
                        result.is_some() || !sorted,
                        "{name}: between kernel declined"
                    );
                    if let Some(result) = result {
                        assert_arrays_eq!(result, expected.clone(), &mut ctx);
                    }
                    assert_arrays_eq!(
                        sliced
                            .clone()
                            .between(lower.clone(), upper.clone(), options)?,
                        expected,
                        &mut ctx
                    );
                }
            }
        }
    }
    Ok(())
}

/// Accumulates `is_constant` as chunked arrays do, which hands every batch to the kernels.
fn accumulate_is_constant(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<Scalar> {
    let mut accumulator = Accumulator::try_new(IsConstant, EmptyOptions, array.dtype().clone())?;
    accumulator.accumulate(array, ctx)?;
    accumulator.finish()
}

/// `values` with `values[i]` replaced by `other` at each of `positions`.
fn with_values_at<T: NativePType>(values: &[T], other: T, positions: &[usize]) -> PrimitiveArray {
    let mut values = values.to_vec();
    for &position in positions {
        values[position] = other;
    }
    PrimitiveArray::new(Buffer::from(values), Validity::NonNullable)
}

#[test]
fn is_constant_matches() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let constant = vec![5i64; N];
    let zeros = vec![0.0f64; N];
    let mut arrays = vec![
        PrimitiveArray::new(Buffer::from(constant.clone()), Validity::NonNullable),
        with_values_at(&constant, 6, &[0]),
        with_values_at(&constant, 6, &[1500]),
        with_values_at(&constant, 6, &[N - 1]),
        with_values_at(&constant, 6, &[1500, 9000]),
        // Bitwise distinct, though equal as numbers.
        with_values_at(&zeros, -0.0, &[3000]),
        with_nulls(
            PrimitiveArray::new(Buffer::from(constant), Validity::NonNullable),
            3,
        ),
        PrimitiveArray::new(Buffer::from(vec![7u16; N]), Validity::AllInvalid),
    ];
    arrays.extend(datasets().into_iter().map(|(_, array)| array));
    // Whole arrays, slices cutting through blocks around the odd values, and slices short enough
    // to be all null.
    let slices = [
        0..N,
        0..1500,
        1501..N,
        1400..1600,
        1024..2048,
        1000..3000,
        2999..3001,
        123..N - 77,
        N - 2..N,
        4000..4001,
        4000..4003,
    ];
    for array in &arrays {
        let encodings = [
            compress(array, &mut ctx)?.into_array(),
            Pco::from_primitive(array.as_view(), 8, 1024, &mut ctx)?.into_array(),
        ];
        for range in slices.clone() {
            let expected = accumulate_is_constant(&array.slice(range.clone())?, &mut ctx)?;
            for encoded in &encodings {
                assert_eq!(
                    accumulate_is_constant(&encoded.slice(range.clone())?, &mut ctx)?,
                    expected,
                    "{} {range:?}",
                    encoded.encoding_id(),
                );
            }
        }
    }
    Ok(())
}

#[rstest]
#[case(timestamps())]
#[case(prices_f64())]
#[case(with_nulls(small_i32(), 1))]
#[case(f16s())]
fn consistency(#[case] array: PrimitiveArray) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let pco2 = compress(&array, &mut ctx)?.into_array();
    test_array_consistency(&pco2, &mut ctx);
    Ok(())
}

fn serialize_round_trip(array: ArrayRef) -> VortexResult<ArrayRef> {
    let context = ArrayContext::empty();
    let bytes = array
        .serialize(
            &context,
            &SESSION,
            &SerializeOptions {
                offset: 0,
                include_padding: true,
            },
        )?
        .into_iter()
        .flat_map(|buffer| buffer.into_iter())
        .collect::<BufferMut<u8>>()
        .freeze();
    SerializedArray::try_from(bytes)?.decode(
        array.dtype(),
        array.len(),
        &ReadContext::new(context.to_ids()),
        &SESSION,
    )
}

#[rstest]
#[case(timestamps(), true)]
#[case(with_nulls(prices_f64(), 2), true)]
#[case(uniform_u32(), false)]
fn serde_round_trip(#[case] array: PrimitiveArray, #[case] block_stats: bool) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let pco2 = compress_with(
        &array,
        &PcoOptions {
            block_stats,
            ..options()
        },
        &mut ctx,
    )?
    .into_array();
    let decoded = serialize_round_trip(pco2)?;
    assert!(decoded.as_::<Pco>().has_index());
    assert_arrays_eq!(decoded, array, &mut ctx);
    assert_arrays_eq!(
        decoded.slice(5000..5100)?,
        array.slice(5000..5100)?,
        &mut ctx
    );
    Ok(())
}

/// Stats are kept where blocks narrow the value range and left out where every block spans it.
#[test]
fn block_stats_only_where_they_prune() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let has_stats = |array: &PrimitiveArray, ctx: &mut ExecutionCtx| -> VortexResult<bool> {
        Ok(compress(array, ctx)?
            .data()
            .index
            .as_ref()
            .is_some_and(|index| index.block_stats.is_some()))
    };
    assert!(has_stats(&timestamps(), &mut ctx)?);
    assert!(has_stats(&sorted_i16(), &mut ctx)?);
    assert!(has_stats(
        &PrimitiveArray::from_iter([7u32; 5000]),
        &mut ctx
    )?);
    assert!(!has_stats(&uniform_u32(), &mut ctx)?);
    assert!(!has_stats(&prices_f64(), &mut ctx)?);
    Ok(())
}

/// Near-constant chunks space their checkpoints out up to the maximum interval, while chunks of
/// varied values keep the requested one.
#[test]
fn intervals_adapt_to_index_cost() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let intervals = |array: &PrimitiveArray, ctx: &mut ExecutionCtx| -> VortexResult<Vec<u32>> {
        let pco2 = Pco::compress(array.as_view(), &PcoOptions::v2(), ctx)?;
        assert_arrays_eq!(pco2, array, ctx);
        Ok(pco2
            .data()
            .index
            .as_ref()
            .map(|index| index.checkpoint_intervals.clone())
            .unwrap_or_default())
    };
    let mut rng = Rng(21);
    let varied = PrimitiveArray::from_iter((0..100_000).map(|_| rng.below(1 << 20) as u32));
    assert_eq!(intervals(&varied, &mut ctx)?, vec![1024]);
    let near_constant =
        PrimitiveArray::from_iter((0..100_000u32).map(|i| u32::from(i % 50_000 == 7)));
    assert_eq!(intervals(&near_constant, &mut ctx)?, vec![8192]);
    Ok(())
}

#[test]
fn default_options_use_one_page_per_chunk() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let array = PrimitiveArray::from_iter((0..600_000u64).map(|i| i * i % 1_000_003));
    let pco2 = Pco::compress(array.as_view(), &PcoOptions::v2(), &mut ctx)?;
    let chunks = &pco2.metadata.chunks;
    assert_eq!(chunks.len(), 3);
    assert!(chunks.iter().all(|chunk| chunk.pages.len() == 1));
    assert_eq!(chunks[0].pages[0].n_values, 262_144);
    assert_arrays_eq!(
        pco2.slice(262_000..263_000)?,
        array.slice(262_000..263_000)?,
        &mut ctx
    );
    Ok(())
}

#[test]
fn empty_and_all_null() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let empty = PrimitiveArray::new(Buffer::<i32>::empty(), Validity::NonNullable);
    assert_arrays_eq!(compress(&empty, &mut ctx)?, empty, &mut ctx);

    let all_null = PrimitiveArray::new(Buffer::from(vec![0i64; 3000]), Validity::AllInvalid);
    let pco2 = compress(&all_null, &mut ctx)?;
    assert_arrays_eq!(pco2, all_null, &mut ctx);
    let rhs = ConstantArray::new(Scalar::primitive(0i64, false.into()), 3000).into_array();
    assert_arrays_eq!(
        pco2.into_array().binary(rhs.clone(), Operator::Eq)?,
        all_null.into_array().binary(rhs, Operator::Eq)?,
        &mut ctx
    );
    Ok(())
}

#[test]
fn rejects_invalid_interval() {
    let mut ctx = SESSION.create_execution_ctx();
    let array = timestamps();
    for checkpoint_interval in [0, 100, 1000, 2048] {
        let options = PcoOptions {
            checkpoint_interval: Some(checkpoint_interval),
            ..options()
        };
        assert!(Pco::compress(array.as_view(), &options, &mut ctx).is_err());
    }
    let stats_without_checkpoints = PcoOptions {
        block_stats: true,
        ..PcoOptions::v1(8, 0)
    };
    assert!(Pco::compress(array.as_view(), &stats_without_checkpoints, &mut ctx).is_err());
}

fn serialized_id(array: &ArrayRef) -> VortexResult<vortex_array::ArrayId> {
    let context = ArrayContext::empty();
    array.serialize(&context, &SESSION, &SerializeOptions::default())?;
    let ids = context.to_ids();
    Ok(ids[0])
}

/// Indexed arrays write v2; arrays without an index, including an indexed array whose index is
/// dropped, write the frozen v1 format.
#[test]
fn writer_picks_wire_format() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let array = timestamps();
    let v1 = Pco::from_primitive(array.as_view(), 8, 0, &mut ctx)?;
    let v2 = compress(&array, &mut ctx)?;
    assert_eq!(serialized_id(&v1.into_array())?, pco_v1_id());
    assert_eq!(serialized_id(&v2.clone().into_array())?, pco_v2_id());

    let downgraded = Pco::try_new(
        v2.dtype().clone(),
        v2.data().without_index(),
        v2.unsliced_validity(),
    )?
    .into_array();
    assert_eq!(serialized_id(&downgraded)?, pco_v1_id());
    assert_arrays_eq!(serialize_round_trip(downgraded)?, array, &mut ctx);
    Ok(())
}

/// A reader that registers only the frozen v1 format reads downgraded v2 data and rejects v2.
#[test]
fn v1_reader_reads_downgraded_v2() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let array = with_nulls(prices_f64(), 3);
    let v2 = compress(&array, &mut ctx)?;
    let v1_session = vortex_array::array_session();
    v1_session.arrays().register(Pco);

    let read_with = |array: ArrayRef, session: &VortexSession| -> VortexResult<ArrayRef> {
        let context = ArrayContext::empty();
        let bytes = array
            .serialize(&context, &SESSION, &SerializeOptions::default())?
            .into_iter()
            .flat_map(|buffer| buffer.into_iter())
            .collect::<BufferMut<u8>>()
            .freeze();
        SerializedArray::try_from(bytes)?.decode(
            array.dtype(),
            array.len(),
            &ReadContext::new(context.to_ids()),
            session,
        )
    };
    let downgraded = Pco::try_new(
        v2.dtype().clone(),
        v2.data().without_index(),
        v2.unsliced_validity(),
    )?;
    assert_arrays_eq!(
        read_with(downgraded.into_array(), &v1_session)?,
        array,
        &mut ctx
    );
    assert!(read_with(v2.into_array(), &v1_session).is_err());
    Ok(())
}

/// v1 arrays have no checkpoints, so their pages are their blocks; the kernels still decode
/// only the pages an operation touches.
#[test]
fn v1_pages_are_blocks() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let array = timestamps();
    let v1 = Pco::from_primitive(array.as_view(), 8, 1024, &mut ctx)?;
    let len = array.len();
    let mask = Mask::from_iter((0..len).map(|i| (5000..5300).contains(&i)));
    let filtered = <Pco as FilterKernel>::filter(v1.as_view(), &mask, &mut ctx)?
        .expect("filter kernel applies to v1 pages");
    assert_arrays_eq!(filtered, array.filter(mask)?, &mut ctx);
    let indices = PrimitiveArray::from_iter([3u32, 19_999, 7000, 7001]).into_array();
    let taken = <Pco as TakeExecute>::take(v1.as_view(), &indices, &mut ctx)?
        .expect("take kernel applies to v1 pages");
    assert_arrays_eq!(taken, array.take(indices)?, &mut ctx);
    // Without block stats, compare falls back to decoding.
    let rhs = ConstantArray::new(Scalar::primitive(0i64, false.into()), len).into_array();
    assert!(
        <Pco as CompareKernel>::compare(v1.as_view(), &rhs, CompareOperator::Lt, &mut ctx)?
            .is_none()
    );
    Ok(())
}
