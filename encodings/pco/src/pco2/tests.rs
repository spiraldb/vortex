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
use vortex_array::scalar_fn::fns::binary::CompareKernel;
use vortex_array::scalar_fn::fns::operators::CompareOperator;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::serde::SerializeOptions;
use vortex_array::serde::SerializedArray;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;

use super::BlockDecoder;
use super::Pco2;
use super::Pco2Array;
use super::Pco2Options;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    super::initialize(&session);
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

fn halfs() -> PrimitiveArray {
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

fn options() -> Pco2Options {
    // Small chunks so every dataset spans several chunks and partial blocks.
    Pco2Options {
        values_per_chunk: 7_000,
        ..Pco2Options::default()
    }
}

fn compress(array: &PrimitiveArray, ctx: &mut ExecutionCtx) -> VortexResult<Pco2Array> {
    Pco2::from_primitive(array.as_view(), &options(), ctx)
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
        ("halfs", halfs()),
        ("repeating_u32", repeating_u32()),
    ]
}

/// Every checkpoint block, and every run of three blocks, decodes to the original values.
#[rstest]
fn every_checkpoint_resumes_decoding(
    #[values(256, 1024, 4096)] checkpoint_interval: usize,
) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let options = Pco2Options {
        checkpoint_interval,
        ..options()
    };
    for (name, array) in datasets() {
        let pco2 = Pco2::from_primitive(array.as_view(), &options, &mut ctx)?;
        let data = pco2.data();
        let interval = data.interval();
        let mut n_checkpoints = 0;
        vortex_array::match_each_native_ptype!(array.ptype(), |T| {
            let expected = array.as_slice::<T>();
            let mut decoder = BlockDecoder::<T>::new(data)?;
            for (page, entry) in data.index.iter().enumerate() {
                let n_blocks = data.blocks_in_page(entry);
                n_checkpoints += n_blocks - 1;
                for start in 0..n_blocks {
                    for run in [1, 3] {
                        let blocks = start..(start + run).min(n_blocks);
                        let values = entry.values.start + blocks.start * interval
                            ..(entry.values.start + blocks.end * interval).min(entry.values.end);
                        let decoded = decoder.decode_page_blocks_scratch(page, blocks.clone())?;
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
            // Lookback chunks fall back to pages no longer than the checkpoint interval.
            assert_eq!(n_checkpoints, 0, "{name} should not be checkpointed");
        } else {
            assert!(n_checkpoints > 0, "{name} should be checkpointed");
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
        for n in [1, 17, 3000] {
            let indices = random_indices(&mut rng, n, sliced.len()).into_array();
            let taken = <Pco2 as TakeExecute>::take(sliced.as_::<Pco2>(), &indices, &mut ctx)?
                .expect("Pco2 take kernel applies");
            assert_arrays_eq!(taken, expected_sliced.take(indices.clone())?, &mut ctx);
        }
        let nullable_indices =
            PrimitiveArray::from_option_iter([Some(3u64), None, Some(18_000), Some(0)])
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

        // A clustered selection touches few blocks, so the kernel gathers.
        let clustered = Mask::from_iter((0..len).map(|i| (4000..4100).contains(&i) || i == 9000));
        let filtered = <Pco2 as FilterKernel>::filter(sliced.as_::<Pco2>(), &clustered, &mut ctx)?
            .expect("Pco2 filter kernel applies to clustered masks");
        assert_arrays_eq!(filtered, expected_sliced.filter(clustered)?, &mut ctx);

        // A scattered selection touches every block, so the kernel declines.
        let scattered = Mask::from_iter((0..len).map(|_| rng.below(100) == 0));
        assert!(
            <Pco2 as FilterKernel>::filter(sliced.as_::<Pco2>(), &scattered, &mut ctx)?.is_none()
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
                    <Pco2 as CompareKernel>::compare(sliced.as_::<Pco2>(), &rhs, op, &mut ctx)?;
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

#[rstest]
#[case(timestamps())]
#[case(prices_f64())]
#[case(with_nulls(small_i32(), 1))]
#[case(halfs())]
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
    let pco2 = Pco2::from_primitive(
        array.as_view(),
        &Pco2Options {
            block_stats,
            ..options()
        },
        &mut ctx,
    )?
    .into_array();
    let decoded = serialize_round_trip(pco2)?;
    assert!(decoded.is::<Pco2>());
    assert_arrays_eq!(decoded, array, &mut ctx);
    assert_arrays_eq!(
        decoded.slice(5000..5100)?,
        array.slice(5000..5100)?,
        &mut ctx
    );
    Ok(())
}

#[test]
fn default_options_use_one_page_per_chunk() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let array = PrimitiveArray::from_iter((0..600_000u64).map(|i| i * i % 1_000_003));
    let pco2 = Pco2::from_primitive(array.as_view(), &Pco2Options::default(), &mut ctx)?;
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
    for checkpoint_interval in [0, 100, 1000] {
        let options = Pco2Options {
            checkpoint_interval,
            ..options()
        };
        assert!(Pco2::from_primitive(array.as_view(), &options, &mut ctx).is_err());
    }
}
