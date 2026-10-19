// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::hash::DefaultHasher;
use std::hash::Hasher;
use std::slice;
use std::sync::LazyLock;

use num_traits::AsPrimitive;
use prost::Message;
use rand::RngExt;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rstest::rstest;
use vortex_array::Array;
use vortex_array::ArrayContext;
use vortex_array::ArrayEq;
use vortex_array::ArrayHash;
use vortex_array::ArrayParts;
use vortex_array::ArrayRef;
use vortex_array::EqMode;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::Primitive;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::assert_arrays_eq;
use vortex_array::buffer::BufferHandle;
use vortex_array::compute::conformance::consistency::test_array_consistency;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability::NonNullable;
use vortex_array::dtype::PType;
use vortex_array::expr::stats::Precision;
use vortex_array::expr::stats::Stat;
use vortex_array::expr::stats::StatsProviderExt;
use vortex_array::match_each_unsigned_integer_ptype;
use vortex_array::serde::SerializeOptions;
use vortex_array::serde::SerializedArray;
use vortex_array::test_harness::check_metadata;
use vortex_array::validity::Validity;
use vortex_array::vtable::VTable;
use vortex_buffer::BitBuffer;
use vortex_buffer::ByteBufferMut;
use vortex_buffer::buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_session::VortexSession;
use vortex_session::registry::ReadContext;

use super::Unary;
use super::UnaryArray;
use super::array::UnaryMetadata;
use super::unary_encode;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    crate::initialize(&session);
    session
});

/// `values` as a primitive array of `ptype`, which must hold every one of them.
fn primitive(ptype: PType, values: &[u64]) -> PrimitiveArray {
    match_each_unsigned_integer_ptype!(ptype, |T| {
        PrimitiveArray::from_iter(values.iter().map(|&value| AsPrimitive::<T>::as_(value)))
    })
}

fn encode_as(ptype: PType, values: &[u64]) -> VortexResult<Option<UnaryArray>> {
    let array = primitive(ptype, values);
    unary_encode(array.as_ref().as_::<Primitive>())
}

fn encode(values: &[u64]) -> VortexResult<UnaryArray> {
    encode_as(PType::U64, values)?
        .ok_or_else(|| vortex_err!("{} values did not encode", values.len()))
}

/// `len` sorted values drawn from `0..=span`, with duplicates wherever they fall.
fn sorted(len: usize, span: u64, seed: u64) -> Vec<u64> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut values: Vec<u64> = (0..len).map(|_| rng.random_range(0..=span)).collect();
    values.sort_unstable();
    values
}

/// Every value read back through `execute_scalar`, in a shuffled order and then in sequence.
fn check_access(array: &ArrayRef, expected: &PrimitiveArray, seed: u64) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let mut order: Vec<usize> = (0..array.len()).collect();
    order.shuffle(&mut StdRng::seed_from_u64(seed));
    for index in order.into_iter().chain(0..array.len()) {
        assert_eq!(
            array.execute_scalar(index, &mut ctx)?,
            expected.execute_scalar(index, &mut ctx)?,
            "index {index}"
        );
    }
    Ok(())
}

fn serde_roundtrip(array: &ArrayRef) -> VortexResult<ArrayRef> {
    let array_ctx = ArrayContext::empty();
    let mut concat = ByteBufferMut::empty();
    for buffer in array.serialize(&array_ctx, &SESSION, &SerializeOptions::default())? {
        concat.extend_from_slice(buffer.as_ref());
    }
    SerializedArray::try_from(concat.freeze())?.decode(
        array.dtype(),
        array.len(),
        &ReadContext::new(array_ctx.to_ids()),
        &SESSION,
    )
}

fn hash_of(array: &ArrayRef) -> u64 {
    let mut hasher = DefaultHasher::new();
    array.array_hash(&mut hasher, EqMode::Value);
    hasher.finish()
}

#[test]
fn test_layout_example() -> VortexResult<()> {
    let array = encode(&[3, 5, 5, 9])?;
    assert_eq!(array.base(), 3);
    assert_eq!(array.max(), 9);
    assert_eq!(
        array.bits(),
        &BitBuffer::from(vec![
            true, false, false, true, true, false, false, false, false, true
        ])
    );
    Ok(())
}

#[rstest]
#[case::empty(PType::U64, vec![])]
#[case::single(PType::U64, vec![42])]
#[case::all_equal(PType::U64, vec![7; 300])]
#[case::consecutive(PType::U64, (0..300).collect())]
#[case::dense(PType::U64, sorted(500, 500, 1))]
#[case::spread(PType::U64, sorted(500, 1000, 2))]
#[case::runs(PType::U64, (0..500).map(|i| i / 64 * 3).collect())]
#[case::near_max(
    PType::U64,
    sorted(500, 1000, 3).into_iter().map(|v| u64::MAX - 1000 + v).collect()
)]
#[case::u8(PType::U8, sorted(300, 255, 4))]
#[case::u16(PType::U16, (0..500).map(|i| 60_000 + 2 * i).collect())]
#[case::u32(
    PType::U32,
    sorted(500, 1000, 5).into_iter().map(|v| u64::from(u32::MAX) - 1000 + v).collect()
)]
fn test_roundtrip(#[case] ptype: PType, #[case] values: Vec<u64>) -> VortexResult<()> {
    let expected = primitive(ptype, &values);
    let encoded = encode_as(ptype, &values)?
        .ok_or_else(|| vortex_err!("{} values did not encode", values.len()))?
        .into_array();
    assert_eq!(encoded.dtype(), &DType::Primitive(ptype, NonNullable));
    assert_arrays_eq!(encoded, expected, &mut SESSION.create_execution_ctx());
    check_access(&encoded, &expected, 0x5EED)
}

#[test]
fn test_encode_returns_none() -> VortexResult<()> {
    let inputs = [
        PrimitiveArray::from_iter([3u64, 1, 2]),
        PrimitiveArray::new(buffer![1u64, 2, 3], Validity::AllValid),
        PrimitiveArray::from_iter([1i64, 2, 3]),
        PrimitiveArray::from_iter([1.0f64, 2.0, 3.0]),
        // 2 + 1,000 bits, against the plain array's 128.
        PrimitiveArray::from_iter([0u64, 1000]),
    ];
    for input in inputs {
        assert!(
            unary_encode(input.as_ref().as_::<Primitive>())?.is_none(),
            "{} encoded",
            input.dtype()
        );
    }
    Ok(())
}

#[rstest]
#[case(PType::U8)]
#[case(PType::U16)]
#[case(PType::U32)]
#[case(PType::U64)]
fn test_size_rule_boundary(#[case] ptype: PType) -> VortexResult<()> {
    // Four values take `4 + span` bits, against the plain array's `4 × width`.
    let span = 4 * u64::try_from(ptype.bit_width())? - 4;
    assert!(encode_as(ptype, &[0, 0, 0, span])?.is_some());
    assert!(encode_as(ptype, &[0, 0, 0, span + 1])?.is_none());
    Ok(())
}

#[test]
fn test_try_new_rejects() -> VortexResult<()> {
    let bits = |bits: &[bool]| BitBuffer::from(bits);
    let rejected = [
        (
            "base above max",
            Unary::try_new(bits(&[true]), 5, 3, PType::U64, 1),
        ),
        (
            "max above the ptype",
            Unary::try_new(bits(&[true]), 0, 300, PType::U8, 1),
        ),
        ("signed", Unary::try_new(bits(&[true]), 0, 0, PType::I64, 1)),
        (
            "nonzero offset",
            Unary::try_new(
                bits(&[false, false, false, true]).slice(3..),
                0,
                0,
                PType::U64,
                1,
            ),
        ),
        (
            "wrong bit length",
            Unary::try_new(bits(&[true, true]), 0, 0, PType::U64, 1),
        ),
        (
            "first bit unset",
            Unary::try_new(bits(&[false, true, true]), 0, 1, PType::U64, 2),
        ),
        (
            "last bit unset",
            Unary::try_new(bits(&[true, true, false]), 0, 1, PType::U64, 2),
        ),
        (
            "empty with a span",
            Unary::try_new(BitBuffer::new_unset(3), 0, 3, PType::U64, 0),
        ),
        // `[0, 0, 1]` takes the bits `11 01`, and `[0, 2]` takes `1 001`. Clearing or setting bit 1
        // keeps both ends set, so only the count of set bits catches them.
        (
            "a set bit missing",
            Unary::try_new(bits(&[true, false, false, true]), 0, 1, PType::U64, 3),
        ),
        (
            "an extra set bit",
            Unary::try_new(bits(&[true, true, false, true]), 0, 2, PType::U64, 2),
        ),
    ];
    for (case, result) in rejected {
        assert!(result.is_err(), "{case} was accepted");
    }

    // The vtable refuses children however the parts are put together.
    let array = encode(&[1, 2])?;
    let child = primitive(PType::U64, &[1]).into_array();
    assert!(
        Array::try_from_parts(ArrayParts::new(
            Unary,
            array.dtype().clone(),
            array.len(),
            array.data().clone(),
            [Some(child)].into_iter().collect(),
        ))
        .is_err()
    );
    Ok(())
}

#[test]
fn test_deserialize_rejects() {
    let dtype = DType::Primitive(PType::U64, NonNullable);
    let metadata = |base: u64, max: u64| UnaryMetadata { base, max }.encode_to_vec();
    // `[0, 1]` takes the bits `1 01`.
    let bits = BufferHandle::new_host(BitBuffer::from(vec![true, false, true]).inner().clone());
    let no_children: [ArrayRef; 0] = [];
    let deserialize = |metadata: &[u8], buffers: &[BufferHandle], children: &[ArrayRef]| {
        Unary.deserialize(&dtype, 2, metadata, buffers, &children, &SESSION)
    };

    let one = slice::from_ref(&bits);

    assert!(deserialize(&metadata(0, 1), one, &no_children).is_ok());
    assert!(deserialize(&metadata(0, 1), &[], &no_children).is_err());
    assert!(deserialize(&metadata(0, 1), &[bits.clone(), bits.clone()], &no_children).is_err());
    assert!(
        deserialize(
            &metadata(0, 1),
            one,
            &[primitive(PType::U64, &[1]).into_array()]
        )
        .is_err()
    );
    // The buffer holds 8 bits, short of the 102 these claim.
    assert!(deserialize(&metadata(0, 100), one, &no_children).is_err());
    // 2 + `u64::MAX` bits overflow the count.
    assert!(deserialize(&metadata(0, u64::MAX), one, &no_children).is_err());
    // `1 11` keeps both ends set, but holds three set bits for two values.
    let extra = BufferHandle::new_host(BitBuffer::from(vec![true, true, true]).inner().clone());
    assert!(deserialize(&metadata(0, 1), slice::from_ref(&extra), &no_children).is_err());
}

#[test]
fn test_serde_roundtrip() -> VortexResult<()> {
    let values = sorted(1000, 1500, 6);
    let decoded = serde_roundtrip(&encode(&values)?.into_array())?;
    assert!(decoded.is::<Unary>());
    assert_arrays_eq!(
        decoded,
        primitive(PType::U64, &values),
        &mut SESSION.create_execution_ctx()
    );
    Ok(())
}

#[test]
fn test_with_buffers_roundtrip() -> VortexResult<()> {
    let array = encode(&sorted(1000, 1500, 7))?.into_array();
    let buffers = array.buffer_handles();
    // SAFETY: the replacement buffers are the array's own, so they hold the same values.
    let rebuilt = unsafe { array.clone().with_buffers(buffers) }?;
    assert!(rebuilt.is::<Unary>());
    assert!(rebuilt.array_eq(&array, EqMode::Value));
    Ok(())
}

#[test]
fn test_array_eq_hash() -> VortexResult<()> {
    let values: Vec<u64> = (0..500).map(|i| 2 * i).collect();
    let array = encode(&values)?.into_array();
    let same = encode(&values)?.into_array();
    assert!(array.array_eq(&same, EqMode::Value));
    assert_eq!(hash_of(&array), hash_of(&same));

    let shifted: Vec<u64> = values.iter().map(|value| value + 1).collect();
    // The same base and max, with different bits.
    let mut moved = values;
    moved[1] = 3;
    for other in [encode(&shifted)?, encode(&moved)?] {
        assert!(!array.array_eq(&other.into_array(), EqMode::Value));
    }
    Ok(())
}

#[test]
fn test_nbytes() -> VortexResult<()> {
    let values = sorted(1000, 1500, 9);
    let span = values[999] - values[0];
    assert_eq!(
        encode(&values)?.into_array().nbytes(),
        (1000 + span).div_ceil(8)
    );
    Ok(())
}

#[cfg_attr(miri, ignore)]
#[test]
fn test_unary_metadata() {
    check_metadata(
        "unary.metadata",
        &UnaryMetadata {
            base: u64::MAX,
            max: u64::MAX,
        }
        .encode_to_vec(),
    );
}

#[rstest]
#[case::dense(sorted(1000, 1000, 10))]
#[case::spread(sorted(1000, 2000, 11))]
#[case::runs((0..1000).map(|i| i / 64).collect())]
#[case::single(vec![5])]
fn test_unary_consistency(#[case] values: Vec<u64>) -> VortexResult<()> {
    let array = encode(&values)?.into_array();
    let mut ctx = SESSION.create_execution_ctx();
    test_array_consistency(&array, &mut ctx);
    if values.len() > 2 {
        test_array_consistency(&array.slice(1..values.len() - 1)?, &mut ctx);
    }
    Ok(())
}

#[test]
fn test_is_sorted_stat() -> VortexResult<()> {
    for values in [vec![], vec![4], sorted(500, 500, 12)] {
        let array = encode(&values)?.into_array();
        assert_eq!(
            array
                .statistics()
                .with_typed_stats_set(|stats| stats.get_as::<bool>(Stat::IsSorted)),
            Precision::Exact(true),
            "IsSorted over {} values",
            values.len()
        );
    }
    Ok(())
}

#[test]
fn test_lazy_slice() -> VortexResult<()> {
    let values = sorted(1000, 1500, 13);
    let sliced = encode(&values)?.into_array().slice(100..900)?;
    let expected = primitive(PType::U64, &values[100..900]);
    assert_arrays_eq!(sliced, expected, &mut SESSION.create_execution_ctx());
    check_access(&sliced, &expected, 0x51CE)
}
