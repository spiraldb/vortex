// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::LazyLock;

use prost::Message as _;
use rstest::rstest;
use vortex_array::ArrayContext;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::assert_arrays_eq;
use vortex_array::serde::SerializeOptions;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::dtype::PType;
use vortex_array::serde::SerializedArray;
use vortex_array::session::ArraySessionExt;
use vortex_array::validity::Validity;
use vortex_buffer::ByteBufferMut;
use vortex_buffer::buffer;
use vortex_session::registry::ReadContext;

use super::*;
use crate::RunEndArray;

static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = vortex_array::array_session();
    crate::initialize(&session);
    session
});

fn roundtrip(array: &ArrayRef, ctx: &ArrayContext) -> VortexResult<ArrayRef> {
    let buffers = array.serialize(ctx, &SESSION, &SerializeOptions::default())?;
    let mut bytes = ByteBufferMut::empty();
    for buffer in buffers {
        bytes.extend_from_slice(&buffer);
    }
    SerializedArray::try_from(bytes.freeze())?.decode(
        array.dtype(),
        array.len(),
        &ReadContext::new(ctx.to_ids()),
        &SESSION,
    )
}

fn serialized_id(array: &ArrayRef) -> VortexResult<ArrayId> {
    Ok(SESSION
        .array_serialize(array)?
        .ok_or_else(|| vortex_err!("RunEnd must serialize"))?
        .serialized_id)
}

fn encode_bools(values: &[Option<bool>]) -> VortexResult<RunEndArray> {
    let bools = if values.iter().all(Option::is_some) {
        BoolArray::from_iter(values.iter().map(|v| v.unwrap_or_default()))
    } else {
        BoolArray::from_iter(values.iter().copied())
    };
    RunEnd::encode(bools.into_array(), &mut SESSION.create_execution_ctx())
}

#[rstest]
#[case::primitive(RunEnd::encode(
    PrimitiveArray::from_iter([1i32, 1, 2, 2, 2, 3]).into_array(),
    &mut SESSION.create_execution_ctx(),
))]
#[case::explicit_bools(RunEnd::try_new(
    buffer![2u8, 3, 5].into_array(),
    BoolArray::from_iter([false, false, true]).into_array(),
    &mut SESSION.create_execution_ctx(),
))]
#[case::encoded_bools(encode_bools(&[Some(true), Some(true), Some(false), Some(true)]))]
fn v1_roundtrip(#[case] array: VortexResult<RunEndArray>) -> VortexResult<()> {
    let array = array?.into_array();
    assert_eq!(serialized_id(&array)?, runend_v1_id());

    let read = roundtrip(&array, &ArrayContext::empty())?;
    assert_eq!(read.encoding_id(), runend_v1_id());
    assert_eq!(read.as_::<RunEnd>().alternating_start(), None);
    assert_arrays_eq!(read, array, &mut SESSION.create_execution_ctx());
    Ok(())
}

#[rstest]
#[case::non_nullable(&[Some(false), Some(false), Some(true), Some(true), Some(false)], false)]
#[case::starts_true(&[Some(true), Some(false), Some(false)], true)]
#[case::nullable_without_nulls(&[Some(true), Some(false)], true)]
#[case::nulls_in_the_alternation(&[Some(true), None, None, Some(true), Some(false)], true)]
#[case::leading_null(&[None, Some(true), Some(true), Some(false)], false)]
#[case::all_null(&[None, None, None], false)]
fn v2_roundtrip(#[case] values: &[Option<bool>], #[case] start: bool) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let encoded = encode_bools(values)?;
    let normalized = RunEnd::normalize_bool(&encoded, &mut ctx)?;
    assert_eq!(normalized.alternating_start(), Some(start));

    let array = normalized.into_array();
    let serialization = SESSION
        .array_serialize(&array)?
        .ok_or_else(|| vortex_err!("RunEnd must serialize"))?;
    assert_eq!(serialization.serialized_id, runend_v2_id());
    assert!(v2::RunEndV2Metadata::decode(serialization.metadata.as_slice())?.start == start);
    // Only the ends and, with nulls, the validity are stored.
    let has_nulls = values.iter().any(Option::is_none);
    assert_eq!(serialization.children.len(), 1 + usize::from(has_nulls));

    let read = roundtrip(&array, &ArrayContext::empty())?;
    assert_eq!(read.as_::<RunEnd>().alternating_start(), Some(start));
    assert_arrays_eq!(read, encoded, &mut ctx);
    Ok(())
}

#[rstest]
#[case::whole(0..9)]
#[case::from_second_run(3..9)]
#[case::within_runs(1..7)]
fn v2_sliced_roundtrip(#[case] range: std::ops::Range<usize>) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let array = RunEnd::try_new_alternating(
        buffer![2u32, 4, 7, 9].into_array(),
        true,
        Validity::from_iter([true, false, true, true]),
        &mut ctx,
    )?
    .into_array()
    .slice(range)?
    .execute::<ArrayRef>(&mut ctx)?;

    let read = roundtrip(&array, &ArrayContext::empty())?;
    assert_arrays_eq!(read, array, &mut ctx);
    Ok(())
}

#[test]
fn normalize_keeps_explicit_values_when_nulls_break_the_alternation() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let encoded = encode_bools(&[Some(true), None, Some(true)])?;
    let normalized = RunEnd::normalize_bool(&encoded, &mut ctx)?;
    assert_eq!(normalized.alternating_start(), None);
    assert_eq!(serialized_id(&normalized.into_array())?, runend_v1_id());
    Ok(())
}

#[test]
fn edition_without_v2_rejects_alternating_arrays() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let ids = vec![
        runend_v1_id(),
        VTable::id(&vortex_array::arrays::Primitive),
        VTable::id(&vortex_array::arrays::Bool),
    ];
    let ctx_v1 = ArrayContext::new(ids.clone()).with_allowed_ids(ids.into_iter().collect());
    let explicit = encode_bools(&[Some(true), Some(false)])?;
    assert!(roundtrip(explicit.as_array(), &ctx_v1).is_ok());

    let alternating = RunEnd::normalize_bool(&explicit, &mut ctx)?;
    let error = roundtrip(alternating.as_array(), &ctx_v1)
        .expect_err("vortex.runend.v2 is not permitted");
    assert!(error.to_string().contains("not permitted by ctx"));
    Ok(())
}

#[test]
fn v2_rejects_malformed_parts() -> VortexResult<()> {
    let metadata = v2::RunEndV2Metadata {
        ends_ptype: PType::U8 as i32,
        num_runs: 2,
        offset: 0,
        start: true,
    }
    .encode_to_vec();
    let ends = buffer![2u8, 4].into_array();
    let deserialize = |dtype: &DType, children: &[ArrayRef]| {
        RunEndPlugin.deserialize(
            ArrayDeserialization::new(runend_v2_id(), dtype, 4, &metadata, &[], &children),
            &SESSION,
        )
    };

    let bools = DType::Bool(Nullability::NonNullable);
    assert!(deserialize(&bools, std::slice::from_ref(&ends)).is_ok());
    let ints = DType::Primitive(PType::I32, Nullability::NonNullable);
    assert!(deserialize(&ints, std::slice::from_ref(&ends)).is_err());
    assert!(deserialize(&bools, &[ends.clone(), ends]).is_err());
    Ok(())
}

#[test]
fn vtable_serde_requires_plugin() -> VortexResult<()> {
    let array = encode_bools(&[Some(true), Some(false)])?;
    let session = vortex_array::array_session();
    session.arrays().register(RunEnd);
    assert!(session.array_serialize(&array.into_array()).is_err());
    Ok(())
}
