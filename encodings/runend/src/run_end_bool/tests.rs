// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::LazyLock;

use rstest::rstest;
use vortex_array::ArrayContext;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::Bool;
use vortex_array::arrays::BoolArray;
use vortex_array::arrays::Primitive;
use vortex_array::assert_arrays_eq;
use vortex_array::serde::SerializeOptions;
use vortex_array::serde::SerializedArray;
use vortex_buffer::ByteBufferMut;
use vortex_buffer::buffer;
use vortex_error::vortex_err;
use vortex_session::registry::ReadContext;

use super::*;

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

fn bools(values: &[Option<bool>]) -> ArrayRef {
    if values.iter().all(Option::is_some) {
        BoolArray::from_iter(values.iter().map(|v| v.unwrap_or_default())).into_array()
    } else {
        BoolArray::from_iter(values.iter().copied()).into_array()
    }
}

#[rstest]
#[case::empty(&[], false)]
#[case::non_nullable(&[Some(false), Some(false), Some(true), Some(true), Some(false)], false)]
#[case::starts_true(&[Some(true), Some(false), Some(false)], true)]
#[case::nulls_in_the_alternation(&[Some(true), None, None, Some(true), Some(false)], true)]
#[case::leading_null(&[None, Some(true), Some(true), Some(false)], false)]
#[case::all_null(&[None, None, None], false)]
fn encode_roundtrip(#[case] values: &[Option<bool>], #[case] start: bool) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let expected = bools(values);
    let array = RunEndBool::encode(&expected, &mut ctx)?
        .ok_or_else(|| vortex_err!("runs must alternate"))?;
    assert_eq!(array.start(), start);
    let has_nulls = values.iter().any(Option::is_none);
    assert_eq!(array.run_validity().is_some(), has_nulls);
    assert_arrays_eq!(array, expected, &mut ctx);

    let array = array.into_array();
    let read = roundtrip(&array, &ArrayContext::empty())?;
    assert_eq!(read.encoding_id(), VTable::id(&RunEndBool));
    assert_arrays_eq!(read, expected, &mut ctx);
    Ok(())
}

#[test]
fn encode_rejects_nulls_between_equal_runs() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let values = bools(&[Some(true), None, Some(true)]);
    assert!(RunEndBool::encode(&values, &mut ctx)?.is_none());
    // RunEnd keeps explicit values for such arrays.
    assert_arrays_eq!(RunEnd::encode(values.clone(), &mut ctx)?, values, &mut ctx);
    Ok(())
}

#[test]
fn from_run_end_merges_repeated_runs() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let run_end = RunEnd::try_new_offset_length(
        buffer![2u32, 3, 5, 7, 9].into_array(),
        BoolArray::from_iter([true, false, false, true, true]).into_array(),
        1,
        7,
        &mut ctx,
    )?;
    let array = RunEndBool::from_run_end(&run_end, &mut ctx)?
        .ok_or_else(|| vortex_err!("runs must alternate"))?;
    assert!(array.start());
    assert_eq!(array.offset(), 0);
    assert_arrays_eq!(array.ends(), buffer![1u8, 4, 7].into_array(), &mut ctx);
    assert_arrays_eq!(array, run_end, &mut ctx);
    Ok(())
}

#[rstest]
#[case::whole(0..9)]
#[case::from_second_run(3..9)]
#[case::within_runs(1..7)]
fn slice_keeps_the_encoding(#[case] range: Range<usize>) -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let array = RunEndBool::try_new(
        buffer![2u32, 4, 7, 9].into_array(),
        true,
        Validity::from_iter([true, false, true, true]),
        &mut ctx,
    )?
    .into_array();
    let expected = array
        .clone()
        .execute::<BoolArray>(&mut ctx)?
        .into_array()
        .slice(range.clone())?;

    let sliced = array.slice(range)?;
    assert!(sliced.is::<RunEndBool>());
    assert_arrays_eq!(sliced, expected, &mut ctx);
    assert_arrays_eq!(roundtrip(&sliced, &ArrayContext::empty())?, expected, &mut ctx);
    Ok(())
}

#[test]
fn edition_without_v2_rejects_run_end_bool() -> VortexResult<()> {
    let mut ctx = SESSION.create_execution_ctx();
    let ids = vec![
        VTable::id(&RunEnd),
        VTable::id(&Primitive),
        VTable::id(&Bool),
    ];
    let v1_only = ArrayContext::new(ids.clone()).with_allowed_ids(ids.into_iter().collect());
    let values = bools(&[Some(true), Some(false), Some(false)]);

    let array = RunEndBool::encode(&values, &mut ctx)?
        .ok_or_else(|| vortex_err!("runs must alternate"))?;
    let error = roundtrip(array.as_array(), &v1_only).expect_err("vortex.runend.v2 is forbidden");
    assert!(error.to_string().contains("not permitted by ctx"));

    // The explicit form is readable by every edition.
    let read = roundtrip(array.to_run_end()?.as_array(), &v1_only)?;
    assert_arrays_eq!(read, values, &mut ctx);
    Ok(())
}
