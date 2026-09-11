// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use rstest::rstest;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::ProbeUsage;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::assert_arrays_eq;
use vortex_array::builders::builder_with_capacity_in;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;

use crate::RLE;
use crate::RLEData;

#[rstest]
#[case(ProbeUsage::Once, false)]
#[case(ProbeUsage::Repeated, false)]
#[case(ProbeUsage::Once, true)]
#[case(ProbeUsage::Repeated, true)]
fn random_access_across_chunks_and_nulls(
    #[case] usage: ProbeUsage,
    #[case] sliced: bool,
) -> VortexResult<()> {
    let mut ctx = crate::test::SESSION.create_execution_ctx();
    let input =
        PrimitiveArray::from_option_iter((0..8192u32).map(|i| (i % 11 != 0).then_some(i / 16)));
    let encoded = RLEData::encode(input.as_view(), &mut ctx)?.into_array();
    let range = if sliced { 1777..7333 } else { 0..8192 };
    let source = if sliced {
        encoded
            .slice(range.clone())?
            .execute::<ArrayRef>(&mut ctx)?
    } else {
        encoded
    };
    assert!(source.is::<RLE>());
    let input = input.slice(range)?;
    let indices = [0u32, 1, 1023, 1024, 2048, 2047, 4097, 11, 33, 17, 0];
    let mut actual = builder_with_capacity_in(source.dtype(), indices.len(), ctx.allocator());
    let mut probe = source.probe(usage);
    for index in indices {
        actual.append_scalar(&probe.scalar_at(index as usize, &mut ctx)?)?;
    }
    assert_arrays_eq!(
        actual.finish(),
        input.take(PrimitiveArray::from_iter(indices).into_array())?,
        &mut ctx
    );
    assert!(probe.scalar_at(source.len(), &mut ctx).is_err());
    Ok(())
}

#[test]
fn primitive_slots_use_direct_readers() -> VortexResult<()> {
    let mut ctx = crate::test::SESSION.create_execution_ctx();
    let input = PrimitiveArray::from_iter((0..4096u32).map(|i| i / 16));
    let encoded = RLEData::encode(input.as_view(), &mut ctx)?;
    for slot in [
        super::RLESlots::INDICES,
        super::RLESlots::VALUES_IDX_OFFSETS,
        super::RLESlots::VALUES,
    ] {
        let reader = super::Child::new(encoded.as_view().slots(), slot, &mut ctx)?;
        assert!(matches!(reader, super::Child::Primitive { .. }));
    }
    Ok(())
}

#[test]
fn lazy_validity_does_not_evaluate_unrequested_rows() -> VortexResult<()> {
    let mut ctx = crate::test::SESSION.create_execution_ctx();
    let numerators = PrimitiveArray::from_iter(vec![1u32; 1024]).into_array();
    let denominators =
        PrimitiveArray::from_iter((0..1024).map(|i| u32::from(i != 1023))).into_array();
    let validity = numerators
        .binary(denominators, Operator::Div)?
        .binary(numerators, Operator::Eq)?;
    let array = RLE::try_new(
        PrimitiveArray::from_iter([42u32]).into_array(),
        PrimitiveArray::new(vec![0u16; 1024], Validity::Array(validity)).into_array(),
        PrimitiveArray::from_iter([0u64]).into_array(),
        0,
        1024,
    )?
    .into_array();
    let expected = array.execute_scalar(0, &mut ctx)?;
    let mut probe = array.probe(ProbeUsage::Repeated);
    assert_eq!(probe.scalar_at(0, &mut ctx)?, expected);
    assert!(probe.scalar_at(1023, &mut ctx).is_err());
    Ok(())
}
