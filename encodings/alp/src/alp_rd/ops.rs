// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::ProbeState;
use vortex_array::dtype::PType;
use vortex_array::scalar::Scalar;
use vortex_array::vtable::OperationsVTable;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;

use crate::ALPRD;
use crate::ALPRDArrayExt;
use crate::ALPRDSlots;
use crate::alp_rd::array::LP_PATCH_SLOTS;

impl OperationsVTable<ALPRD> for ALPRD {
    type ProbeState = ALPRDProbeState;

    fn probe_scalar(
        state: &mut ProbeState<'_, ALPRD>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        if !state.is_valid(index, ctx)? {
            return Ok(Scalar::null(state.array().dtype().clone()));
        }
        let array = state.array();
        let (mut retained, mut children) = state.split();
        // The left value can either be a direct value, or an exception.
        // The exceptions array represents exception positions with non-null values.
        let patch_idx = if array.array().slots()[ALPRDSlots::PATCH_INDICES].is_none() {
            None
        } else {
            let cached = retained
                .as_ref()
                .and_then(|state| state.last_query)
                .filter(|(cached_index, _)| *cached_index == index);
            match cached {
                Some((_, patch_idx)) => patch_idx,
                None => {
                    let patch_idx = array
                        .left_parts_patches()
                        .vortex_expect("patch indices are present")
                        .search_index_with_probes(index, &mut children, LP_PATCH_SLOTS, ctx)?
                        .to_found();
                    if let Some(state) = retained.as_deref_mut() {
                        state.last_query = (state.last_index == Some(index))
                            .then_some((index, patch_idx));
                        state.last_index = Some(index);
                    }
                    patch_idx
                }
            }
        };
        let maybe_patched_value = patch_idx
            .map(|patch_idx| {
                children
                    .slot(ALPRDSlots::PATCH_VALUES)?
                    .vortex_expect("patch values are present")
                    .execute_scalar(patch_idx, ctx)
            })
            .transpose()?;
        let left = match maybe_patched_value {
            Some(patched_value) => patched_value
                .as_primitive()
                .as_::<u16>()
                .vortex_expect("patched values must be non-null"),
            _ => {
                let left_code: u16 = children
                    .slot(ALPRDSlots::LEFT_PARTS)?
                    .vortex_expect("left parts are present")
                    .execute_scalar(index, ctx)?
                    .as_primitive()
                    .as_::<u16>()
                    .vortex_expect("left_code must be non-null");
                array.left_parts_dictionary()[left_code as usize]
            }
        };

        // combine left and right values
        Ok(if array.dtype().as_ptype() == PType::F32 {
            let right: u32 = children
                .slot(ALPRDSlots::RIGHT_PARTS)?
                .vortex_expect("right parts are present")
                .execute_scalar(index, ctx)?
                .as_primitive()
                .as_::<u32>()
                .vortex_expect("non-null");
            let packed = f32::from_bits((left as u32) << array.right_bit_width() | right);
            Scalar::primitive(packed, array.dtype().nullability())
        } else {
            let right: u64 = children
                .slot(ALPRDSlots::RIGHT_PARTS)?
                .vortex_expect("right parts are present")
                .execute_scalar(index, ctx)?
                .as_primitive()
                .as_::<u64>()
                .vortex_expect("non-null");
            let packed = f64::from_bits(((left as u64) << array.right_bit_width()) | right);
            Scalar::primitive(packed, array.dtype().nullability())
        })
    }

    fn scalar_at(
        array: ArrayView<'_, ALPRD>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        Self::probe_scalar(&mut ProbeState::once(array), index, ctx)
    }
}

#[derive(Default)]
pub struct ALPRDProbeState {
    last_index: Option<usize>,
    last_query: Option<(usize, Option<usize>)>,
}

#[cfg(test)]
mod test {
    use std::sync::LazyLock;

    use rstest::rstest;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::compute::conformance::consistency::test_scalar_probe_consistency;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::NativePType;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::patches::Patches;
    use vortex_array::scalar::Scalar;
    use vortex_buffer::Buffer;
    use vortex_error::VortexResult;
    use vortex_session::VortexSession;

    use crate::ALPRD;
    use crate::ALPRDArrayExt;
    use crate::ALPRDFloat;
    use crate::RDEncoder;
    use crate::RDEncoderExt;

    static SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
        let session = vortex_array::array_session();
        crate::initialize(&session);
        session
    });

    #[rstest]
    #[case(0.1f32, 0.2f32, 3e25f32)]
    #[case(0.1f64, 0.2f64, 3e100f64)]
    fn test_slice<T: ALPRDFloat + NativePType>(#[case] a: T, #[case] b: T, #[case] outlier: T) {
        let mut ctx = SESSION.create_execution_ctx();
        let array = PrimitiveArray::from_iter([a, b, outlier]);
        let encoded = RDEncoder::new(&[a, b]).encode(array.as_view());

        assert!(encoded.left_parts_patches().is_some());
        let sliced = encoded.slice(1..3).unwrap();
        assert_arrays_eq!(sliced, PrimitiveArray::from_iter([b, outlier]), &mut ctx);
    }

    #[rstest]
    #[case(0.1f32, 0.2f32, 3e25f32)]
    #[case(0.1f64, 0.2f64, 3e100f64)]
    fn test_scalar_at<T: ALPRDFloat + NativePType + Into<Scalar>>(
        #[case] a: T,
        #[case] b: T,
        #[case] outlier: T,
    ) {
        let mut ctx = SESSION.create_execution_ctx();
        let array = PrimitiveArray::from_iter([a, b, outlier]);
        let encoded = RDEncoder::new(&[a, b]).encode(array.as_view());
        assert!(encoded.left_parts_patches().is_some());
        assert_arrays_eq!(encoded, array, &mut ctx);
        for (index, value) in [a, b, outlier].into_iter().enumerate() {
            let expected: Scalar = value.into();
            let actual = encoded.execute_scalar(index, &mut ctx).unwrap();
            assert_eq!(actual.dtype(), expected.dtype());
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn nullable_scalar_at() {
        let mut ctx = SESSION.create_execution_ctx();
        let a = 0.1f64;
        let b = 0.2f64;
        let outlier = 3e100f64;
        let array = PrimitiveArray::from_option_iter([Some(a), Some(b), Some(outlier)]);
        let encoded = RDEncoder::new(&[a, b]).encode(array.as_view());
        assert!(encoded.left_parts_patches().is_some());
        assert_arrays_eq!(
            encoded,
            PrimitiveArray::from_option_iter([Some(a), Some(b), Some(outlier)]),
            &mut ctx
        );
        for (index, value) in [a, b, outlier].into_iter().enumerate() {
            let expected = Scalar::primitive(value, Nullability::Nullable);
            let actual = encoded.execute_scalar(index, &mut ctx).unwrap();
            assert_eq!(actual.dtype(), expected.dtype());
            assert_eq!(actual, expected);
        }
    }

    #[rstest]
    #[case(0.1f32, 0.2f32, 3e25f32)]
    #[case(0.1f64, 0.2f64, 3e100f64)]
    fn probe_patches_nulls_and_slices<T: ALPRDFloat + NativePType + Into<Scalar>>(
        #[case] a: T,
        #[case] b: T,
        #[case] outlier: T,
    ) -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        for values in [
            [Some(a), None, Some(outlier), Some(b), Some(a)],
            [Some(a), None, Some(b), Some(a), Some(b)],
        ] {
            let primitive = PrimitiveArray::from_option_iter(values);
            let encoded = RDEncoder::new(&[a, b]).encode(primitive.as_view());
            let array = encoded.into_array();
            test_scalar_probe_consistency(&array, &mut ctx);
            let sliced = array.slice(1..5)?;
            test_scalar_probe_consistency(&sliced, &mut ctx);
            let mut probe = sliced.repeated_probe();
            for index in [1, 0, 3, 1, 2, 0] {
                let expected = match values[index + 1] {
                    Some(value) => {
                        let scalar: Scalar = value.into();
                        scalar.cast(sliced.dtype())?
                    }
                    None => Scalar::null(sliced.dtype().clone()),
                };
                assert_eq!(probe.execute_scalar(index, &mut ctx)?, expected);
            }
        }
        Ok(())
    }

    #[test]
    fn probe_chunk_offsets_after_double_slice() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let len = 2052;
        let patches = Patches::new(
            len,
            0,
            PrimitiveArray::from_iter([0u64, 1023, 1024, 2048, 2051]).into_array(),
            PrimitiveArray::from_iter([0x4000u16; 5]).into_array(),
            Some(PrimitiveArray::from_iter([0u64, 2, 3]).into_array()),
            &mut ctx,
        )?;
        let array = ALPRD::try_new(
            DType::Primitive(PType::F64, Nullability::NonNullable),
            PrimitiveArray::from_iter(vec![0u16; len]).into_array(),
            Buffer::from(vec![0x3ff0u16]),
            PrimitiveArray::from_iter(vec![0u64; len]).into_array(),
            48,
            Some(patches),
        )?
        .into_array();
        let sliced = array.slice(100..2052)?.slice(900..1952)?;
        test_scalar_probe_consistency(&sliced, &mut ctx);
        let mut probe = sliced.repeated_probe();
        for index in [23, 24, 25, 1048, 1051, 23, 48, 1048] {
            let expected = if [23, 24, 1048, 1051].contains(&index) {
                2.0f64
            } else {
                1.0f64
            };
            assert_eq!(probe.execute_scalar(index, &mut ctx)?, Scalar::from(expected));
            assert_eq!(sliced.execute_scalar(index, &mut ctx)?, Scalar::from(expected));
        }
        Ok(())
    }
}
