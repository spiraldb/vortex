// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::ProbeState;
use vortex_array::scalar::Scalar;
use vortex_array::vtable::OperationsVTable;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;

use crate::BitPacked;
use crate::BitPackedSlots;
use crate::BitWidthsView;
use crate::bitpack_decompress;
use crate::bitpacking::array::BitPackedArrayExt;
use crate::bitpacking::array::PATCH_SLOTS;

impl OperationsVTable<BitPacked> for BitPacked {
    type ProbeState = BitPackedProbeState;

    fn probe_scalar(
        state: &mut ProbeState<'_, BitPacked>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        if !state.is_valid(index, ctx)? {
            return Ok(Scalar::null(state.array().dtype().clone()));
        }
        let array = state.array();
        let (mut retained, mut children) = state.split();
        let patch_idx = array
            .patches()
            .map(|patches| {
                patches
                    .search_index_with_probes(index, &mut children, PATCH_SLOTS, ctx)
                    .map(|result| result.to_found())
            })
            .transpose()?
            .flatten();
        if let Some(patch_idx) = patch_idx {
            return children
                .slot(BitPackedSlots::PATCH_VALUES)?
                .vortex_expect("patch values are present")
                .execute_scalar(patch_idx, ctx)?
                .cast(array.dtype());
        }

        match array.bit_widths() {
            BitWidthsView::Global(_) => bitpack_decompress::unpack_single(array, index),
            BitWidthsView::Blocked(offsets) => {
                bitpack_decompress::unpack_single_blocked_with_probes(
                    array,
                    offsets,
                    index,
                    &mut children,
                    retained.as_deref_mut().map(|state| &mut state.block_cache),
                    ctx,
                )
            }
        }
    }

    fn scalar_at(
        array: ArrayView<'_, BitPacked>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        Self::probe_scalar(&mut ProbeState::once(array), index, ctx)
    }
}

#[derive(Default)]
pub struct BitPackedProbeState {
    block_cache: bitpack_decompress::BlockProbeCache,
}

#[cfg(test)]
mod test {
    use std::ops::Range;

    use vortex_array::ArrayRef;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::array_session;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::SliceArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::assert_nth_scalar;
    use vortex_array::buffer::BufferHandle;
    use vortex_array::compute::conformance::consistency::test_scalar_probe_consistency;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::patches::Patches;
    use vortex_array::scalar::Scalar;
    use vortex_array::validity::Validity;
    use vortex_buffer::Alignment;
    use vortex_buffer::Buffer;
    use vortex_buffer::ByteBuffer;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;

    use crate::BitPacked;
    use crate::BitPackedArray;
    use crate::BitPackedData;
    use crate::FoR;
    use crate::bitpack_compress::bitpack_encode;
    use crate::bitpack_compress::bitpack_encode_blocked;
    use crate::bitpacking::array::BitPackedArrayExt;
    use crate::test::SESSION;

    fn bp(array: &ArrayRef, bit_width: u8) -> BitPackedArray {
        BitPackedData::encode(array, bit_width, &mut SESSION.create_execution_ctx()).unwrap()
    }

    fn slice_via_reduce(array: &BitPackedArray, range: Range<usize>) -> BitPackedArray {
        let array_ref = array.clone().into_array();
        let slice_array = SliceArray::new(array_ref.clone(), range);
        let sliced = array_ref
            .reduce_parent(&slice_array.into_array(), 0)
            .expect("execute_parent failed")
            .expect("expected slice kernel to execute");
        sliced.as_::<BitPacked>().into_owned()
    }

    #[test]
    pub fn slice_block() {
        let arr = bp(
            &PrimitiveArray::from_iter((0u32..2048).map(|v| v % 64)).into_array(),
            6,
        );
        let sliced = slice_via_reduce(&arr, 1024..2048);
        assert_nth_scalar!(sliced, 0, 1024u32 % 64, &mut SESSION.create_execution_ctx());
        assert_nth_scalar!(
            sliced,
            1023,
            2047u32 % 64,
            &mut SESSION.create_execution_ctx()
        );
        assert_eq!(sliced.offset(), 0);
        assert_eq!(sliced.len(), 1024);
    }

    #[test]
    fn slice_within_block_u8s() {
        let packed = bp(
            &PrimitiveArray::from_iter((0..10_000).map(|i| (i % 63) as u8)).into_array(),
            7,
        );

        let compressed = packed.slice(768..9999).unwrap();
        assert_nth_scalar!(
            compressed,
            0,
            (768 % 63) as u8,
            &mut SESSION.create_execution_ctx()
        );
        assert_nth_scalar!(
            compressed,
            compressed.len() - 1,
            (9998 % 63) as u8,
            &mut SESSION.create_execution_ctx()
        );
    }

    #[test]
    fn slice_block_boundary_u8s() {
        let packed = bp(
            &PrimitiveArray::from_iter((0..10_000).map(|i| (i % 63) as u8)).into_array(),
            7,
        );

        let compressed = packed.slice(7168..9216).unwrap();
        assert_nth_scalar!(
            compressed,
            0,
            (7168 % 63) as u8,
            &mut SESSION.create_execution_ctx()
        );
        assert_nth_scalar!(
            compressed,
            compressed.len() - 1,
            (9215 % 63) as u8,
            &mut SESSION.create_execution_ctx()
        );
    }

    #[test]
    fn double_slice_within_block() {
        let arr = bp(
            &PrimitiveArray::from_iter((0u32..2048).map(|v| v % 64)).into_array(),
            6,
        );
        let sliced = slice_via_reduce(&arr, 512..1434);
        assert_nth_scalar!(sliced, 0, 512u32 % 64, &mut SESSION.create_execution_ctx());
        assert_nth_scalar!(
            sliced,
            921,
            1433u32 % 64,
            &mut SESSION.create_execution_ctx()
        );
        assert_eq!(sliced.offset(), 512);
        assert_eq!(sliced.len(), 922);
        let doubly_sliced = slice_via_reduce(&sliced, 127..911);
        assert_nth_scalar!(
            doubly_sliced,
            0,
            (512u32 + 127) % 64,
            &mut SESSION.create_execution_ctx()
        );
        assert_nth_scalar!(
            doubly_sliced,
            783,
            (512u32 + 910) % 64,
            &mut SESSION.create_execution_ctx()
        );
        assert_eq!(doubly_sliced.offset(), 639);
        assert_eq!(doubly_sliced.len(), 784);
    }

    #[test]
    fn slice_empty_patches() {
        let mut ctx = SESSION.create_execution_ctx();
        // We create an array that has 1 element that does not fit in the 6-bit range.
        let array = BitPackedData::encode(&buffer![0u32..=64].into_array(), 6, &mut ctx).unwrap();

        assert!(array.patches().is_some());

        let patch_indices = array.patches().unwrap().indices().clone();
        assert_eq!(patch_indices.len(), 1);

        // Slicing with patches requires the execute path (not reduce) since patches.slice()
        // reads buffers. The slice range 0..64 excludes the patch at index 64, so the
        // resulting array should have no patches.
        let array_ref = array.into_array();
        let slice_array = SliceArray::new(array_ref, 0..64);
        let mut ctx = SESSION.create_execution_ctx();
        let sliced_bp = slice_array
            .into_array()
            .execute::<ArrayRef>(&mut ctx)
            .expect("slice execution failed")
            .as_::<BitPacked>()
            .into_owned();
        assert!(sliced_bp.patches().is_none());
    }

    #[test]
    fn take_after_slice() {
        // Check that our take implementation respects the offsets applied after slicing.

        let array = bp(
            &PrimitiveArray::from_iter((63u32..).take(3072)).into_array(),
            6,
        );

        // Slice the array.
        // The resulting array will still have 3 1024-element chunks.
        let sliced = array.slice(922..2061).unwrap();

        // Take one element from each chunk.
        // Chunk 1: physical indices  922-1023, logical indices    0-101
        // Chunk 2: physical indices 1024-2047, logical indices  102-1125
        // Chunk 3: physical indices 2048-2060, logical indices 1126-1138

        let taken = sliced
            .take(buffer![101i64, 1125, 1138].into_array())
            .unwrap();
        assert_arrays_eq!(
            taken,
            PrimitiveArray::from_iter([1086u32, 2110, 2123]),
            &mut SESSION.create_execution_ctx()
        );
    }

    #[test]
    fn scalar_at_invalid_patches() {
        let mut ctx = array_session().create_execution_ctx();
        let packed_array = BitPacked::try_new(
            BufferHandle::new_host(ByteBuffer::copy_from_aligned(
                [0u8; 128],
                Alignment::of::<u32>(),
            )),
            PType::U32,
            Validity::AllInvalid,
            Some(
                Patches::new(
                    8,
                    0,
                    buffer![1u32].into_array(),
                    PrimitiveArray::new(buffer![999u32], Validity::AllValid).into_array(),
                    None,
                    &mut ctx,
                )
                .unwrap(),
            ),
            1,
            8,
            0,
        )
        .unwrap()
        .into_array();
        assert_eq!(
            packed_array
                .execute_scalar(1, &mut SESSION.create_execution_ctx())
                .unwrap(),
            Scalar::null(DType::Primitive(PType::U32, Nullability::Nullable))
        );
    }

    #[test]
    fn scalar_at() {
        let mut ctx = SESSION.create_execution_ctx();
        let values = (0u32..257).collect::<Buffer<_>>();
        let uncompressed = values.clone().into_array();
        let packed = BitPackedData::encode(&uncompressed, 8, &mut ctx).unwrap();
        assert!(packed.patches().is_some());

        let patches = packed.patches().unwrap().indices().clone();
        assert_eq!(
            usize::try_from(
                &patches
                    .execute_scalar(0, &mut SESSION.create_execution_ctx())
                    .unwrap()
            )
            .unwrap(),
            256
        );

        for (index, value) in values.iter().copied().enumerate() {
            let scalar = packed.execute_scalar(index, &mut ctx).unwrap();
            let expected = Scalar::from(value);
            assert_eq!(scalar.dtype(), expected.dtype());
            assert_eq!(scalar, expected);
        }

        let expected = PrimitiveArray::from_iter(values.iter().copied());
        assert_arrays_eq!(packed, expected, &mut ctx);
    }

    #[test]
    fn probe_global_signed_nulls_and_patches() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let values = PrimitiveArray::from_option_iter((0..3072).map(|index| {
            if index == 1024 {
                None
            } else if index % 64 == 0 {
                Some(1000i32)
            } else {
                Some((index % 64) as i32)
            }
        }));
        let encoded = bitpack_encode(&values, 6, None, &mut ctx)?.into_array();
        test_scalar_probe_consistency(&encoded, &mut ctx);
        let sliced = encoded.slice(511..3071)?.slice(500..2550)?;
        test_scalar_probe_consistency(&sliced, &mut ctx);
        let mut probe = sliced.repeated_probe();
        for index in [13, 14, 1024, 13, 1537, 14] {
            assert_eq!(
                probe.execute_scalar(index, &mut ctx)?,
                values.execute_scalar(index + 1011, &mut ctx)?
            );
        }
        Ok(())
    }

    #[test]
    fn probe_blocked_encoded_offsets_and_patches() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let widths = [4u8, 6, 7];
        let values = PrimitiveArray::from_option_iter((0..3072).map(|index| {
            if index == 1024 {
                None
            } else if index % 64 == 0 {
                Some(1000u32)
            } else {
                Some((index as u32) & ((1u32 << widths[index / 1024]) - 1))
            }
        }));
        let encoded = bitpack_encode_blocked(&values, &widths, None, &mut ctx)?;
        let offsets = encoded.block_offsets().unwrap().clone();
        let reference = Scalar::zero_value(offsets.dtype());
        let offsets = FoR::try_new(offsets, reference)?.into_array();
        let blocked = BitPacked::try_new_with_block_offsets(
            encoded.packed().clone(),
            PType::U32,
            values.validity()?,
            encoded.patches(),
            offsets,
            encoded.len(),
            encoded.offset(),
        )?
        .into_array();
        test_scalar_probe_consistency(&blocked, &mut ctx);
        let mut probe = blocked.repeated_probe();
        for index in [0, 1023, 1024, 1025, 2048, 1023, 64, 2048] {
            assert_eq!(
                probe.execute_scalar(index, &mut ctx)?,
                values.execute_scalar(index, &mut ctx)?
            );
        }
        Ok(())
    }

    #[test]
    fn probe_skips_invalid_unselected_block() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let offsets = FoR::try_new(
            PrimitiveArray::from_iter([0u64, 128, 0]).into_array(),
            0u64.into(),
        )?
        .into_array();
        let patches = Patches::new(
            2048,
            0,
            PrimitiveArray::from_iter([1024u32]).into_array(),
            PrimitiveArray::from_iter([999u32]).into_array(),
            None,
            &mut ctx,
        )?;
        let array = BitPacked::try_new_with_block_offsets(
            BufferHandle::new_host(ByteBuffer::zeroed(128)),
            PType::U32,
            Validity::from_iter((0..2048).map(|index| index != 1025)),
            Some(patches),
            offsets,
            2048,
            0,
        )?
        .into_array();
        let mut probe = array.repeated_probe();
        assert_eq!(probe.execute_scalar(0, &mut ctx)?, Scalar::from(Some(0u32)));
        assert_eq!(
            probe.execute_scalar(1024, &mut ctx)?,
            Scalar::from(Some(999u32))
        );
        assert_eq!(
            probe.execute_scalar(1025, &mut ctx)?,
            Scalar::null(array.dtype().clone())
        );
        assert!(probe.execute_scalar(1026, &mut ctx).is_err());
        assert_eq!(probe.execute_scalar(0, &mut ctx)?, Scalar::from(Some(0u32)));
        assert_eq!(
            probe.execute_scalar(1024, &mut ctx)?,
            Scalar::from(Some(999u32))
        );
        Ok(())
    }

    #[test]
    fn probe_chunked_patches_after_double_slice() -> VortexResult<()> {
        let mut ctx = SESSION.create_execution_ctx();
        let len = 2052;
        let values = PrimitiveArray::from_iter(vec![1u32; len]);
        let encoded = bitpack_encode(&values, 1, None, &mut ctx)?;
        let patches = Patches::new(
            len,
            0,
            PrimitiveArray::from_iter([0u64, 1023, 1024, 2048, 2051]).into_array(),
            PrimitiveArray::from_iter([999u32; 5]).into_array(),
            Some(PrimitiveArray::from_iter([0u64, 2, 3]).into_array()),
            &mut ctx,
        )?;
        let array = BitPacked::try_new(
            encoded.packed().clone(),
            PType::U32,
            Validity::NonNullable,
            Some(patches),
            1,
            len,
            0,
        )?
        .into_array();
        let sliced = array.slice(100..2052)?.slice(900..1952)?;
        test_scalar_probe_consistency(&sliced, &mut ctx);
        let mut probe = sliced.repeated_probe();
        for index in [23, 24, 25, 1048, 1051, 23, 48, 1048] {
            let expected = if [23, 24, 1048, 1051].contains(&index) {
                999u32
            } else {
                1u32
            };
            assert_eq!(probe.execute_scalar(index, &mut ctx)?, Scalar::from(expected));
        }
        Ok(())
    }
}
