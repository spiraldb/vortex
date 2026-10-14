// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::OperationsVTable;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::optimizer::ArrayOptimizer;
use vortex_array::scalar::Scalar;
use vortex_error::VortexResult;

use crate::Patched;
use crate::PatchedArrayExt;
use crate::PatchedArraySlotsExt;

impl OperationsVTable<Patched> for Patched {
    type ProbeState = ();

    fn scalar_at(
        array: ArrayView<'_, Patched>,
        index: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Scalar> {
        let chunk = (index + array.offset()) / 1024;

        #[expect(
            clippy::cast_possible_truncation,
            reason = "N % 1024 always fits in u16"
        )]
        let chunk_index = ((index + array.offset()) % 1024) as u16;

        let lane = (index + array.offset()) % array.n_lanes();

        let range = array.lane_range(chunk, lane)?;

        // Get the range of indices corresponding to the lane, potentially decoding them to avoid
        // the overhead of repeated scalar_at calls.
        let patch_indices = array
            .patch_indices()
            .slice(range.clone())?
            .optimize()?
            .execute::<PrimitiveArray>(ctx)?;

        // NOTE: we do linear scan as lane has <= 32 patches, binary search would likely
        //  be slower.
        for (&patch_index, idx) in std::iter::zip(patch_indices.as_slice::<u16>(), range) {
            if patch_index == chunk_index {
                return array
                    .patch_values()
                    .execute_scalar(idx, ctx)?
                    .cast(array.dtype());
            }
        }

        // Otherwise, access the underlying value.
        array.inner().execute_scalar(index, ctx)
    }
}

#[cfg(test)]
mod tests {
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::dtype::Nullability;
    use vortex_array::optimizer::ArrayOptimizer;
    use vortex_array::patches::Patches;
    use vortex_array::scalar::Scalar;
    use vortex_buffer::buffer;
    use vortex_session::VortexSession;

    use crate::Patched;

    #[test]
    fn test_simple() {
        let values = buffer![0u16; 1024].into_array();
        let patches = Patches::new(
            1024,
            0,
            buffer![1u32, 2, 3].into_array(),
            buffer![1u16; 3].into_array(),
            None,
        )
        .unwrap();

        let session = VortexSession::empty();
        let mut ctx = session.create_execution_ctx();

        let array = Patched::from_array_and_patches(values, &patches, &mut ctx)
            .unwrap()
            .into_array();

        assert_eq!(
            array
                .execute_scalar(0, &mut crate::test_session().create_execution_ctx())
                .unwrap(),
            Scalar::primitive(0u16, Nullability::NonNullable)
        );
        assert_eq!(
            array
                .execute_scalar(1, &mut crate::test_session().create_execution_ctx())
                .unwrap(),
            Scalar::primitive(1u16, Nullability::NonNullable)
        );
        assert_eq!(
            array
                .execute_scalar(2, &mut crate::test_session().create_execution_ctx())
                .unwrap(),
            Scalar::primitive(1u16, Nullability::NonNullable)
        );
        assert_eq!(
            array
                .execute_scalar(3, &mut crate::test_session().create_execution_ctx())
                .unwrap(),
            Scalar::primitive(1u16, Nullability::NonNullable)
        );
    }

    #[test]
    fn test_multi_chunk() {
        let values = buffer![0u16; 4096].into_array();
        let patches = Patches::new(
            4096,
            0,
            buffer![1u32, 2, 3].into_array(),
            buffer![1u16; 3].into_array(),
            None,
        )
        .unwrap();

        let session = VortexSession::empty();
        let mut ctx = session.create_execution_ctx();

        let array = Patched::from_array_and_patches(values, &patches, &mut ctx)
            .unwrap()
            .into_array();

        for index in 0..array.len() {
            let value = array
                .execute_scalar(index, &mut crate::test_session().create_execution_ctx())
                .unwrap();

            if [1, 2, 3].contains(&index) {
                assert_eq!(value, 1u16.into());
            } else {
                assert_eq!(value, 0u16.into());
            }
        }
    }

    #[test]
    fn test_multi_chunk_sliced() {
        let values = buffer![0u16; 4096].into_array();
        let patches = Patches::new(
            4096,
            0,
            buffer![1u32, 2, 3].into_array(),
            buffer![1u16; 3].into_array(),
            None,
        )
        .unwrap();

        let session = VortexSession::empty();
        let mut ctx = session.create_execution_ctx();

        let array = Patched::from_array_and_patches(values, &patches, &mut ctx)
            .unwrap()
            .into_array()
            .slice(3..4096)
            .unwrap()
            .optimize()
            .unwrap();

        assert!(array.is::<Patched>());

        assert_eq!(
            array
                .execute_scalar(0, &mut crate::test_session().create_execution_ctx())
                .unwrap(),
            1u16.into()
        );
        for index in 1..array.len() {
            assert_eq!(
                array
                    .execute_scalar(index, &mut crate::test_session().create_execution_ctx())
                    .unwrap(),
                0u16.into()
            );
        }
    }
}
