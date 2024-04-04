use arrow_buffer::{BooleanBuffer, Buffer};
use vortex::array::bool::BoolArray;
use vortex::array::validity::Validity;
use vortex::array::Array;
use vortex::compute::flatten::{FlattenFn, FlattenedArray};
use vortex::compute::scalar_at::ScalarAtFn;
use vortex::compute::ArrayCompute;
use vortex::scalar::{AsBytes, Scalar};
use vortex_error::{vortex_err, VortexResult};
use vortex_schema::Nullability;

use crate::RoaringBoolArray;

impl ArrayCompute for RoaringBoolArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl FlattenFn for RoaringBoolArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        // TODO(ngates): benchmark the fastest conversion from BitMap.
        //  Via bitset requires two copies.
        let bitset = self
            .bitmap
            .to_bitset()
            .ok_or(vortex_err!("Failed to convert RoaringBitmap to Bitset"))?;

        let bytes = &bitset.as_slice().as_bytes()[0..bitset.size_in_bytes()];
        let buffer = Buffer::from_slice_ref(bytes);
        Ok(FlattenedArray::Bool(BoolArray::new(
            BooleanBuffer::new(buffer, 0, bitset.size_in_bits()),
            match self.nullability() {
                Nullability::NonNullable => None,
                Nullability::Nullable => Some(Validity::Valid(self.len())),
            },
        )))
    }
}

impl ScalarAtFn for RoaringBoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.bitmap.contains(index as u32) {
            Ok(true.into())
        } else {
            Ok(false.into())
        }
    }
}
