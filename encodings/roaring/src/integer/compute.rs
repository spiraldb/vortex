use vortex::compute::unary::ScalarAtFn;
use vortex::compute::ArrayCompute;
use vortex_dtype::PType;
use vortex_error::{vortex_err, VortexResult, VortexUnwrap as _};
use vortex_scalar::Scalar;

use crate::RoaringIntArray;

impl ArrayCompute for RoaringIntArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for RoaringIntArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let bitmap_value = self
            .bitmap()
            .select(index as u32)
            .ok_or_else(|| vortex_err!(OutOfBounds: index, 0, self.len()))?;
        let scalar: Scalar = match self.metadata().ptype {
            PType::U8 => (bitmap_value as u8).into(),
            PType::U16 => (bitmap_value as u16).into(),
            PType::U32 => bitmap_value.into(),
            PType::U64 => (bitmap_value as u64).into(),
            _ => unreachable!("RoaringIntArray constructor should have disallowed this type"),
        };
        Ok(scalar)
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        <Self as ScalarAtFn>::scalar_at(self, index).vortex_unwrap()
    }
}
