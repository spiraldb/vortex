use vortex::compute::unary::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::{slice, ArrayCompute, SliceFn};
use vortex::{Array, IntoArray};
use vortex_dtype::PType;
use vortex_error::VortexResult;
use vortex_scalar::{PrimitiveScalar, Scalar};
use zigzag::ZigZag as ExternalZigZag;

use crate::ZigZagArray;

impl ArrayCompute for ZigZagArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }
}

impl ScalarAtFn for ZigZagArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let scalar = scalar_at(&self.encoded(), index)?;
        let pscalar = PrimitiveScalar::try_from(&scalar)?;
        match pscalar.ptype() {
            PType::U8 => Ok(i8::decode(pscalar.typed_value::<u8>().unwrap()).into()),
            PType::U16 => Ok(i16::decode(pscalar.typed_value::<u16>().unwrap()).into()),
            PType::U32 => Ok(i32::decode(pscalar.typed_value::<u32>().unwrap()).into()),
            PType::U64 => Ok(i64::decode(pscalar.typed_value::<u64>().unwrap()).into()),
            _ => unreachable!(),
        }
    }
}

impl SliceFn for ZigZagArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::try_new(slice(&self.encoded(), start, stop)?)?.into_array())
    }
}
