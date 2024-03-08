use crate::ZigZagArray;
use vortex::array::Array;
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::dtype::{DType, IntWidth, Signedness};
use vortex::error::{VortexError, VortexResult};
use vortex::scalar::{NullableScalar, Scalar, ScalarRef};
use zigzag::ZigZag;

impl ArrayCompute for ZigZagArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for ZigZagArray {
    fn scalar_at(&self, index: usize) -> VortexResult<ScalarRef> {
        let scalar = scalar_at(self.encoded(), index)?;
        let Some(scalar) = scalar.as_nonnull() else {
            return Ok(NullableScalar::none(self.dtype().clone()).boxed());
        };
        match self.dtype() {
            DType::Int(IntWidth::_8, Signedness::Signed, _) => {
                Ok(i8::decode(scalar.try_into()?).into())
            }
            DType::Int(IntWidth::_16, Signedness::Signed, _) => {
                Ok(i16::decode(scalar.try_into()?).into())
            }
            DType::Int(IntWidth::_32, Signedness::Signed, _) => {
                Ok(i32::decode(scalar.try_into()?).into())
            }
            DType::Int(IntWidth::_64, Signedness::Signed, _) => {
                Ok(i64::decode(scalar.try_into()?).into())
            }
            _ => Err(VortexError::InvalidDType(self.dtype().clone())),
        }
    }
}
