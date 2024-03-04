use crate::ALPArray;
use codecz::alp;
use vortex::array::Array;
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::dtype::{DType, FloatWidth};
use vortex::error::VortexResult;
use vortex::scalar::{NullableScalar, Scalar, ScalarRef};

impl ArrayCompute for ALPArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl ScalarAtFn for ALPArray {
    fn scalar_at(&self, index: usize) -> VortexResult<ScalarRef> {
        if let Some(patch) = self
            .patches()
            .and_then(|p| scalar_at(p, index).ok())
            .and_then(|p| p.into_nonnull())
        {
            return Ok(patch);
        }

        let Some(encoded_val) = scalar_at(self.encoded(), index)?.into_nonnull() else {
            return Ok(NullableScalar::none(self.dtype().clone()).boxed());
        };
        match self.dtype() {
            DType::Float(FloatWidth::_32, _) => {
                let encoded_val: i32 = encoded_val.try_into().unwrap();
                Ok(alp::decode_single::<f32>(encoded_val, self.exponents())
                    .unwrap()
                    .into())
            }

            DType::Float(FloatWidth::_64, _) => {
                let encoded_val: i64 = encoded_val.try_into().unwrap();
                Ok(alp::decode_single::<f64>(encoded_val, self.exponents())
                    .unwrap()
                    .into())
            }

            _ => unreachable!(),
        }
    }
}
