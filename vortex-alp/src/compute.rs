use vortex::array::Array;
use vortex::compute::ArrayCompute;
use vortex::compute::flatten::{FlattenedArray, FlattenFn};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::dtype::{DType, FloatWidth};
use vortex::error::{VortexError, VortexResult};
use vortex::scalar::Scalar;

use crate::{ALPArray, ALPFloat, match_each_alp_float_ptype};
use crate::compress::decompress;

impl ArrayCompute for ALPArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl FlattenFn for ALPArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        decompress(self).map(FlattenedArray::Primitive)
    }
}

impl ScalarAtFn for ALPArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if let Some(patch) = self.patches().and_then(|p| scalar_at(p, index).ok()) {
            return Ok(patch);
        }

        let encoded_val = scalar_at(self.encoded(), index)?;
        match_each_alp_float_ptype!(self.dtype().try_into().unwrap(), |$T| {
            let encoded_val: <$T as ALPFloat>::ALPInt = encoded_val.try_into().unwrap();
            Scalar::from(<$T as ALPFloat>::decode_single(
                encoded_val,
                self.exponents(),
            ))
        })
    }
}
