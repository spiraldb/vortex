use vortex::compute::unary::{scalar_at, ScalarAtFn};
use vortex::compute::{slice, take, ArrayCompute, SliceFn, TakeFn};
use vortex::{Array, IntoArray};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::{match_each_alp_float_ptype, ALPArray, ALPFloat};

impl ArrayCompute for ALPArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for ALPArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if let Some(patch) = self.patches().and_then(|p| {
            p.with_dyn(|arr| {
                // We need to make sure the value is actually in the patches array
                if arr.is_valid(index) {
                    scalar_at(&p, index).ok()
                } else {
                    None
                }
            })
        }) {
            return Ok(patch);
        }

        let encoded_val = scalar_at(&self.encoded(), index)?;

        match_each_alp_float_ptype!(self.ptype(), |$T| {
            // If we have a null value, no need to decode it
            if !encoded_val.is_valid() {
                return Ok(Scalar::null(<$T as ALPFloat>::dtype()));
            }
            let encoded_val: <$T as ALPFloat>::ALPInt = encoded_val.as_ref().try_into().unwrap();
            Ok(Scalar::from(<$T as ALPFloat>::decode_single(
                encoded_val,
                self.exponents(),
            )))
        })
    }
}

impl TakeFn for ALPArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        // TODO(ngates): wrap up indices in an array that caches decompression?
        Ok(Self::try_new(
            take(&self.encoded(), indices)?,
            self.exponents(),
            self.patches().map(|p| take(&p, indices)).transpose()?,
        )?
        .into_array())
    }
}

impl SliceFn for ALPArray {
    fn slice(&self, start: usize, end: usize) -> VortexResult<Array> {
        Ok(Self::try_new(
            slice(&self.encoded(), start, end)?,
            self.exponents(),
            self.patches().map(|p| slice(&p, start, end)).transpose()?,
        )?
        .into_array())
    }
}
