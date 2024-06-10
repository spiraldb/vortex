use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::{match_each_alp_float_ptype, ALPArray};

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
        if let Some(patch) = self.patches().and_then(|p| scalar_at(&p, index).ok()) {
            return Ok(patch);
        }
        use crate::ALPFloat;
        let encoded_val = scalar_at(&self.encoded(), index)?;
        match_each_alp_float_ptype!(self.dtype().try_into().unwrap(), |$T| {
            let encoded_val: <$T as ALPFloat>::ALPInt = encoded_val.as_ref().try_into().unwrap();
            Scalar::from(<$T as ALPFloat>::decode_single(
                encoded_val,
                self.exponents(),
            ))
        })
    }
}

impl TakeFn for ALPArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        // TODO(ngates): wrap up indices in an array that caches decompression?
        Ok(Self::try_new(
            take(&self.encoded(), indices)?,
            self.exponents().clone(),
            self.patches().map(|p| take(&p, indices)).transpose()?,
        )?
        .into_array())
    }
}

impl SliceFn for ALPArray {
    fn slice(&self, start: usize, end: usize) -> VortexResult<Array> {
        Ok(Self::try_new(
            slice(&self.encoded(), start, end)?,
            self.exponents().clone(),
            self.patches().map(|p| slice(&p, start, end)).transpose()?,
        )?
        .into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::scalar_at::scalar_at;
    use vortex::validity::Validity;
    use vortex::IntoArray;

    use crate::ALPArray;

    #[test]
    fn test_as_contiguous() {
        let values = vec![1.0, 2.0, 3.0];
        let primitives = PrimitiveArray::from_vec(values, Validity::NonNullable);
        let encoded = ALPArray::encode(primitives.into_array()).unwrap();
        let alp = ALPArray::try_from(&encoded).unwrap();

        let flat = alp.as_contiguous(&[encoded]).unwrap();

        let a: f64 = scalar_at(&flat, 0).unwrap().try_into().unwrap();
        let b: f64 = scalar_at(&flat, 1).unwrap().try_into().unwrap();

        let c: f64 = scalar_at(&flat, 2).unwrap().try_into().unwrap();

        assert_eq!(a, 1.0);
        assert_eq!(b, 2.0);
        assert_eq!(c, 3.0);
    }
}
