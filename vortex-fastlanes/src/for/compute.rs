use vortex::compute::as_contiguous::AsContiguousFn;
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::{Array, impl_default_as_contiguous_fn, IntoArray};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::{PrimitiveScalar, Scalar, ScalarValue};

use crate::FoRArray;

impl_default_as_contiguous_fn!(FoRArray);

impl ArrayCompute for FoRArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }
}

impl TakeFn for FoRArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        Self::try_new(
            take(&self.encoded(), indices)?,
            self.reference().clone(),
            self.shift(),
        )
        .map(|a| a.into_array())
    }
}

impl ScalarAtFn for FoRArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let encoded_scalar = scalar_at(&self.encoded(), index)?;
        let encoded = PrimitiveScalar::try_from(&encoded_scalar)?;
        let reference = PrimitiveScalar::try_from(self.reference())?;

        if encoded.ptype() != reference.ptype() {
            vortex_bail!("Reference and encoded values had different dtypes");
        }

        match_each_integer_ptype!(encoded.ptype(), |$P| {
            use num_traits::WrappingAdd;
            Ok(encoded.typed_value::<$P>().map(|v| (v << self.shift()).wrapping_add(reference.typed_value::<$P>().unwrap()))
                    .map(|v| Scalar::primitive::<$P>(v, encoded.dtype().nullability()))
                    .unwrap_or_else(|| Scalar::new(encoded.dtype().clone(), ScalarValue::Null)))
        })
    }
}

impl SliceFn for FoRArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Self::try_new(
            slice(&self.encoded(), start, stop)?,
            self.reference().clone(),
            self.shift(),
        )
        .map(|a| a.into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::compress::{Compressor, EncodingCompression};
    use vortex::compute::as_contiguous::as_contiguous;
    use vortex::compute::scalar_at::scalar_at;
    use vortex::Context;

    use crate::FoREncoding;

    #[test]
    fn for_scalar_at() {
        let forarr = FoREncoding
            .compress(
                PrimitiveArray::from(vec![11, 15, 19]).array(),
                None,
                Compressor::new(&Context::default()),
            )
            .unwrap();
        assert_eq!(scalar_at(&forarr, 0).unwrap(), 11.into());
        assert_eq!(scalar_at(&forarr, 1).unwrap(), 15.into());
        assert_eq!(scalar_at(&forarr, 2).unwrap(), 19.into());
    }

    #[test]
    fn for_as_contiguous() {
        let forarr1 = FoREncoding
            .compress(
                PrimitiveArray::from(vec![1, 2, 3, 4]).array(),
                None,
                Compressor::new(&Context::default()),
            )
            .unwrap();
        let forarr2 = FoREncoding
            .compress(
                PrimitiveArray::from(vec![5, 6, 7, 8]).array(),
                None,
                Compressor::new(&Context::default()),
            )
            .unwrap();

        let flattened = as_contiguous(&[forarr1, forarr2]).unwrap();

        vec![1, 2, 3, 4, 5, 6, 7, 8]
            .iter()
            .enumerate()
            .for_each(|(idx, value)| {
                assert_eq!(scalar_at(&flattened, idx).unwrap(), (*value).into());
            });
    }
}
