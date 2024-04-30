use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::slice::{slice, SliceFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::{Array, IntoArray, OwnedArray};
use vortex_dtype::match_each_integer_ptype;
use vortex_error::VortexResult;
use vortex_scalar::{PrimitiveScalar, Scalar};

use crate::FoRArray;

impl ArrayCompute for FoRArray<'_> {
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

impl TakeFn for FoRArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        FoRArray::try_new(
            take(&self.encoded(), indices)?,
            self.reference().clone(),
            self.shift(),
        )
        .map(|a| a.into_array())
    }
}

impl ScalarAtFn for FoRArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let encoded_scalar = scalar_at(&self.encoded(), index)?;

        match (&encoded_scalar, self.reference()) {
            (Scalar::Primitive(p), Scalar::Primitive(r)) => match p.value() {
                None => Ok(encoded_scalar),
                Some(pv) => match_each_integer_ptype!(pv.ptype(), |$P| {
                    use num_traits::WrappingAdd;
                    Ok(PrimitiveScalar::try_new::<$P>(
                        Some((p.typed_value::<$P>().unwrap() << self.shift()).wrapping_add(r.typed_value::<$P>().unwrap())),
                        p.dtype().nullability()
                    ).unwrap().into())
                }),
            },
            _ => unreachable!("Reference and encoded values had different dtypes"),
        }
    }
}

impl SliceFn for FoRArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        FoRArray::try_new(
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
    use vortex::compress::{CompressCtx, EncodingCompression};
    use vortex::compute::scalar_at::scalar_at;

    use crate::FoREncoding;

    #[test]
    fn for_scalar_at() {
        let forarr = FoREncoding
            .compress(
                PrimitiveArray::from(vec![11, 15, 19]).array(),
                None,
                CompressCtx::default(),
            )
            .unwrap();
        assert_eq!(scalar_at(&forarr, 0).unwrap(), 11.into());
        assert_eq!(scalar_at(&forarr, 1).unwrap(), 15.into());
        assert_eq!(scalar_at(&forarr, 2).unwrap(), 19.into());
    }
}
