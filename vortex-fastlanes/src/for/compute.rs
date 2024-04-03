use vortex::array::{Array, ArrayRef};
use vortex::compute::flatten::{FlattenFn, FlattenedArray};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex::match_each_integer_ptype;
use vortex::scalar::{PrimitiveScalar, Scalar};
use vortex_error::VortexResult;

use crate::r#for::compress::decompress;
use crate::FoRArray;

impl ArrayCompute for FoRArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl FlattenFn for FoRArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        decompress(self).map(FlattenedArray::Primitive)
    }
}

impl TakeFn for FoRArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        Ok(FoRArray::try_new(
            take(self.encoded(), indices)?,
            self.reference.clone(),
            self.shift,
        )?
        .into_array())
    }
}

impl ScalarAtFn for FoRArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let encoded_scalar = scalar_at(self.encoded(), index)?;

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
                &PrimitiveArray::from(vec![11, 15, 19]),
                None,
                CompressCtx::default(),
            )
            .unwrap();
        assert_eq!(scalar_at(&forarr, 0).unwrap(), 11.into());
        assert_eq!(scalar_at(&forarr, 1).unwrap(), 15.into());
        assert_eq!(scalar_at(&forarr, 2).unwrap(), 19.into());
    }
}
