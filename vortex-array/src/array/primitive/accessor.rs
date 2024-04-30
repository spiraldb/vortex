use vortex_dtype::NativePType;
use vortex_error::VortexResult;

use crate::accessor::ArrayAccessor;
use crate::array::primitive::PrimitiveArray;
use crate::validity::ArrayValidity;

impl<T: NativePType> ArrayAccessor<T> for PrimitiveArray<'_> {
    fn with_iterator<F, R>(&self, f: F) -> VortexResult<R>
    where
        F: for<'a> FnOnce(&mut (dyn Iterator<Item = Option<&'a T>>)) -> R,
    {
        match self.logical_validity().to_null_buffer()? {
            None => {
                let mut iter = self.typed_data::<T>().iter().map(Some);
                Ok(f(&mut iter))
            }
            Some(nulls) => {
                let mut iter = self
                    .typed_data::<T>()
                    .iter()
                    .zip(nulls.iter())
                    .map(|(value, valid)| if valid { Some(value) } else { None });
                Ok(f(&mut iter))
            }
        }
    }
}
