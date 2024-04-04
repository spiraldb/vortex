use num_traits::PrimInt;
use vortex_error::VortexResult;

use crate::array::primitive::compute::PrimitiveTrait;
use crate::array::primitive::PrimitiveArray;
use crate::array::{Array, ArrayRef};
use crate::compute::flatten::flatten_primitive;
use crate::compute::take::TakeFn;
use crate::match_each_integer_ptype;
use crate::ptype::NativePType;
<<<<<<< HEAD
use crate::validity::OwnedValidity;
use crate::{match_each_integer_ptype, match_each_native_ptype};
=======
>>>>>>> develop

impl<T: NativePType> TakeFn for &dyn PrimitiveTrait<T> {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let validity = self.validity_view().map(|v| v.take(indices)).transpose()?;
        let indices = flatten_primitive(indices)?;
        match_each_integer_ptype!(indices.ptype(), |$I| {
            Ok(PrimitiveArray::from_nullable(
                take_primitive(self.typed_data(), indices.typed_data::<$I>()),
                validity,
            ).into_array())
        })
    }
}

fn take_primitive<T: NativePType, I: NativePType + PrimInt>(array: &[T], indices: &[I]) -> Vec<T> {
    indices
        .iter()
        .map(|&idx| array[idx.to_usize().unwrap()])
        .collect()
}

#[cfg(test)]
mod test {
    use crate::array::primitive::compute::take::take_primitive;

    #[test]
    fn test_take() {
        let a = vec![1i32, 2, 3, 4, 5];
        let result = take_primitive(&a, &[0, 0, 4, 2]);
        assert_eq!(result, vec![1i32, 1, 5, 3]);
    }
}
