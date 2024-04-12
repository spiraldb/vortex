use num_traits::PrimInt;
use vortex::ptype::NativePType;
use vortex::{match_each_integer_ptype, match_each_native_ptype};
use vortex_error::VortexResult;

use crate::array::primitive::{PrimitiveArray, PrimitiveData};
use crate::compute::flatten::flatten_primitive;
use crate::compute::take::TakeFn;
use crate::IntoArray;
use crate::{Array, OwnedArray};

impl TakeFn for PrimitiveArray<'_> {
    fn take(&self, indices: &Array) -> VortexResult<OwnedArray> {
        let validity = self.validity();
        let indices_data = flatten_primitive(indices)?;
        let indices = indices_data.as_typed_array();
        match_each_native_ptype!(self.ptype(), |$T| {
            match_each_integer_ptype!(indices.ptype(), |$I| {
                Ok(PrimitiveData::from_vec(
                    take_primitive(self.typed_data::<$T>(), indices.typed_data::<$I>()),
                    validity.take(indices.array())?,
                ).into_array())
            })
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
