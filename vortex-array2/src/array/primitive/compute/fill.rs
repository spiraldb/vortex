use vortex::match_each_native_ptype;
use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray;
use crate::compute::fill::FillForwardFn;
use crate::validity::ArrayValidity;
use crate::{ArrayTrait, IntoArray, OwnedArray, ToArrayData};

impl FillForwardFn for PrimitiveArray<'_> {
    fn fill_forward(&self) -> VortexResult<OwnedArray> {
        let validity = self.logical_validity();
        let Some(nulls) = validity.to_null_buffer()? else {
            return Ok(self.to_array_data().into_array());
        };

        let typed_data = self.typed_data::<u16>();
        let mut last_value = u16::default();
        let filled = typed_data
            .iter()
            .zip(nulls.into_iter())
            .map(|(v, valid)| {
                if valid {
                    last_value = *v;
                }
                last_value
            })
            .collect::<Vec<_>>();
        Ok(filled.into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::primitive::PrimitiveArray;
    use crate::compute;

    #[test]
    fn leading_none() {
        let arr = PrimitiveArray::from_iter(vec![None, Some(8u8), None, Some(10), None]);
        let filled = compute::fill::fill_forward(&arr).unwrap();
        let filled_primitive = filled.as_primitive();
        assert_eq!(filled_primitive.typed_data::<u8>(), vec![0, 8, 8, 10, 10]);
        assert!(filled_primitive.validity().is_none());
    }

    #[test]
    fn all_none() {
        let arr = PrimitiveArray::from_iter(vec![Option::<u8>::None, None, None, None, None]);
        let filled = compute::fill::fill_forward(&arr).unwrap();
        let filled_primitive = filled.as_primitive();
        assert_eq!(filled_primitive.typed_data::<u8>(), vec![0, 0, 0, 0, 0]);
        assert!(filled_primitive.validity().is_none());
    }

    #[test]
    fn nullable_non_null() {
        let arr = PrimitiveArray::from_nullable(
            vec![8u8, 10u8, 12u8, 14u8, 16u8],
            Some(vec![true, true, true, true, true].into()),
        );
        let filled = compute::fill::fill_forward(&arr).unwrap();
        let filled_primitive = filled.as_primitive();
        assert_eq!(filled_primitive.typed_data::<u8>(), vec![8, 10, 12, 14, 16]);
        assert!(filled_primitive.validity().is_none());
    }
}
