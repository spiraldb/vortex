use vortex_error::VortexResult;
use vortex_schema::Nullability;

use crate::array::bool::{BoolArray, BoolData};
use crate::compute::fill::FillForwardFn;
use crate::validity::ArrayValidity;
use crate::{Array, ArrayTrait, IntoArray, ToArrayData};

impl FillForwardFn for BoolArray<'_> {
    fn fill_forward(&self) -> VortexResult<Array<'static>> {
        if self.dtype().nullability() == Nullability::NonNullable {
            return Ok(self.to_array_data().into_array());
        }

        let validity = self.logical_validity().to_null_buffer()?.unwrap();
        let bools = self.buffer();
        let mut last_value = false;
        let filled = bools
            .iter()
            .zip(validity.inner().iter())
            .map(|(v, valid)| {
                if valid {
                    last_value = v;
                }
                last_value
            })
            .collect::<Vec<_>>();
        Ok(BoolData::from(filled).into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::bool::{BoolData, BoolDef};
    use crate::validity::Validity;
    use crate::{compute, IntoArray};

    #[test]
    fn fill_forward() {
        let barr =
            BoolData::from_iter(vec![None, Some(false), None, Some(true), None]).into_array();
        let filled = compute::fill::fill_forward(&barr).unwrap();
        let filled_bool = filled.to_typed_array::<BoolDef>().unwrap();
        assert_eq!(
            filled_bool.buffer().iter().collect::<Vec<bool>>(),
            vec![false, false, false, true, true]
        );
        assert_eq!(*filled_bool.validity(), Validity::NonNullable);
    }
}
