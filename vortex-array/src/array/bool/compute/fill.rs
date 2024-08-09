use vortex_dtype::Nullability;
use vortex_error::{vortex_err, VortexResult};

use crate::array::BoolArray;
use crate::compute::unary::FillForwardFn;
use crate::validity::ArrayValidity;
use crate::{Array, ArrayDType, IntoArray};

impl FillForwardFn for BoolArray {
    fn fill_forward(&self) -> VortexResult<Array> {
        let validity = self.logical_validity();
        if self.dtype().nullability() == Nullability::NonNullable || validity.all_valid() || validity.all_invalid() {
            return Ok(self.clone().into());
        } 

        let validity = self.logical_validity().to_null_buffer()?.ok_or_else(|| vortex_err!("Failed to convert array validity to null buffer"))?;
        let bools = self.boolean_buffer();
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
        Ok(Self::from(filled).into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::BoolArray;
    use crate::validity::Validity;
    use crate::{compute, IntoArray};

    #[test]
    fn fill_forward() {
        let barr =
            BoolArray::from_iter(vec![None, Some(false), None, Some(true), None]).into_array();
        let filled_bool =
            BoolArray::try_from(compute::unary::fill_forward(&barr).unwrap()).unwrap();
        assert_eq!(
            filled_bool.boolean_buffer().iter().collect::<Vec<bool>>(),
            vec![false, false, false, true, true]
        );
        assert_eq!(filled_bool.validity(), Validity::NonNullable);
    }
}
