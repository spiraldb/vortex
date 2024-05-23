use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};
use vortex_expr::operators::Operator;
use vortex_scalar::Scalar;

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::compute::compare::compare;
use crate::compute::compare_scalar::CompareScalarFn;
use crate::{Array, ArrayTrait, IntoArray};

impl CompareScalarFn for BoolArray {
    fn compare_scalar(&self, op: Operator, scalar: &Scalar) -> VortexResult<Array> {
        match scalar.dtype() {
            DType::Bool(_) => {}
            _ => {
                vortex_bail!("Invalid dtype for boolean scalar comparison")
            }
        }
        compare(
            &self.clone().into_array(),
            &ConstantArray::new(scalar.clone(), self.len()).into_array(),
            op,
        )
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use crate::compute::compare_scalar::compare_scalar;
    use crate::validity::Validity;

    fn to_int_indices(indices_bits: BoolArray) -> Vec<u64> {
        let filtered = indices_bits
            .boolean_buffer()
            .iter()
            .enumerate()
            .flat_map(|(idx, v)| if v { Some(idx as u64) } else { None })
            .collect_vec();
        filtered
    }

    #[test]
    fn test_basic_comparisons() -> VortexResult<()> {
        let arr = BoolArray::from_vec(
            vec![true, true, false, true, false],
            Validity::Array(BoolArray::from(vec![false, true, true, true, false]).into_array()),
        )
        .into_array();

        let matches = compare_scalar(&arr, Operator::EqualTo, &false.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [2u64]);

        let matches = compare_scalar(&arr, Operator::NotEqualTo, &false.into())?.flatten_bool()?;
        assert_eq!(to_int_indices(matches), [1u64, 3]);
        Ok(())
    }
}
