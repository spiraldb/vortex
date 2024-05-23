use std::ops::{BitAnd, BitOr, BitXor, Not};

use arrow_buffer::BooleanBufferBuilder;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_expr::operators::Operator;
use vortex_scalar::Scalar;

use crate::array::bool::BoolArray;
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
        let lhs = self.boolean_buffer();

        let scalar_val = scalar
            .value()
            .as_bool()?
            .ok_or_else(|| vortex_err!("Invalid scalar for comparison"))?;

        let mut rhs = BooleanBufferBuilder::new(self.len());
        rhs.append_n(self.len(), scalar_val);
        let rhs = rhs.finish();
        let result_buf = match op {
            Operator::EqualTo => lhs.bitxor(&rhs).not(),
            Operator::NotEqualTo => lhs.bitxor(&rhs),
            Operator::GreaterThan => lhs.bitand(&rhs.not()),
            Operator::GreaterThanOrEqualTo => lhs.bitor(&rhs.not()),
            Operator::LessThan => lhs.not().bitand(&rhs),
            Operator::LessThanOrEqualTo => lhs.not().bitor(&rhs),
        };

        let present = self
            .validity()
            .to_logical(self.len())
            .to_present_null_buffer()?
            .into_inner();

        Ok(BoolArray::from(result_buf.bitand(&present)).into_array())
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
