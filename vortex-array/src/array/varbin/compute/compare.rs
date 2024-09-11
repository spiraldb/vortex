use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::ByteArrayType;
use arrow_array::{Array as ArrowArray, Datum, GenericByteArray};
use arrow_ord::cmp;
use arrow_schema::DataType;
use vortex_error::{vortex_bail, VortexResult};

use crate::array::{ConstantArray, VarBinArray};
use crate::arrow::FromArrowArray;
use crate::compute::{MaybeCompareFn, Operator};
use crate::{Array, IntoCanonical};

impl MaybeCompareFn for VarBinArray {
    fn maybe_compare(&self, other: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        if let Ok(rhs_const) = ConstantArray::try_from(other) {
            Some(compare_constant(self, &rhs_const, operator))
        } else {
            None
        }
    }
}

fn compare_constant(
    lhs: &VarBinArray,
    rhs: &ConstantArray,
    operator: Operator,
) -> VortexResult<Array> {
    let arrow_lhs = lhs.clone().into_canonical()?.into_arrow()?;
    let constant = Arc::<dyn Datum>::try_from(rhs.scalar())?;

    match arrow_lhs.data_type() {
        DataType::Binary => {
            compare_constant_arrow(arrow_lhs.as_binary::<i32>(), constant, operator)
        }
        DataType::LargeBinary => {
            compare_constant_arrow(arrow_lhs.as_binary::<i64>(), constant, operator)
        }
        DataType::Utf8 => compare_constant_arrow(arrow_lhs.as_string::<i32>(), constant, operator),
        DataType::LargeUtf8 => {
            compare_constant_arrow(arrow_lhs.as_string::<i64>(), constant, operator)
        }
        _ => {
            vortex_bail!("Cannot compare VarBinArray with non-binary type");
        }
    }
}

fn compare_constant_arrow<T: ByteArrayType>(
    lhs: &GenericByteArray<T>,
    rhs: Arc<dyn Datum>,
    operator: Operator,
) -> VortexResult<Array> {
    let rhs = rhs.as_ref();
    let array = match operator {
        Operator::Eq => cmp::eq(lhs, rhs)?,
        Operator::NotEq => cmp::neq(lhs, rhs)?,
        Operator::Gt => cmp::gt(lhs, rhs)?,
        Operator::Gte => cmp::gt_eq(lhs, rhs)?,
        Operator::Lt => cmp::lt(lhs, rhs)?,
        Operator::Lte => cmp::lt_eq(lhs, rhs)?,
    };
    Ok(crate::Array::from_arrow(&array, true))
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use vortex_dtype::{DType, Nullability};
    use vortex_scalar::Scalar;

    use super::*;
    use crate::array::PrimitiveArray;
    use crate::validity::Validity;
    use crate::{IntoArrayVariant, ToArray};

    #[test]
    fn basic_test() {
        let x = vec![
            b"one".as_slice(),
            b"two".as_slice(),
            b"three".as_slice(),
            b"four".as_slice(),
            b"five".as_slice(),
            b"six".as_slice(),
        ]
        .into_iter()
        .flat_map(|x| x.iter().cloned())
        .collect_vec();

        let bytes = PrimitiveArray::from(x).to_array();
        let offsets = PrimitiveArray::from(vec![0, 3, 6, 11, 15, 19, 22]).to_array();
        let arr = VarBinArray::try_new(
            offsets,
            bytes,
            DType::Utf8(Nullability::Nullable),
            Validity::AllValid,
        )
        .unwrap();
        let s = Scalar::utf8("seven".to_string(), Nullability::Nullable);

        let constant_array = ConstantArray::new(s, arr.len());

        let r = compare_constant(&arr, &constant_array, Operator::Eq)
            .unwrap()
            .into_bool()
            .unwrap();

        for v in r.boolean_buffer().iter() {
            assert!(!v);
        }
    }
}
