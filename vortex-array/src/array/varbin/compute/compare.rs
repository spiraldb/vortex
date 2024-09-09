use arrow_array::cast::AsArray;
use arrow_array::types::ByteArrayType;
use arrow_array::{
    Array as ArrowArray, BinaryArray, GenericByteArray, LargeBinaryArray, LargeStringArray, Scalar,
    StringArray,
};
use arrow_cast::cast;
use arrow_ord::cmp;
use arrow_schema::DataType;
use vortex_error::{vortex_bail, VortexResult};

use crate::array::{ConstantArray, VarBinArray};
use crate::arrow::FromArrowArray;
use crate::compute::{MaybeCompareFn, Operator, SliceFn};
use crate::{Array, IntoCanonical};

impl MaybeCompareFn for VarBinArray {
    fn maybe_compare(&self, array: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        if let Ok(rhs_const) = ConstantArray::try_from(array) {
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
    let arrow_lhs = lhs.clone().into_canonical()?.into_arrow();
    let constant = rhs.slice(0, 1)?.clone();
    let arrow_rhs = constant.into_canonical()?.into_arrow();

    match arrow_lhs.data_type() {
        DataType::Binary => {
            let arrow_rhs = cast(arrow_rhs.as_ref(), &DataType::Binary)?;
            let scalar = Scalar::<BinaryArray>::new(arrow_rhs.as_binary().clone());
            compare_constant_arrow(arrow_lhs.as_binary(), &scalar, operator)
        }
        DataType::LargeBinary => {
            let arrow_rhs = cast(arrow_rhs.as_ref(), &DataType::LargeBinary)?;
            let scalar = Scalar::<LargeBinaryArray>::new(arrow_rhs.as_binary().clone());
            compare_constant_arrow(arrow_lhs.as_binary(), &scalar, operator)
        }
        DataType::Utf8 => {
            let arrow_rhs = cast(arrow_rhs.as_ref(), &DataType::Utf8)?;
            let scalar = Scalar::<StringArray>::new(arrow_rhs.as_string().clone());
            compare_constant_arrow(arrow_lhs.as_string(), &scalar, operator)
        }
        DataType::LargeUtf8 => {
            let arrow_rhs = cast(arrow_rhs.as_ref(), &DataType::LargeUtf8)?;
            let scalar = Scalar::<LargeStringArray>::new(arrow_rhs.as_string().clone());
            compare_constant_arrow(arrow_lhs.as_string(), &scalar, operator)
        }
        _ => {
            vortex_bail!("Cannot compare VarBinArray with non-binary type");
        }
    }
}

fn compare_constant_arrow<T: ByteArrayType>(
    lhs: &GenericByteArray<T>,
    rhs: &Scalar<GenericByteArray<T>>,
    operator: Operator,
) -> VortexResult<Array> {
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
