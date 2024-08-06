use arrow_ord::cmp;
use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_bail, VortexResult};
use vortex_expr::Operator;
use vortex_scalar::Scalar;

use crate::arrow::FromArrowArray;
use crate::{Array, ArrayDType, ArrayData, IntoCanonical};

pub trait CompareFn {
    fn compare(&self, array: &Array, operator: Operator) -> VortexResult<Array>;
}

pub fn compare(left: &Array, right: &Array, operator: Operator) -> VortexResult<Array> {
    if left.len() != right.len() {
        vortex_bail!("Compare operations only support arrays of the same length");
    }

    // TODO(adamg): This is a placeholder until we figure out type coercion and casting
    if !left.dtype().eq_ignore_nullability(right.dtype()) {
        vortex_bail!("Compare operations only support arrays of the same type");
    }

    if let Some(selection) =
        left.with_dyn(|lhs| lhs.compare().map(|lhs| lhs.compare(right, operator)))
    {
        return selection;
    }

    if let Some(selection) = right.with_dyn(|rhs| {
        rhs.compare()
            .map(|rhs| rhs.compare(left, operator.inverse()))
    }) {
        return selection;
    }

    // Fallback to arrow on canonical types
    let lhs = left.clone().into_canonical()?.into_arrow();
    let rhs = right.clone().into_canonical()?.into_arrow();

    let array = match operator {
        Operator::Eq => cmp::eq(&lhs.as_ref(), &rhs.as_ref())?,
        Operator::NotEq => cmp::neq(&lhs.as_ref(), &rhs.as_ref())?,
        Operator::Gt => cmp::gt(&lhs.as_ref(), &rhs.as_ref())?,
        Operator::Gte => cmp::gt_eq(&lhs.as_ref(), &rhs.as_ref())?,
        Operator::Lt => cmp::lt(&lhs.as_ref(), &rhs.as_ref())?,
        Operator::Lte => cmp::lt_eq(&lhs.as_ref(), &rhs.as_ref())?,
    };

    Ok(ArrayData::from_arrow(&array, true).into())
}

pub fn scalar_cmp(lhs: &Scalar, rhs: &Scalar, operator: Operator) -> Scalar {
    if lhs.is_null() | rhs.is_null() {
        Scalar::null(DType::Bool(Nullability::Nullable))
    } else {
        let b = match operator {
            Operator::Eq => lhs == rhs,
            Operator::NotEq => lhs != rhs,
            Operator::Gt => lhs > rhs,
            Operator::Gte => lhs >= rhs,
            Operator::Lt => lhs < rhs,
            Operator::Lte => lhs <= rhs,
        };

        Scalar::bool(b, Nullability::Nullable)
    }
}
