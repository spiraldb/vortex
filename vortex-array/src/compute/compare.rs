use arrow_ord::cmp;
use vortex_error::VortexResult;
use vortex_expr::Operator;

use crate::arrow::FromArrowArray;
use crate::{Array, ArrayData, IntoArray, IntoCanonical};

pub trait CompareFn {
    fn compare(&self, array: &Array, operator: Operator) -> VortexResult<Array>;
}

pub fn compare(left: &Array, right: &Array, operator: Operator) -> VortexResult<Array> {
    if let Some(selection) =
        left.with_dyn(|lhs| lhs.compare().map(|lhs| lhs.compare(right, operator)))
    {
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

    Ok(ArrayData::from_arrow(&array, true).into_array())
}
