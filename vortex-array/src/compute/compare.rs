use core::fmt;
use std::fmt::{Display, Formatter};

use arrow_ord::cmp;
use vortex_dtype::{DType, NativePType, Nullability};
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::arrow::FromArrowArray;
use crate::{Array, ArrayDType, IntoCanonical};

#[derive(Copy, Clone, Debug, Eq, PartialEq, PartialOrd)]
pub enum Operator {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let display = match &self {
            Operator::Eq => "=",
            Operator::NotEq => "!=",
            Operator::Gt => ">",
            Operator::Gte => ">=",
            Operator::Lt => "<",
            Operator::Lte => "<=",
        };
        Display::fmt(display, f)
    }
}

impl Operator {
    pub fn inverse(self) -> Self {
        match self {
            Operator::Eq => Operator::NotEq,
            Operator::NotEq => Operator::Eq,
            Operator::Gt => Operator::Lte,
            Operator::Gte => Operator::Lt,
            Operator::Lt => Operator::Gte,
            Operator::Lte => Operator::Gt,
        }
    }

    /// Change the sides of the operator, where changing lhs and rhs won't change the result of the operation
    pub fn swap(self) -> Self {
        match self {
            Operator::Eq => Operator::Eq,
            Operator::NotEq => Operator::NotEq,
            Operator::Gt => Operator::Lt,
            Operator::Gte => Operator::Lte,
            Operator::Lt => Operator::Gt,
            Operator::Lte => Operator::Gte,
        }
    }

    pub fn to_fn<T: NativePType>(&self) -> fn(T, T) -> bool {
        match self {
            Operator::Eq => |l, r| l == r,
            Operator::NotEq => |l, r| l != r,
            Operator::Gt => |l, r| l > r,
            Operator::Gte => |l, r| l >= r,
            Operator::Lt => |l, r| l < r,
            Operator::Lte => |l, r| l <= r,
        }
    }
}

pub trait CompareFn {
    fn compare(&self, other: &Array, operator: Operator) -> VortexResult<Array>;
}

pub trait MaybeCompareFn {
    fn maybe_compare(&self, other: &Array, operator: Operator) -> Option<VortexResult<Array>>;
}

pub fn compare(left: &Array, right: &Array, operator: Operator) -> VortexResult<Array> {
    if left.len() != right.len() {
        vortex_bail!("Compare operations only support arrays of the same length");
    }

    // TODO(adamg): This is a placeholder until we figure out type coercion and casting
    if !left.dtype().eq_ignore_nullability(right.dtype()) {
        vortex_bail!("Compare operations only support arrays of the same type");
    }

    if let Some(selection) = left.with_dyn(|lhs| lhs.compare(right, operator)) {
        return selection;
    }

    if let Some(selection) = right.with_dyn(|rhs| rhs.compare(left, operator.swap())) {
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

    Ok(Array::from_arrow(&array, true))
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
