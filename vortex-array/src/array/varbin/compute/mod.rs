use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use vortex_error::VortexResult;
use vortex_expr::Operator;
use vortex_scalar::Scalar;

use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::arrow::FromArrowArray;
use crate::compute::unary::scalar_at::ScalarAtFn;
use crate::compute::{ArrayCompute, CompareFn, SliceFn, TakeFn};
use crate::validity::ArrayValidity;
use crate::{Array, ArrayDType, ArrayData, IntoArray, IntoCanonical};

mod slice;
mod take;

impl ArrayCompute for VarBinArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }

    fn compare(&self) -> Option<&dyn CompareFn> {
        Some(self)
    }
}

impl ScalarAtFn for VarBinArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            Ok(varbin_scalar(
                self.bytes_at(index)?
                    // TODO(ngates): update to use buffer when we refactor scalars.
                    .into_vec()
                    .unwrap_or_else(|b| b.as_ref().to_vec()),
                self.dtype(),
            ))
        } else {
            Ok(Scalar::null(self.dtype().clone()))
        }
    }
}

impl CompareFn for VarBinArray {
    fn compare(&self, other: &Array, operator: Operator) -> VortexResult<Array> {
        let lhs = self.clone().into_canonical()?.into_arrow();
        let rhs = other.clone().into_canonical()?.into_arrow();

        let r = match operator {
            Operator::Eq => eq(&lhs.as_ref(), &rhs.as_ref())?,
            Operator::NotEq => neq(&lhs.as_ref(), &rhs.as_ref())?,
            Operator::Gt => gt(&lhs.as_ref(), &rhs.as_ref())?,
            Operator::Gte => gt_eq(&lhs.as_ref(), &rhs.as_ref())?,
            Operator::Lt => lt(&lhs.as_ref(), &rhs.as_ref())?,
            Operator::Lte => lt_eq(&lhs.as_ref(), &rhs.as_ref())?,
        };

        let data = ArrayData::from_arrow(&r, true);
        Ok(data.into_array())
    }
}
