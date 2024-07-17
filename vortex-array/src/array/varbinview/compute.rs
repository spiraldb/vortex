use arrow_array::Array as ArrowArray;
use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use vortex_error::VortexResult;
use vortex_expr::Operator;
use vortex_scalar::Scalar;

use crate::array::varbin::varbin_scalar;
use crate::array::varbinview::{VarBinViewArray, VIEW_SIZE};
use crate::arrow::FromArrowArray;
use crate::compute::unary::scalar_at::ScalarAtFn;
use crate::compute::{slice, ArrayCompute, CompareFn, SliceFn};
use crate::validity::ArrayValidity;
use crate::{Array, ArrayDType, ArrayData, IntoArray, IntoArrayData, IntoCanonical};

impl ArrayCompute for VarBinViewArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn compare(&self) -> Option<&dyn CompareFn> {
        Some(self)
    }
}

impl ScalarAtFn for VarBinViewArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            self.bytes_at(index)
                .map(|bytes| varbin_scalar(bytes, self.dtype()))
        } else {
            Ok(Scalar::null(self.dtype().clone()))
        }
    }
}

impl SliceFn for VarBinViewArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::try_new(
            slice(&self.views(), start * VIEW_SIZE, stop * VIEW_SIZE)?
                .into_array_data()
                .into_array(),
            (0..self.metadata().data_lens.len())
                .map(|i| self.bytes(i))
                .collect::<Vec<_>>(),
            self.dtype().clone(),
            self.validity().slice(start, stop)?,
        )?
        .into_array())
    }
}

impl CompareFn for VarBinViewArray {
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

        let data = ArrayData::from_arrow(&r, r.null_count() > 0);
        Ok(data.into_array())
    }
}
