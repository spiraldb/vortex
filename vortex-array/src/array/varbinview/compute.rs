use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::ByteViewType;
use arrow_array::{Datum, GenericByteViewArray};
use arrow_ord::cmp;
use arrow_schema::DataType;
use vortex_buffer::Buffer;
use vortex_error::{vortex_bail, VortexResult, VortexUnwrap};
use vortex_scalar::Scalar;

use crate::array::varbin::varbin_scalar;
use crate::array::varbinview::{VarBinViewArray, VIEW_SIZE_BYTES};
use crate::array::{varbinview_as_arrow, ConstantArray};
use crate::arrow::FromArrowArray;
use crate::compute::unary::ScalarAtFn;
use crate::compute::{slice, ArrayCompute, MaybeCompareFn, Operator, SliceFn, TakeFn};
use crate::{Array, ArrayDType, IntoArray, IntoCanonical};

impl ArrayCompute for VarBinViewArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl ScalarAtFn for VarBinViewArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        self.bytes_at(index)
            .map(|bytes| varbin_scalar(Buffer::from(bytes), self.dtype()))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        <Self as ScalarAtFn>::scalar_at(self, index).vortex_unwrap()
    }
}

impl SliceFn for VarBinViewArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::try_new(
            slice(
                self.views(),
                start * VIEW_SIZE_BYTES,
                stop * VIEW_SIZE_BYTES,
            )?,
            (0..self.metadata().buffer_lens.len())
                .map(|i| self.buffer(i))
                .collect::<Vec<_>>(),
            self.dtype().clone(),
            self.validity().slice(start, stop)?,
        )?
        .into_array())
    }
}

/// Take involves creating a new array that references the old array, just with the given set of views.
impl TakeFn for VarBinViewArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let array_ref = varbinview_as_arrow(self);
        let indices_arrow = indices.clone().into_canonical()?.into_arrow()?;

        let take_arrow = arrow_select::take::take(&array_ref, &indices_arrow, None)?;
        let nullable = take_arrow.is_nullable();
        Ok(Array::from_arrow(take_arrow, nullable))
    }
}

impl MaybeCompareFn for VarBinViewArray {
    fn maybe_compare(&self, other: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        if let Ok(rhs_const) = ConstantArray::try_from(other) {
            Some(compare_constant(self, &rhs_const, operator))
        } else {
            None
        }
    }
}

fn compare_constant(
    lhs: &VarBinViewArray,
    rhs: &ConstantArray,
    operator: Operator,
) -> VortexResult<Array> {
    let arrow_lhs = lhs.clone().into_canonical()?.into_arrow()?;
    let constant = Arc::<dyn Datum>::try_from(&rhs.owned_scalar())?;

    match arrow_lhs.data_type() {
        DataType::BinaryView => {
            compare_constant_arrow(arrow_lhs.as_binary_view(), constant, operator)
        }
        DataType::Utf8View => {
            compare_constant_arrow(arrow_lhs.as_string_view(), constant, operator)
        }
        _ => {
            vortex_bail!("Cannot compare VarBinViewArray with non-binary type");
        }
    }
}

fn compare_constant_arrow<T: ByteViewType>(
    lhs: &GenericByteViewArray<T>,
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
    use vortex_dtype::Nullability;
    use vortex_scalar::Scalar;

    use crate::array::varbinview::compute::compare_constant;
    use crate::array::{ConstantArray, VarBinViewArray};
    use crate::compute::Operator;
    use crate::IntoArrayVariant;

    #[test]
    fn basic_test() {
        let arr = VarBinViewArray::from_iter_nullable_str([
            Some("one"),
            Some("two"),
            Some("three"),
            Some("four"),
            Some("five"),
            Some("six"),
        ]);

        let s = Scalar::utf8("seven".to_string(), Nullability::Nullable);

        let constant_array = ConstantArray::new(s, arr.len());

        let r = compare_constant(&arr, &constant_array, Operator::Eq)
            .unwrap()
            .into_bool()
            .unwrap();

        assert!(r.boolean_buffer().iter().all(|v| !v));
    }
}
