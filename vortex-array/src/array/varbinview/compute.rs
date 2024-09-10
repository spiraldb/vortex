use arrow_buffer::BooleanBufferBuilder;
use vortex_buffer::Buffer;
use vortex_error::{vortex_panic, VortexResult};
use vortex_scalar::{Scalar, ScalarValue};

use crate::array::varbin::varbin_scalar;
use crate::array::varbinview::{VarBinViewArray, VIEW_SIZE};
use crate::array::{BoolArray, ConstantArray};
use crate::compute::unary::ScalarAtFn;
use crate::compute::{slice, ArrayCompute, MaybeCompareFn, Operator, SliceFn};
use crate::{Array, ArrayDType, IntoArray};

impl ArrayCompute for VarBinViewArray {
    fn compare(&self, other: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        MaybeCompareFn::maybe_compare(self, other, operator)
    }
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }
}

impl MaybeCompareFn for VarBinViewArray {
    fn maybe_compare(&self, other: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        if let Ok(const_array) = ConstantArray::try_from(other) {
            Some(const_compare(self, const_array, operator))
        } else {
            None
        }
    }
}

fn const_compare(
    binview: &VarBinViewArray,
    other: ConstantArray,
    operator: Operator,
) -> VortexResult<Array> {
    let buffer = match other.scalar().value() {
        // Self::Null => Ok(None), TODO
        ScalarValue::Buffer(b) => b.clone(),
        ScalarValue::BufferString(b) => Buffer::from(b.clone()),
        _ => unreachable!(),
    };

    let mut builder = BooleanBufferBuilder::new(binview.len());
    for (index, view) in binview.view_slice().iter().enumerate() {
        let (data, data_len) = if view.is_inlined() {
            let data = unsafe { view.inlined.data.as_ref() };
            let data_len = unsafe { view.inlined.size };
            (data, data_len as usize)
        } else {
            let data = unsafe { view._ref.prefix.as_ref() };
            let data_len = unsafe { view._ref.size };
            (data, data_len as usize)
        };

        if buffer.len() != data_len {
            builder.append(false);
            continue;
        }

        let scalar_prefix = &buffer[0..data_len];

        let r = match operator {
            Operator::Eq => data == scalar_prefix,
            Operator::NotEq => data != scalar_prefix,
            Operator::Gt => data > scalar_prefix,
            Operator::Gte => data >= scalar_prefix,
            Operator::Lt => data < scalar_prefix,
            Operator::Lte => data <= scalar_prefix,
        };

        if !r {
            builder.append(false);
            continue;
        } else if view.is_inlined() {
            builder.append(true);
            continue;
        } else {
            let bytes = binview.bytes_at(index)?;

            let r = match operator {
                Operator::Eq => bytes.as_slice() == buffer.as_slice(),
                Operator::NotEq => bytes.as_slice() != buffer.as_slice(),
                Operator::Gt => bytes.as_slice() > buffer.as_slice(),
                Operator::Gte => bytes.as_slice() >= buffer.as_slice(),
                Operator::Lt => bytes.as_slice() < buffer.as_slice(),
                Operator::Lte => bytes.as_slice() >= buffer.as_slice(),
            };

            builder.append(r);
        }
    }

    BoolArray::try_new(builder.finish(), binview.validity()).map(|a| a.into_array())
}

impl ScalarAtFn for VarBinViewArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        self.bytes_at(index)
            .map(|bytes| varbin_scalar(Buffer::from(bytes), self.dtype()))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        <Self as ScalarAtFn>::scalar_at(self, index).unwrap_or_else(|err| vortex_panic!(err))
    }
}

impl SliceFn for VarBinViewArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        Ok(Self::try_new(
            slice(&self.views(), start * VIEW_SIZE, stop * VIEW_SIZE)?,
            (0..self.metadata().data_lens.len())
                .map(|i| self.bytes(i))
                .collect::<Vec<_>>(),
            self.dtype().clone(),
            self.validity().slice(start, stop)?,
        )?
        .into_array())
    }
}
