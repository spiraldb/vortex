use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, BinaryViewArray, StringViewArray};
use arrow_buffer::Buffer as ArrowBuffer;
use arrow_buffer::ScalarBuffer;
use itertools::Itertools;
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::DType;

use crate::array::varbin::varbin_scalar;
use crate::array::varbinview::{VarBinViewArray, VIEW_SIZE};
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::slice::{slice, SliceFn};
use crate::compute::ArrayCompute;
use crate::ptype::PType;
use crate::scalar::Scalar;
use crate::validity::ArrayValidity;
use crate::{ArrayDType, IntoArray, IntoArrayData, OwnedArray};

impl ArrayCompute for VarBinViewArray<'_> {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }
}

impl ScalarAtFn for VarBinViewArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            self.bytes_at(index)
                .map(|bytes| varbin_scalar(bytes, self.dtype()))
        } else {
            Ok(Scalar::null(self.dtype()))
        }
    }
}

impl AsArrowArray for VarBinViewArray<'_> {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // Views should be buffer of u8
        let views = self.views().flatten_primitive()?;
        assert_eq!(views.ptype(), PType::U8);
        let nulls = self.logical_validity().to_null_buffer()?;

        let data = (0..self.metadata().n_children)
            .map(|i| self.bytes(i).flatten_primitive())
            .collect::<VortexResult<Vec<_>>>()?;
        if !data.is_empty() {
            assert_eq!(data[0].ptype(), PType::U8);
            assert!(data.iter().map(|d| d.ptype()).all_equal());
        }

        let data = data
            .iter()
            .map(|p| ArrowBuffer::from(p.buffer()))
            .collect::<Vec<_>>();

        // Switch on Arrow DType.
        Ok(match self.dtype() {
            DType::Binary(_) => Arc::new(BinaryViewArray::new(
                ScalarBuffer::<u128>::from(ArrowBuffer::from(views.buffer())),
                data,
                nulls,
            )),
            DType::Utf8(_) => Arc::new(StringViewArray::new(
                ScalarBuffer::<u128>::from(ArrowBuffer::from(views.buffer())),
                data,
                nulls,
            )),
            _ => vortex_bail!(MismatchedTypes: "utf8 or binary", self.dtype()),
        })
    }
}

impl SliceFn for VarBinViewArray<'_> {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<OwnedArray> {
        Ok(VarBinViewArray::try_new(
            slice(&self.views(), start * VIEW_SIZE, stop * VIEW_SIZE)?
                .into_array_data()
                .into_array(),
            (0..self.metadata().n_children)
                .map(|i| self.bytes(i))
                .collect::<Vec<_>>(),
            self.dtype().clone(),
            self.validity().slice(start, stop)?,
        )?
        .into_array())
    }
}
