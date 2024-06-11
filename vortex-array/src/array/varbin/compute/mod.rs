use std::sync::Arc;

use arrow_array::{
    ArrayRef as ArrowArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
};
use vortex_dtype::DType;
use vortex_dtype::PType;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::arrow::wrappers::as_offset_buffer;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::cast::cast;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::slice::SliceFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::validity::ArrayValidity;
use crate::{ArrayDType, ToArray};

mod slice;
mod take;

impl ArrayCompute for VarBinArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

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

impl AsArrowArray for VarBinArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        // Ensure the offsets are either i32 or i64
        let offsets = self.offsets().flatten_primitive()?;
        let offsets = match offsets.ptype() {
            PType::I32 | PType::I64 => offsets,
            // Unless it's u64, everything else can be converted into an i32.
            // FIXME(ngates): do not copy offsets again
            PType::U64 => cast(&offsets.to_array(), PType::I64.into())?.flatten_primitive()?,
            _ => cast(&offsets.to_array(), PType::I32.into())?.flatten_primitive()?,
        };
        let nulls = self.logical_validity().to_null_buffer()?;

        let data = self.bytes().flatten_primitive()?;
        assert_eq!(data.ptype(), PType::U8);
        let data = data.buffer();

        // Switch on Arrow DType.
        Ok(match self.dtype() {
            DType::Binary(_) => match offsets.ptype() {
                PType::I32 => Arc::new(BinaryArray::new(
                    as_offset_buffer::<i32>(offsets),
                    data.into(),
                    nulls,
                )),
                PType::I64 => Arc::new(LargeBinaryArray::new(
                    as_offset_buffer::<i64>(offsets),
                    data.into(),
                    nulls,
                )),
                _ => panic!("Invalid offsets type"),
            },
            DType::Utf8(_) => match offsets.ptype() {
                PType::I32 => Arc::new(StringArray::new(
                    as_offset_buffer::<i32>(offsets),
                    data.into(),
                    nulls,
                )),
                PType::I64 => Arc::new(LargeStringArray::new(
                    as_offset_buffer::<i64>(offsets),
                    data.into(),
                    nulls,
                )),
                _ => panic!("Invalid offsets type"),
            },
            _ => vortex_bail!(MismatchedTypes: "utf8 or binary", self.dtype()),
        })
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
