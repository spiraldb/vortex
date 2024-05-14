use std::sync::Arc;

use arrow_array::{
    ArrayRef as ArrowArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray,
};
use itertools::Itertools;
use vortex_dtype::DType;
use vortex_dtype::PType;
use vortex_error::{vortex_bail, VortexResult};
use vortex_scalar::Scalar;

use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::arrow::wrappers::as_offset_buffer;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::cast::cast;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::slice::SliceFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::validity::{ArrayValidity, Validity};
use crate::{Array, ArrayDType, IntoArray, ToArray};

mod slice;
mod take;

impl ArrayCompute for VarBinArray {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
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

impl AsContiguousFn for VarBinArray {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<Array> {
        let bytes_chunks: Vec<Array> = arrays
            .iter()
            .map(|a| VarBinArray::try_from(a).unwrap().sliced_bytes())
            .try_collect()?;
        let bytes = as_contiguous(&bytes_chunks)?;

        let validity = if self.dtype().is_nullable() {
            Validity::from_iter(arrays.iter().map(|a| a.with_dyn(|a| a.logical_validity())))
        } else {
            Validity::NonNullable
        };

        let mut offsets = Vec::new();
        offsets.push(0);
        for a in arrays.iter().map(|a| VarBinArray::try_from(a).unwrap()) {
            let first_offset: u64 = a.first_offset()?;
            let offsets_array = cast(&a.offsets(), PType::U64.into())?.flatten_primitive()?;
            let shift = offsets.last().copied().unwrap_or(0);
            offsets.extend(
                offsets_array
                    .typed_data::<u64>()
                    .iter()
                    .skip(1) // Ignore the zero offset for each array
                    .map(|o| o + shift - first_offset),
            );
        }

        let offsets_array = PrimitiveArray::from(offsets).into_array();

        VarBinArray::try_new(offsets_array, bytes, self.dtype().clone(), validity)
            .map(|a| a.into_array())
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
