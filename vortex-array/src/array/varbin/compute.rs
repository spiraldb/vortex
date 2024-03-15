use itertools::Itertools;

use crate::array::bool::BoolArray;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::VarBinArray;
use crate::array::{Array, ArrayRef, CloneOptionalArray};
use crate::compute::as_contiguous::{as_contiguous, AsContiguousFn};
use crate::compute::flatten::{flatten, flatten_primitive, FlattenFn};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::dtype::DType;
use crate::error::VortexResult;
use crate::scalar::{BinaryScalar, Scalar, Utf8Scalar};

impl ArrayCompute for VarBinArray {
    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl AsContiguousFn for VarBinArray {
    fn as_contiguous(&self, arrays: Vec<ArrayRef>) -> VortexResult<ArrayRef> {
        let bytes = as_contiguous(
            arrays
                .iter()
                .map(|a| a.as_varbin().sliced_bytes())
                .try_collect()?,
        )?;

        let validity = as_contiguous(
            arrays
                .iter()
                .map(|a| {
                    a.as_varbin()
                        .validity()
                        .clone_optional()
                        .unwrap_or_else(|| BoolArray::from(vec![true; a.len()]).boxed())
                })
                .collect_vec(),
        )?;

        let mut offsets = Vec::new();
        offsets.push(0);
        for a in arrays.iter().map(|a| a.as_varbin()) {
            let first_offset: u64 = a.first_offset()?;
            // FIXME(ngates): cast to u64, or iterate over the offsets as any?
            let offsets_array = flatten_primitive(a.offsets())?;
            let shift = offsets.last().copied().unwrap_or(0);
            offsets.extend(
                offsets_array
                    .typed_data::<u64>()
                    .iter()
                    .skip(1) // Ignore the zero offset for each array
                    .map(|o| o + shift - first_offset),
            );
        }

        let offsets_array = PrimitiveArray::from(offsets).boxed();

        Ok(VarBinArray::new(offsets_array, bytes, self.dtype.clone(), Some(validity)).boxed())
    }
}

impl FlattenFn for VarBinArray {
    fn flatten(&self) -> VortexResult<ArrayRef> {
        let bytes = flatten(self.bytes())?;
        let offsets = flatten(self.offsets())?;
        let validity = self.validity().map(|v| flatten(v)).transpose()?;
        Ok(VarBinArray::new(offsets, bytes, self.dtype.clone(), validity).boxed())
    }
}

impl ScalarAtFn for VarBinArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        if self.is_valid(index) {
            self.bytes_at(index).map(|bytes| {
                if matches!(self.dtype, DType::Utf8(_)) {
                    unsafe { String::from_utf8_unchecked(bytes) }.into()
                } else {
                    bytes.into()
                }
            })
        } else if matches!(self.dtype, DType::Utf8(_)) {
            Ok(Utf8Scalar::new(None).into())
        } else {
            Ok(BinaryScalar::new(None).into())
        }
    }
}
