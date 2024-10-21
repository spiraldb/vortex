use std::sync::Arc;

use arrow_array::{ArrayRef, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
use vortex_dtype::{DType, PType};
use vortex_error::{vortex_bail, VortexResult, VortexUnwrap as _};
use vortex_scalar::Scalar;

use crate::array::varbin::{varbin_scalar, VarBinArray};
use crate::arrow::wrappers::as_offset_buffer;
use crate::compute::unary::{try_cast, ScalarAtFn};
use crate::compute::{ArrayCompute, FilterFn, MaybeCompareFn, Operator, SliceFn, TakeFn};
use crate::validity::ArrayValidity;
use crate::{Array, ArrayDType, IntoArrayVariant, ToArray};

mod compare;
mod filter;
mod slice;
mod take;

impl ArrayCompute for VarBinArray {
    fn compare(&self, other: &Array, operator: Operator) -> Option<VortexResult<Array>> {
        MaybeCompareFn::maybe_compare(self, other, operator)
    }

    fn filter(&self) -> Option<&dyn FilterFn> {
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

impl ScalarAtFn for VarBinArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(varbin_scalar(self.bytes_at(index)?, self.dtype()))
    }

    fn scalar_at_unchecked(&self, index: usize) -> Scalar {
        varbin_scalar(self.bytes_at(index).vortex_unwrap(), self.dtype())
    }
}

/// Convert the array to Arrow variable length binary array type.
fn varbin_to_arrow(varbin_array: &VarBinArray) -> VortexResult<ArrayRef> {
    let offsets = varbin_array
        .offsets()
        .into_primitive()
        .map_err(|err| err.with_context("Failed to canonicalize offsets"))?;
    let offsets = match offsets.ptype() {
        PType::I32 | PType::I64 => offsets,
        PType::U64 => offsets.reinterpret_cast(PType::I64),
        PType::U32 => offsets.reinterpret_cast(PType::I32),
        // Unless it's u64, everything else can be converted into an i32.
        _ => try_cast(offsets.to_array(), PType::I32.into())
            .and_then(|a| a.into_primitive())
            .map_err(|err| err.with_context("Failed to cast offsets to PrimitiveArray of i32"))?,
    };
    let nulls = varbin_array
        .logical_validity()
        .to_null_buffer()
        .map_err(|err| err.with_context("Failed to get null buffer from logical validity"))?;

    let data = varbin_array
        .bytes()
        .into_primitive()
        .map_err(|err| err.with_context("Failed to canonicalize bytes"))?;
    if data.ptype() != PType::U8 {
        vortex_bail!("Expected bytes to be of type U8, got {}", data.ptype());
    }
    let data = data.buffer();

    // Switch on Arrow DType.
    Ok(match varbin_array.dtype() {
        DType::Binary(_) => match offsets.ptype() {
            PType::I32 => Arc::new(unsafe {
                BinaryArray::new_unchecked(
                    as_offset_buffer::<i32>(offsets),
                    data.clone().into_arrow(),
                    nulls,
                )
            }),
            PType::I64 => Arc::new(unsafe {
                LargeBinaryArray::new_unchecked(
                    as_offset_buffer::<i64>(offsets),
                    data.clone().into_arrow(),
                    nulls,
                )
            }),
            _ => vortex_bail!("Invalid offsets type {}", offsets.ptype()),
        },
        DType::Utf8(_) => match offsets.ptype() {
            PType::I32 => Arc::new(unsafe {
                StringArray::new_unchecked(
                    as_offset_buffer::<i32>(offsets),
                    data.clone().into_arrow(),
                    nulls,
                )
            }),
            PType::I64 => Arc::new(unsafe {
                LargeStringArray::new_unchecked(
                    as_offset_buffer::<i64>(offsets),
                    data.clone().into_arrow(),
                    nulls,
                )
            }),
            _ => vortex_bail!("Invalid offsets type {}", offsets.ptype()),
        },
        _ => vortex_bail!(
            "expected utf8 or binary instead of {}",
            varbin_array.dtype()
        ),
    })
}
