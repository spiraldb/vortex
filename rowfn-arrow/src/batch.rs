// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Explicit dictionary materialization and strict Arrow validity.

use std::sync::Arc;

use arrow_array::{Array, ArrayRef, DictionaryArray, UInt64Array, make_array, new_null_array};
use arrow_array::{BooleanArray, PrimitiveArray, StringViewArray};
use arrow_array::types::*;
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_schema::{ArrowError, DataType, Field};
use arrow_select::{filter::filter, take::take};
use rowfn::{BatchBinding, Host, Operand, Selection, TypeBinding, ValiditySummary};
use rowfn::kernels::bit::BitmapView;

use super::ArrowHost;

pub(crate) fn ordinary(field: &Field) -> Result<(), ArrowError> {
    if field.metadata().contains_key("ARROW:extension:name") {
        return Err(ArrowHost::error("unknown Arrow extension requires an explicit semantic binding"));
    }
    Ok(())
}

/// Replace dictionary storage with its supported logical value type, preserving field metadata.
pub fn logical_field(field: &Field) -> Result<Field, ArrowError> {
    let DataType::Dictionary(_, value) = field.data_type() else { return Ok(field.clone()); };
    if !matches!(value.as_ref(), DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64
        | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
        | DataType::Float32 | DataType::Float64 | DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) {
        return Err(ArrowHost::error("dictionary values must be supported primitive or string storage"));
    }
    Ok(field.clone().with_data_type(value.as_ref().clone()))
}

/// Materialize only referenced dictionary rows using Arrow's null-preserving take kernel.
pub fn materialize(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let DataType::Dictionary(key, _) = array.data_type() else { return Ok(array.clone()); };
    logical_field(&Field::new("input", array.data_type().clone(), true))?;
    macro_rules! materialize_key {
        ($key:ty) => {{
            let dict = array.as_any().downcast_ref::<DictionaryArray<$key>>()
                .ok_or_else(|| ArrowHost::error("dictionary storage does not match its key type"))?;
            take(dict.values().as_ref(), dict.keys(), None)
        }};
    }
    match key.as_ref() {
        DataType::Int8 => materialize_key!(Int8Type),
        DataType::Int16 => materialize_key!(Int16Type),
        DataType::Int32 => materialize_key!(Int32Type),
        DataType::Int64 => materialize_key!(Int64Type),
        DataType::UInt8 => materialize_key!(UInt8Type),
        DataType::UInt16 => materialize_key!(UInt16Type),
        DataType::UInt32 => materialize_key!(UInt32Type),
        DataType::UInt64 => materialize_key!(UInt64Type),
        _ => Err(ArrowHost::error("unsupported dictionary key type")),
    }
}

impl TypeBinding for ArrowHost {
    fn nullable(dtype: &Field) -> bool { dtype.is_nullable() }
    fn with_nullable(dtype: &Field, nullable: bool) -> Field { dtype.clone().with_nullable(nullable) }
    fn validate_label(storage: &Field, output: &Field) -> Result<(), ArrowError> {
        ordinary(storage)?;
        ordinary(output)?;
        if storage.data_type() == output.data_type()
            || matches!((storage.data_type(), output.data_type()), (DataType::Int64, DataType::Timestamp(_, _))) {
            return Ok(());
        }
        Err(Self::error("output label must preserve Arrow storage without conversion"))
    }
}

/// Retained validity and row count for Arrow batches without a null buffer.
pub struct ArrowValidity { nulls: Option<NullBuffer>, rows: usize }
// SAFETY: validity construction checks each operand length. The immutable null buffer and row
// count define one stable domain, and bitmap traversal visits each set bit once in order.
unsafe impl Selection for ArrowValidity {
    fn len(&self) -> usize { self.rows }
    fn count(&self) -> usize { self.rows - self.nulls.as_ref().map_or(0, NullBuffer::null_count) }
    fn try_for_each<E>(&self, mut visit: impl FnMut(usize) -> Result<(), E>) -> Result<(), E> {
        match &self.nulls {
            None => (0..self.rows).try_for_each(visit),
            Some(nulls) => {
                let bits = nulls.inner();
                BitmapView::new(bits.values(), bits.offset(), bits.len()).try_for_each(&mut visit)
            }
        }
    }
}
impl BatchBinding for ArrowHost {
    type Validity = ArrowValidity;
    type Selection = ArrowValidity;
    fn validate_operand(input: &Operand<Self>, rows: usize) -> Result<(), ArrowError> {
        let expected = if input.scalar { 1 } else { rows };
        if input.column.len() != expected || input.column.data_type() != input.dtype.data_type() {
            return Err(Self::error("Arrow operand length or field does not match its invocation"));
        }
        if !input.dtype.is_nullable() && input.column.logical_null_count() != 0 {
            return Err(Self::error("non-nullable input field cannot contain logical nulls"));
        }
        Ok(())
    }
    fn validity(inputs: &[Operand<Self>], rows: usize, _: &mut ()) -> Result<ArrowValidity, ArrowError> {
        let mut result = None;
        for input in inputs {
            Self::validate_operand(input, rows)?;
            let nulls = input.column.logical_nulls();
            let expected = if input.scalar { 1 } else { rows };
            if nulls.as_ref().is_some_and(|nulls| nulls.len() != expected) {
                return Err(Self::error("Arrow logical validity must match its operand length"));
            }
            if input.scalar {
                if nulls.as_ref().is_some_and(|nulls| nulls.is_null(0)) {
                    return Ok(ArrowValidity { nulls: Some(NullBuffer::new_null(rows)), rows });
                }
            } else {
                result = NullBuffer::union(result.as_ref(), nulls.as_ref());
            }
        }
        Ok(ArrowValidity { nulls: result, rows })
    }
    fn validity_summary(validity: &ArrowValidity) -> ValiditySummary {
        let count = validity.count();
        if count == validity.rows { ValiditySummary::All }
        else if count == 0 { ValiditySummary::None }
        else { ValiditySummary::Unknown }
    }
    fn selection(validity: &ArrowValidity, _: usize, _: &mut ()) -> Result<ArrowValidity, ArrowError> {
        Ok(ArrowValidity { nulls: validity.nulls.clone(), rows: validity.rows })
    }
    fn filter(input: &Operand<Self>, selection: &ArrowValidity, _: &mut ()) -> Result<Operand<Self>, ArrowError> {
        let column = if input.scalar { input.column.clone() } else {
            let bits = selection.nulls.as_ref().map(|nulls| nulls.inner().clone())
                .unwrap_or_else(|| BooleanBuffer::new_set(selection.rows));
            filter(input.column.as_ref(), &BooleanArray::new(bits, None))?
        };
        Ok(Operand { column, dtype: input.dtype.clone(), scalar: input.scalar })
    }
    fn all_null(dtype: &Field, rows: usize, _: &mut ()) -> Result<ArrayRef, ArrowError> {
        Ok(new_null_array(dtype.data_type(), rows))
    }
    fn broadcast(column: ArrayRef, rows: usize, _: &mut ()) -> Result<ArrayRef, ArrowError> {
        if column.len() != 1 || column.logical_null_count() != 0 {
            return Err(Self::error("scalar output must contain one valid row"));
        }
        macro_rules! primitive {
            ($type:ty) => {{
                let array = column.as_any().downcast_ref::<PrimitiveArray<$type>>()
                    .ok_or_else(|| Self::error("scalar primitive storage downcast failed"))?;
                return Ok(Arc::new(PrimitiveArray::<$type>::new(vec![array.value(0); rows].into(), None)));
            }};
        }
        match column.data_type() {
            DataType::Int8 => primitive!(Int8Type),
            DataType::Int16 => primitive!(Int16Type),
            DataType::Int32 => primitive!(Int32Type),
            DataType::Int64 => primitive!(Int64Type),
            DataType::UInt8 => primitive!(UInt8Type),
            DataType::UInt16 => primitive!(UInt16Type),
            DataType::UInt32 => primitive!(UInt32Type),
            DataType::UInt64 => primitive!(UInt64Type),
            DataType::Float32 => primitive!(Float32Type),
            DataType::Float64 => primitive!(Float64Type),
            DataType::Boolean => {
                let array = column.as_any().downcast_ref::<BooleanArray>()
                    .ok_or_else(|| Self::error("scalar Boolean storage downcast failed"))?;
                let bits = if array.value(0) { BooleanBuffer::new_set(rows) } else { BooleanBuffer::new_unset(rows) };
                return Ok(Arc::new(BooleanArray::new(bits, None)));
            }
            DataType::Utf8View => {
                let array = column.as_any().downcast_ref::<StringViewArray>()
                    .ok_or_else(|| Self::error("scalar UTF-8 storage downcast failed"))?;
                // SAFETY: the one non-null input view is valid. Every copied view retains the
                // same backing buffers, and no offsets or lengths change during repetition.
                return Ok(Arc::new(unsafe {
                    StringViewArray::new_unchecked(vec![array.views()[0]; rows].into(), array.data_buffers(), None)
                }));
            }
            _ => {}
        }
        take(column.as_ref(), &UInt64Array::from(vec![0; rows]), None)
    }
    fn publish(column: ArrayRef, storage: &Field, output: &Field, validity: &ArrowValidity,
        rows: usize, _: &mut ()) -> Result<ArrayRef, ArrowError> {
        Self::validate_label(storage, output)?;
        let data = column.to_data();
        if data.len() != rows || data.data_type() != storage.data_type() || data.null_count() != 0 {
            return Err(Self::error("row output must contain all-valid planned storage with the logical row count"));
        }
        if validity.rows != rows || validity.nulls.as_ref().is_some_and(|nulls| nulls.len() != rows)
            || (!output.is_nullable() && validity.count() != rows) {
            return Err(Self::error("output validity must match the row count and planned nullability"));
        }
        let builder = data.into_builder().data_type(output.data_type().clone())
            .nulls(validity.nulls.clone());
        // SAFETY: column already owns valid Arrow data. validate_label permits only an identical
        // type or Int64-to-Timestamp, which has the same layout and accepts every i64. The private
        // validity retains exactly rows bits. Changing outer nulls preserves all child invariants.
        let data = unsafe { builder.build_unchecked() };
        Ok(make_array(data))
    }
}
