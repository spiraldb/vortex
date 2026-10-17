// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! One-time downcasts retain Arrow owners and borrow compatible primitive, bit, and list storage.

use arrow_array::{Array, ArrayRef, BooleanArray, FixedSizeListArray, PrimitiveArray};
use arrow_array::types::*;
use arrow_buffer::{ArrowNativeType, BooleanBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field};
use rowfn::{FixedSizeList, Host, InputBinding, RowKind, RowView};
use rowfn::kernels::bit::BitmapView;

use super::{ArrowHost, batch::ordinary};

/// Primitive Rust-to-Arrow mapping local to this adapter.
pub trait ArrowPrimitive: ArrowNativeType + Copy + Default + for<'a> RowKind<Value<'a> = Self> {
    /// Corresponding Arrow physical primitive type.
    type Arrow: ArrowPrimitiveType<Native = Self>;
}
macro_rules! primitives {
    ($($native:ty => $arrow:ty),+ $(,)?) => { $(impl ArrowPrimitive for $native { type Arrow = $arrow; })+ };
}
primitives!(i8 => Int8Type, i16 => Int16Type, i32 => Int32Type, i64 => Int64Type,
    u8 => UInt8Type, u16 => UInt16Type, u32 => UInt32Type, u64 => UInt64Type,
    f32 => Float32Type, f64 => Float64Type);

impl<T: ArrowPrimitive> InputBinding<T> for ArrowHost {
    type Decoded = ScalarBuffer<T>;
    type View<'a> = &'a [T];
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    const PREFER_LINEAR_OUTPUT: bool = true;
    fn validate(dtype: &Field) -> Result<(), ArrowError> {
        ordinary(dtype)?;
        if dtype.data_type() != &T::Arrow::DATA_TYPE { return Err(Self::error("primitive semantic type mismatch")); }
        Ok(())
    }
    fn decode(column: &ArrayRef, _: bool, _: &mut ()) -> Result<ScalarBuffer<T>, ArrowError> {
        let array = column.as_any().downcast_ref::<PrimitiveArray<T::Arrow>>()
            .ok_or_else(|| Self::error("primitive storage downcast failed"))?;
        Ok(array.values().clone())
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> Result<bool, ArrowError> { Ok(true) }
    fn view(decoded: &ScalarBuffer<T>) -> &[T] { decoded.as_ref() }
}

/// Borrowed logical Boolean rows, retaining the original byte offset.
pub struct BoolRows<'a>(BitmapView<'a>);

// SAFETY: BitmapView validates the exact borrowed bit range and retains its bytes for 'a.
unsafe impl<'a> RowView<'a, bool> for BoolRows<'a> {
    fn len(&self) -> usize { self.0.len() }
    unsafe fn get_unchecked(&self, index: usize) -> bool {
        // SAFETY: the caller bounds index by this retained bitmap's validated logical length.
        unsafe { self.0.value_unchecked(index) }
    }
}
impl InputBinding<bool> for ArrowHost {
    type Decoded = BooleanBuffer;
    type View<'a> = BoolRows<'a>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &Field) -> Result<(), ArrowError> {
        ordinary(dtype)?;
        if dtype.data_type() != &DataType::Boolean { return Err(Self::error("expected Boolean input")); }
        Ok(())
    }
    fn decode(column: &ArrayRef, _: bool, _: &mut ()) -> Result<BooleanBuffer, ArrowError> {
        Ok(column.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| Self::error("Boolean storage downcast failed"))?.values().clone())
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> Result<bool, ArrowError> { Ok(true) }
    fn view(decoded: &BooleanBuffer) -> BoolRows<'_> {
        BoolRows(BitmapView::new(decoded.values(), decoded.offset(), decoded.len()))
    }
}

/// Retained primitive children with explicit row count for zero-width lists.
pub struct ListValues<T: ArrowPrimitive> { values: ScalarBuffer<T>, rows: usize, width: usize }
/// A borrowed view of fixed-width child slices.
pub struct ListView<'a, T> { values: &'a [T], rows: usize, width: usize }
// SAFETY: decoding validates the complete child range, and this view retains its exact shape.
unsafe impl<'a, T: 'static> RowView<'a, FixedSizeList<T>> for ListView<'a, T> {
    fn len(&self) -> usize { self.rows }
    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> &'a [T] {
        let start = index * self.width;
        // SAFETY: caller bounds index by rows, and decoding checked rows * width.
        unsafe { self.values.get_unchecked(start..start + self.width) }
    }
}
impl<T: ArrowPrimitive> InputBinding<FixedSizeList<T>> for ArrowHost {
    type Decoded = ListValues<T>;
    type View<'a> = ListView<'a, T>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &Field) -> Result<(), ArrowError> {
        ordinary(dtype)?;
        let DataType::FixedSizeList(child, width) = dtype.data_type() else { return Err(Self::error("expected fixed-size list")); };
        if child.is_nullable() || *width < 0 { return Err(Self::error("list width must be nonnegative and children non-nullable")); }
        <Self as InputBinding<T>>::validate(child)
    }
    fn decode(column: &ArrayRef, _: bool, _: &mut ()) -> Result<Self::Decoded, ArrowError> {
        let array = column.as_any().downcast_ref::<FixedSizeListArray>().ok_or_else(|| Self::error("list storage downcast failed"))?;
        let child = array.values().as_any().downcast_ref::<PrimitiveArray<T::Arrow>>().ok_or_else(|| Self::error("list child storage downcast failed"))?;
        let width = usize::try_from(array.value_length()).map_err(|_| Self::error("negative list width"))?;
        if child.logical_null_count() != 0 || array.len().checked_mul(width) != Some(child.len()) {
            return Err(Self::error("list child validity or shape mismatch"));
        }
        Ok(ListValues { values: child.values().clone(), rows: array.len(), width })
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> Result<bool, ArrowError> { Ok(true) }
    fn view(decoded: &Self::Decoded) -> Self::View<'_> {
        ListView { values: decoded.values.as_ref(), rows: decoded.rows, width: decoded.width }
    }
}
