// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Vortex decoders retain native owners and lend the shared row value families.

use ::rowfn::FixedSizeList;
use ::rowfn::InputBinding;
use ::rowfn::RowKind;
use ::rowfn::RowView;
use ::rowfn::TextBinding;
use ::rowfn::TextLayout;
use ::rowfn::TextValue;
use ::rowfn::Utf8;
use vortex_buffer::{BitBuffer, Buffer};
use vortex_error::{VortexResult, vortex_bail, vortex_ensure};

use super::VortexHost;
use crate::{ArrayRef, ExecutionCtx};
use crate::arrays::{BoolArray, FixedSizeListArray, PrimitiveArray};
use crate::arrays::fixed_size_list::{FixedSizeListArrayExt, FixedSizeListArraySlotsExt};
use crate::dtype::{DType, NativePType};
use crate::scalar_fn::unstable::row::{InputElement, Utf8Column, ViewLen};

impl<T> InputBinding<T> for VortexHost
where T: NativePType + for<'a> RowKind<Value<'a> = T> {
    type Decoded = Buffer<T>;
    type View<'a> = &'a [T];
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &DType) -> VortexResult<()> { <T as InputElement>::validate(dtype) }
    fn decode(column: &ArrayRef, scalar: bool, ctx: &mut ExecutionCtx) -> VortexResult<Buffer<T>> {
        let column = super::decoded_input(column, scalar)?;
        Ok(column.execute::<PrimitiveArray>(ctx)?.into_buffer::<T>())
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> VortexResult<bool> { Ok(true) }
    fn view(decoded: &Buffer<T>) -> &[T] { decoded.as_slice() }
}

/// A borrowed Boolean input view with its original bitmap offset.
pub struct BoolView<'a>(&'a BitBuffer);
// SAFETY: BitBuffer retains a stable initialized bit range throughout the borrow.
unsafe impl<'a> RowView<'a, bool> for BoolView<'a> {
    fn len(&self) -> usize { self.0.len() }
    unsafe fn get_unchecked(&self, index: usize) -> bool {
        // SAFETY: the caller bounds the index by this bitmap's logical length.
        unsafe { self.0.value_unchecked(index) }
    }
}
impl InputBinding<bool> for VortexHost {
    type Decoded = BitBuffer;
    type View<'a> = BoolView<'a>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &DType) -> VortexResult<()> { <bool as InputElement>::validate(dtype) }
    fn decode(column: &ArrayRef, scalar: bool, ctx: &mut ExecutionCtx) -> VortexResult<BitBuffer> {
        let column = super::decoded_input(column, scalar)?;
        Ok(column.execute::<BoolArray>(ctx)?.into_bit_buffer())
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> VortexResult<bool> { Ok(true) }
    fn view(decoded: &BitBuffer) -> BoolView<'_> { BoolView(decoded) }
}

/// A validated UTF-8 view borrowed from the existing decoder's retained buffers.
pub struct StringView<'a>(<Utf8Column as InputElement>::View<'a>);
// SAFETY: Utf8Column validates UTF-8 and sanitizes null payloads before lending this exact view.
unsafe impl<'a> RowView<'a, Utf8> for StringView<'a> {
    fn len(&self) -> usize { self.0.len() }
    unsafe fn get_unchecked(&self, index: usize) -> &'a str {
        // SAFETY: the caller supplies an index within the validated retained view.
        unsafe { Utf8Column::get_from_view_unchecked(&self.0, index) }.as_str()
    }
}
impl InputBinding<Utf8> for VortexHost {
    type Decoded = <Utf8Column as InputElement>::Column;
    type View<'a> = StringView<'a>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &DType) -> VortexResult<()> { Utf8Column::validate(dtype) }
    fn decode(column: &ArrayRef, scalar: bool, ctx: &mut ExecutionCtx) -> VortexResult<Self::Decoded> {
        let column = super::decoded_input(column, scalar)?;
        Utf8Column::decode(column, ctx)
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> VortexResult<bool> { Ok(true) }
    fn view(decoded: &Self::Decoded) -> StringView<'_> { StringView(Utf8Column::view(decoded)) }
}

/// Vortex text rows retain the validated string view until bytes are requested.
pub struct VortexText;

impl RowKind for VortexText {
    type Value<'a> = crate::scalar_fn::unstable::row::Utf8View<'a>;
}

impl TextValue for crate::scalar_fn::unstable::row::Utf8View<'_> {
    fn as_str(&self) -> &str { self.as_str() }

    fn byte_len(&self) -> usize { self.raw_view().len() as usize }

    fn prefix(&self, len: usize) -> &[u8] {
        if (self.raw_view().len() as usize) < len { return &[]; }
        if len <= 4 { return &self.prefix()[..len]; }
        &self.as_str().as_bytes()[..len]
    }
}

// SAFETY: the existing decoder validates UTF-8, sanitizes nulls, and retains both headers and data.
unsafe impl<'a> RowView<'a, VortexText> for StringView<'a> {
    fn len(&self) -> usize { self.0.len() }
    unsafe fn get_unchecked(&self, index: usize) -> <VortexText as RowKind>::Value<'a> {
        // SAFETY: the caller supplies an index within the retained validated view.
        unsafe { Utf8Column::get_from_view_unchecked(&self.0, index) }
    }
}

impl InputBinding<VortexText> for VortexHost {
    type Decoded = <Utf8Column as InputElement>::Column;
    type View<'a> = StringView<'a>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &DType) -> VortexResult<()> { <Self as InputBinding<Utf8>>::validate(dtype) }
    fn decode(column: &ArrayRef, scalar: bool, ctx: &mut ExecutionCtx) -> VortexResult<Self::Decoded> {
        <Self as InputBinding<Utf8>>::decode(column, scalar, ctx)
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> VortexResult<bool> { Ok(true) }
    fn view(decoded: &Self::Decoded) -> StringView<'_> { StringView(Utf8Column::view(decoded)) }
}

impl TextBinding for VortexHost {
    type Text = VortexText;
    type Offset32 = VortexText;
    type Offset64 = VortexText;

    fn text_layout(dtype: &DType) -> VortexResult<TextLayout> {
        <Self as InputBinding<VortexText>>::validate(dtype)?;
        Ok(TextLayout::View)
    }
}

/// Retained list children and an explicit count of logical rows.
pub struct ListValues<T> { values: Buffer<T>, width: usize, rows: usize }
/// Borrowed fixed-size row view.
pub struct ListView<'a, T> { values: &'a [T], width: usize, rows: usize }
// SAFETY: decoding validates rows * width against this retained child slice, including width zero.
unsafe impl<'a, T: 'static> RowView<'a, FixedSizeList<T>> for ListView<'a, T> {
    fn len(&self) -> usize { self.rows }
    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> &'a [T] {
        let start = index * self.width;
        // SAFETY: decoded shape and the caller's index establish the full child range.
        unsafe { self.values.get_unchecked(start..start + self.width) }
    }
}
impl<T: NativePType> InputBinding<FixedSizeList<T>> for VortexHost {
    type Decoded = ListValues<T>;
    type View<'a> = ListView<'a, T>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &DType) -> VortexResult<()> {
        let DType::FixedSizeList(child, _, _) = dtype else { vortex_bail!("expected fixed-size list, got {dtype}"); };
        vortex_ensure!(!child.is_nullable(), "fixed-size-list children must be non-nullable");
        <T as InputElement>::validate(child)
    }
    fn decode(column: &ArrayRef, scalar: bool, ctx: &mut ExecutionCtx) -> VortexResult<Self::Decoded> {
        let column = super::decoded_input(column, scalar)?;
        let list = column.execute::<FixedSizeListArray>(ctx)?;
        let width = list.list_size() as usize;
        let rows = list.len();
        vortex_ensure!(list.elements().all_valid(ctx)?, "fixed-size-list children must be valid");
        let values = list.elements().clone().execute::<PrimitiveArray>(ctx)?.into_buffer::<T>();
        vortex_ensure!(rows.checked_mul(width) == Some(values.len()), "fixed-size-list shape mismatch");
        Ok(ListValues { values, width, rows })
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> VortexResult<bool> { Ok(true) }
    fn view(decoded: &Self::Decoded) -> Self::View<'_> {
        ListView { values: decoded.values.as_slice(), width: decoded.width, rows: decoded.rows }
    }
}
