// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Borrowed Arrow strings and an independently owned Utf8View output arena.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow_array::Array;
use arrow_array::ArrayRef;
use arrow_array::GenericStringArray;
use arrow_array::LargeStringArray;
use arrow_array::OffsetSizeTrait;
use arrow_array::StringArray;
use arrow_array::StringViewArray;
use arrow_buffer::Buffer;
use arrow_buffer::MutableBuffer;
use arrow_buffer::ScalarBuffer;
use arrow_data::ByteView;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::Field;
use rowfn::Host;
use rowfn::InputBinding;
use rowfn::RowKind;
use rowfn::RowView;
use rowfn::TextBinding;
use rowfn::TextLayout;
use rowfn::TextValue;
use rowfn::Utf8;
use rowfn::sink::OutputSink;
use rowfn::sink::Utf8Output;
use rowfn::sink::WriteUtf8;

use super::{ArrowHost, batch::ordinary};

/// Retained decoded owners for the three supported Arrow UTF-8 layouts.
pub enum Strings {
    /// 32-bit offsets.
    Utf8(StringArray),
    /// 64-bit offsets.
    Large(LargeStringArray),
    /// Inline or buffer-referencing views.
    View(StringViewArray),
}
/// A borrowed string view whose owner remains in the current invocation.
pub struct StringRows<'a>(&'a Strings);
// SAFETY: Arrow string arrays validate UTF-8 and offsets for every slot, including null slots.
// The retained array owns those bytes for the full view lifetime. Outer nulls are applied later.
unsafe impl<'a> RowView<'a, Utf8> for StringRows<'a> {
    fn len(&self) -> usize {
        match self.0 { Strings::Utf8(a) => a.len(), Strings::Large(a) => a.len(), Strings::View(a) => a.len() }
    }
    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> &'a str {
        // SAFETY: the caller bounds index by the retained array length. Arrow guarantees valid
        // string payloads even for null slots, as required by its safe value accessor.
        match self.0 {
            Strings::Utf8(a) => unsafe { a.value_unchecked(index) },
            Strings::Large(a) => unsafe { a.value_unchecked(index) },
            Strings::View(a) => unsafe { a.value_unchecked(index) },
        }
    }
}
impl InputBinding<Utf8> for ArrowHost {
    type Decoded = Strings;
    type View<'a> = StringRows<'a>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &Field) -> Result<(), ArrowError> {
        ordinary(dtype)?;
        if !matches!(dtype.data_type(), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) {
            return Err(Self::error("expected Utf8, LargeUtf8, or Utf8View"));
        }
        Ok(())
    }
    fn decode(column: &ArrayRef, _: bool, _: &mut ()) -> Result<Strings, ArrowError> {
        if let Some(array) = column.as_any().downcast_ref::<StringArray>() { return Ok(Strings::Utf8(array.clone())); }
        if let Some(array) = column.as_any().downcast_ref::<LargeStringArray>() { return Ok(Strings::Large(array.clone())); }
        if let Some(array) = column.as_any().downcast_ref::<StringViewArray>() { return Ok(Strings::View(array.clone())); }
        Err(Self::error("UTF-8 storage downcast failed"))
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> Result<bool, ArrowError> { Ok(true) }
    fn view(decoded: &Strings) -> StringRows<'_> { StringRows(decoded) }
}

/// A concrete offset layout for text operations, without per-row storage dispatch.
pub struct OffsetText<O>(PhantomData<O>);

impl<O: OffsetSizeTrait> RowKind for OffsetText<O> {
    type Value<'a> = OffsetTextValue<'a, O>;
}

/// A borrowed offset pair whose bytes remain in the decoded owner's storage.
pub struct OffsetTextValue<'a, O> {
    start: O,
    end: O,
    bytes: &'a [u8],
}

impl<O: OffsetSizeTrait> TextValue for OffsetTextValue<'_, O> {
    #[inline]
    fn byte_len(&self) -> usize {
        // The validated offsets are nonnegative and monotonic, so native-width subtraction fits.
        (self.end - self.start).as_usize()
    }

    #[inline]
    fn as_str(&self) -> &str {
        // SAFETY: the private constructor copies a validated offset pair from its matching owner.
        // Arrow establishes valid UTF-8 even at null rows, and the bytes outlive this value.
        let bytes = unsafe { self.bytes.get_unchecked(self.start.as_usize()..self.end.as_usize()) };
        // SAFETY: the complete retained offset range is valid UTF-8.
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }
}

/// Raw offset and byte slices borrowed once from a decoded string array.
pub struct OffsetTextRows<'a, O> {
    offsets: &'a [O],
    bytes: &'a [u8],
}

// SAFETY: construction borrows the validated array's matching offsets and bytes. The offsets have
// one sentinel beyond the row domain, and every range denotes valid UTF-8, including null slots.
unsafe impl<'a, O: OffsetSizeTrait> RowView<'a, OffsetText<O>> for OffsetTextRows<'a, O> {
    fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> OffsetTextValue<'a, O> {
        // SAFETY: the caller bounds index by the row count, leaving room for the trailing offset.
        let (start, end) = unsafe {
            (
                *self.offsets.get_unchecked(index),
                *self.offsets.get_unchecked(index + 1),
            )
        };

        OffsetTextValue {
            start,
            end,
            bytes: self.bytes,
        }
    }
}

impl<O: OffsetSizeTrait> InputBinding<OffsetText<O>> for ArrowHost {
    type Decoded = GenericStringArray<O>;
    type View<'a> = OffsetTextRows<'a, O>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;

    fn validate(dtype: &Field) -> Result<(), ArrowError> {
        ordinary(dtype)?;
        let expected = if O::IS_LARGE {
            DataType::LargeUtf8
        } else {
            DataType::Utf8
        };
        if dtype.data_type() != &expected {
            return Err(Self::error("string input must match the selected offset width"));
        }

        Ok(())
    }

    fn decode(column: &ArrayRef, _: bool, _: &mut ()) -> Result<Self::Decoded, ArrowError> {
        column
            .as_any()
            .downcast_ref::<GenericStringArray<O>>()
            .cloned()
            .ok_or_else(|| Self::error("offset string storage downcast failed"))
    }

    fn can_decode_null_tolerant(_: &ArrayRef) -> Result<bool, ArrowError> {
        Ok(true)
    }

    fn view(decoded: &Self::Decoded) -> Self::View<'_> {
        OffsetTextRows {
            offsets: decoded.value_offsets(),
            bytes: decoded.value_data(),
        }
    }
}

/// Arrow's text family preserves view headers for byte-prefix and byte-suffix operations.
pub struct ArrowText;

impl RowKind for ArrowText {
    type Value<'a> = ArrowTextValue<'a>;
}

/// A borrowed Arrow view whose header and referenced UTF-8 are valid, including at null rows.
pub struct ArrowTextValue<'a> {
    view: &'a u128,
    buffers: &'a [Buffer],
}

impl TextValue for ArrowTextValue<'_> {
    #[inline]
    fn byte_len(&self) -> usize { *self.view as u32 as usize }

    #[inline]
    fn inline_eq_key(&self) -> Option<u128> {
        // Arrow validation requires zero padding after the last inline byte.
        (self.byte_len() <= 12).then_some(*self.view)
    }

    #[inline]
    fn inline_key(&self) -> Option<u128> {
        (self.byte_len() <= 12).then(|| StringViewArray::inline_key_fast(*self.view))
    }

    #[inline]
    fn as_str(&self) -> &str {
        let len = *self.view as u32 as usize;
        let bytes = if len <= 12 {
            // SAFETY: the private value retains a validated inline view with this length.
            unsafe { StringViewArray::inline_value(self.view, len) }
        } else {
            let view = ByteView::from(*self.view);
            // SAFETY: the private constructor retains the validated array's matching buffers.
            unsafe {
                self.buffers.get_unchecked(view.buffer_index as usize)
                    .get_unchecked(view.offset as usize..view.offset as usize + len)
            }
        };
        // SAFETY: Arrow validates UTF-8 for every view, including views at null rows.
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    #[inline]
    fn prefix(&self, len: usize) -> &[u8] {
        let string_len = *self.view as u32 as usize;
        if string_len < len { return &[]; }
        if len <= 4 || string_len <= 12 {
            // SAFETY: a validated view contains the requested prefix or the entire inline string.
            return unsafe { StringViewArray::inline_value(self.view, len) };
        }
        &self.as_str().as_bytes()[..len]
    }

    #[inline]
    fn suffix(&self, len: usize) -> &[u8] {
        let bytes = self.as_str().as_bytes();
        bytes.get(bytes.len().wrapping_sub(len)..).unwrap_or_default()
    }
}

/// Borrowed text rows with storage dispatch already resolved to Utf8View.
pub struct TextRows<'a> {
    views: &'a [u128],
    buffers: &'a [Buffer],
}

// SAFETY: the immutable decoded owner retains the exact row domain and all referenced buffers.
// Arrow validates every view, including null payloads, before safe array construction.
unsafe impl<'a> RowView<'a, ArrowText> for TextRows<'a> {
    fn len(&self) -> usize {
        self.views.len()
    }

    #[inline]
    unsafe fn get_unchecked(&self, index: usize) -> ArrowTextValue<'a> {
        // SAFETY: the caller bounds index by the retained array's row count.
        let view = unsafe { self.views.get_unchecked(index) };
        ArrowTextValue {
            view,
            buffers: self.buffers,
        }
    }
}

impl InputBinding<ArrowText> for ArrowHost {
    type Decoded = StringViewArray;
    type View<'a> = TextRows<'a>;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    const PREFER_LINEAR_OUTPUT: bool = true;
    fn validate(dtype: &Field) -> Result<(), ArrowError> {
        ordinary(dtype)?;
        if dtype.data_type() != &DataType::Utf8View { return Err(Self::error("expected Utf8View")); }
        Ok(())
    }
    fn decode(column: &ArrayRef, _: bool, _: &mut ()) -> Result<StringViewArray, ArrowError> {
        column.as_any().downcast_ref::<StringViewArray>().cloned()
            .ok_or_else(|| Self::error("Utf8View storage downcast failed"))
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> Result<bool, ArrowError> { Ok(true) }
    fn view(decoded: &StringViewArray) -> TextRows<'_> {
        TextRows {
            views: decoded.views(),
            buffers: decoded.data_buffers(),
        }
    }
}

impl TextBinding for ArrowHost {
    type Text = ArrowText;
    type Offset32 = OffsetText<i32>;
    type Offset64 = OffsetText<i64>;

    fn text_layout(dtype: &Field) -> Result<TextLayout, ArrowError> {
        <Self as InputBinding<Utf8>>::validate(dtype)?;
        match dtype.data_type() {
            DataType::Utf8 => Ok(TextLayout::Offset32),
            DataType::LargeUtf8 => Ok(TextLayout::Offset64),
            DataType::Utf8View => Ok(TextLayout::View),
            _ => Err(Self::error("expected a supported UTF-8 layout")),
        }
    }
}

/// Arrow-owned Utf8View output storage, initialized to empty strings.
pub struct StringSink {
    views: MutableBuffer,
    buffers: Vec<MutableBuffer>,
    rows: usize,
    error: Option<ArrowError>,
}
/// A retained writable view of all output rows and their byte arena.
pub struct StringOutputRows<'a> {
    views: &'a mut [u128],
    buffers: &'a mut Vec<MutableBuffer>,
    error: &'a mut Option<ArrowError>,
}
/// A consuming writer for one initialized output view.
pub struct StringWriter<'a> {
    view: &'a mut u128,
    buffers: &'a mut Vec<MutableBuffer>,
    error: &'a mut Option<ArrowError>,
}
impl WriteUtf8 for StringWriter<'_> {
    #[inline]
    fn write(self, value: &str) {
        self.write_parts(&[value]);
    }

    #[inline]
    fn write_parts(self, parts: &[&str]) {
        if self.error.is_some() { return; }
        let Some(len) = parts.iter().try_fold(0u32, |total, part| {
            total.checked_add(u32::try_from(part.len()).ok()?)
        }) else {
            *self.error = Some(ArrowHost::error("Arrow UTF-8 view length exceeds u32"));
            return;
        };
        if len <= 12 {
            let mut view = [0u8; 16];
            view[..4].copy_from_slice(&len.to_le_bytes());
            let mut offset = 4;
            for part in parts {
                view[offset..offset + part.len()].copy_from_slice(part.as_bytes());
                offset += part.len();
            }
            *self.view = u128::from_le_bytes(view);
            return;
        }

        if self.buffers.last().is_none_or(|buffer| buffer.len().saturating_add(len as usize) > u32::MAX as usize) {
            self.buffers.push(MutableBuffer::new(len as usize));
        }
        let Ok(index) = u32::try_from(self.buffers.len() - 1) else {
            *self.error = Some(ArrowHost::error("Arrow UTF-8 buffer count exceeds u32"));
            return;
        };
        let buffer = &mut self.buffers[index as usize];
        let offset = buffer.len();
        buffer.reserve(len as usize);
        for part in parts {
            buffer.extend_from_slice(part.as_bytes());
        }
        *self.view = ByteView::new(len, &buffer.as_slice()[offset..offset + 4])
            .with_buffer_index(index).with_offset(offset as u32).as_u128();
    }
}
impl Utf8Output for ArrowHost { type Sink = StringSink; }
// SAFETY: all views start as valid empty strings. Each borrowed handle identifies one distinct
// slot and writes only output-owned data. Partial abandonment drops only initialized buffers.
unsafe impl OutputSink<ArrowHost> for StringSink {
    type Params = ();
    type Rows<'a> = StringOutputRows<'a>;
    type Row<'a> = StringWriter<'a>;
    type WriteToken = ();
    fn storage_type(_: &()) -> Result<Field, ArrowError> { Ok(Field::new("result", DataType::Utf8View, false)) }
    fn allocate(rows: usize, _: &(), _: &mut ()) -> Result<Self, ArrowError> {
        let bytes = rows.checked_mul(16).ok_or_else(|| ArrowHost::error("string view byte count exceeds usize"))?;
        Ok(Self { views: MutableBuffer::from_len_zeroed(bytes), buffers: Vec::new(), rows, error: None })
    }
    fn rows(&mut self) -> Self::Rows<'_> {
        StringOutputRows { views: self.views.typed_data_mut::<u128>(), buffers: &mut self.buffers, error: &mut self.error }
    }
    fn len(rows: &Self::Rows<'_>) -> usize { rows.views.len() }
    fn initialize(_: &mut Self::Rows<'_>) {}
    unsafe fn row<'a>(rows: &'a mut Self::Rows<'_>, index: usize) -> Self::Row<'a> {
        // SAFETY: caller bounds index by the retained view length.
        StringWriter { view: unsafe { rows.views.get_unchecked_mut(index) }, buffers: rows.buffers, error: rows.error }
    }
    unsafe fn finish(self, _: &mut ()) -> Result<ArrayRef, ArrowError> {
        // Representation limits are terminal adapter failures, never deferred row evidence.
        if let Some(error) = self.error { return Err(error); }
        let buffers: Vec<Buffer> = self.buffers.into_iter().map(Buffer::from).collect();
        let views = ScalarBuffer::new(self.views.into(), 0, self.rows);
        // SAFETY: every view starts as a valid empty string. Writers copy only valid UTF-8,
        // check representation limits, and record offsets into retained output-owned buffers.
        // No safe row operation can modify another view or invalidate the referenced bytes.
        Ok(Arc::new(unsafe { StringViewArray::new_unchecked(views, buffers, None) }))
    }
}
