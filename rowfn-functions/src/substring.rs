// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Extract UTF-8 substrings at byte positions through either host binding.
//!
//! The byte positions follow Arrow's string substring kernel for parameters that fit its
//! 32-bit string offsets. The function rejects larger parameters instead of reproducing Arrow's
//! narrowing casts for `Utf8`. RowFn skips null rows, so an invalid boundary in a null payload
//! does not fail this function even though Arrow's offset-based kernel can report that error.

use rowfn::HostResult;
use rowfn::InputBinding;
use rowfn::RowFn;
use rowfn::RowKind;
use rowfn::RowVisitor;
use rowfn::TextBinding;
use rowfn::TextValue;
use rowfn::TypeBinding;
use rowfn::sink::OutputSink;
use rowfn::sink::Utf8Output;
use rowfn::sink::WriteUtf8;

use crate::NoOptions;
use crate::text_dispatch::dispatch_text;

/// Extract a UTF-8 substring using byte positions, with strict null propagation.
///
/// A negative `start` counts from the end in bytes. A start past either end is clamped to the
/// string boundary. `length` limits the number of bytes, or takes the rest when absent. Both
/// positions must be UTF-8 character boundaries for every valid row. Parameters must fit a
/// signed 32-bit offset, which avoids Arrow's narrowing behavior for ordinary `Utf8` arrays.
#[derive(Clone, Copy, Debug)]
pub struct SubstringBytes {
    /// Byte position counted from the front when nonnegative, or the back when negative.
    pub start: i64,
    /// Maximum output length in bytes; `None` takes the rest of the input.
    pub length: Option<u64>,
}

impl SubstringBytes {
    /// Construct a substring function with the given byte bounds.
    pub const fn new(start: i64, length: Option<u64>) -> Self {
        Self { start, length }
    }
}

impl<H> RowFn<H> for SubstringBytes
where
    H: TypeBinding + TextBinding + Utf8Output,
    for<'a> <H::Text as RowKind>::Value<'a>: TextValue,
    for<'a> <H::Sink as OutputSink<H>>::Row<'a>: WriteUtf8,
{
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["text"];
    const INFALLIBLE: bool = false;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        if i32::try_from(self.start).is_err()
            || self.length.is_some_and(|length| i32::try_from(length).is_err())
        {
            return Err(H::error("substring byte bounds must fit a signed 32-bit offset"));
        }

        dispatch_text!(H, &args[0], dispatch_substring, [V], *self, visitor)
    }
}

fn dispatch_substring<H, K, V>(substring: SubstringBytes, visitor: V)
    -> HostResult<H, V::VisitResult>
where
    H: TypeBinding + InputBinding<K> + Utf8Output,
    K: RowKind,
    for<'a> K::Value<'a>: TextValue,
    for<'a> <H::Sink as OutputSink<H>>::Row<'a>: WriteUtf8,
    V: RowVisitor<H>,
{
    visitor.visit_into::<(K,), H::Sink, Result<(), H::Error>>((), move |(text,), row| {
        let text = text.as_str();
        let start = if substring.start >= 0 {
            usize::try_from(substring.start).unwrap_or(usize::MAX).min(text.len())
        } else {
            text.len().saturating_sub(
                usize::try_from(substring.start.unsigned_abs()).unwrap_or(usize::MAX),
            )
        };
        let end = substring.length.map_or(text.len(), |length| {
            start.saturating_add(usize::try_from(length).unwrap_or(usize::MAX)).min(text.len())
        });

        let value = text.get(start..end).ok_or_else(|| {
            let offset = if !text.is_char_boundary(start) { start } else { end };
            H::error(&format!("The offset {offset} is at an invalid utf-8 boundary."))
        })?;
        row.write(value);
        Ok(())
    })
}

/// Extract a UTF-8 substring using Unicode scalar positions, with strict null propagation.
///
/// A negative `start` counts from the last character. An absent `length` takes the rest of the
/// string. This matches the indexing contract of Arrow's `substring_by_char` for string arrays.
#[derive(Clone, Copy, Debug)]
pub struct SubstringChars {
    /// Character position counted from the front when nonnegative, or the back when negative.
    pub start: i64,
    /// Maximum output length in characters; `None` takes the rest of the input.
    pub length: Option<u64>,
}

impl SubstringChars {
    /// Construct a substring function with the given character bounds.
    pub const fn new(start: i64, length: Option<u64>) -> Self {
        Self { start, length }
    }
}

impl<H> RowFn<H> for SubstringChars
where
    H: TypeBinding + TextBinding + Utf8Output,
    for<'a> <H::Text as RowKind>::Value<'a>: TextValue,
    for<'a> <H::Sink as OutputSink<H>>::Row<'a>: WriteUtf8,
{
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["text"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        dispatch_text!(H, &args[0], dispatch_chars, [V], *self, visitor)
    }
}

fn dispatch_chars<H, K, V>(substring: SubstringChars, visitor: V)
    -> HostResult<H, V::VisitResult>
where
    H: TypeBinding + InputBinding<K> + Utf8Output,
    K: RowKind,
    for<'a> K::Value<'a>: TextValue,
    for<'a> <H::Sink as OutputSink<H>>::Row<'a>: WriteUtf8,
    V: RowVisitor<H>,
{
    let length = substring.length.map(|length| usize::try_from(length).unwrap_or(usize::MAX));
    visitor.visit_into::<(K,), H::Sink, ()>((), move |(text,), row| {
        let text = text.as_str();
        let (start, end) = char_bounds(text, substring.start, length);
        row.write(&text[start..end]);
    })
}

fn char_bounds(text: &str, start: i64, length: Option<usize>) -> (usize, usize) {
    if text.is_ascii() {
        let start = if start >= 0 {
            usize::try_from(start).unwrap_or(usize::MAX).min(text.len())
        } else {
            text.len().saturating_sub(usize::try_from(start.unsigned_abs()).unwrap_or(usize::MAX))
        };
        let end = length.map_or(text.len(), |length| start.saturating_add(length).min(text.len()));
        return (start, end);
    }

    let start = if start >= 0 {
        text.char_indices()
            .nth(usize::try_from(start).unwrap_or(usize::MAX))
            .map_or(text.len(), |(offset, _)| offset)
    } else {
        let back = usize::try_from(start.unsigned_abs()).unwrap_or(usize::MAX);
        text.char_indices().nth_back(back - 1).map_or(0, |(offset, _)| offset)
    };
    let end = length.map_or(text.len(), |length| {
        if length >= text.len() - start {
            text.len()
        } else {
            text[start..]
                .char_indices()
                .nth(length)
                .map_or(text.len(), |(offset, _)| start + offset)
        }
    });
    (start, end)
}
