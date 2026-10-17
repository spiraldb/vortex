// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Controlled row-algorithm and input-binding variants for the Arrow scalar benchmark.

use std::marker::PhantomData;

use arrow_array::{Array, ArrayRef, StringViewArray};
use arrow_schema::{ArrowError, DataType, Field};
use memchr::memmem;
use rowfn::{BooleanOutput, Host, HostResult, InputBinding, RowFn, RowKind, RowView, RowVisitor, TypeBinding, Utf8};
use rowfn_arrow::ArrowHost;
use rowfn_examples::{NoOptions, StringPredicate};

pub struct ExactView;
impl RowKind for ExactView { type Value<'a> = &'a str; }

// SAFETY: the retained Arrow owner preserves the row domain and string storage for the borrow.
// Null rows are sanitized before accessing their payload, just as in the general Utf8 binding.
unsafe impl<'a> RowView<'a, ExactView> for &'a StringViewArray {
    fn len(&self) -> usize { Array::len(*self) }
    unsafe fn get_unchecked(&self, index: usize) -> &'a str {
        if self.is_null(index) { "" } else { self.value(index) }
    }
}

impl InputBinding<ExactView> for ArrowHost {
    type Decoded = StringViewArray;
    type View<'a> = &'a StringViewArray;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &Field) -> Result<(), ArrowError> {
        <Self as InputBinding<Utf8>>::validate(dtype)?;
        if dtype.data_type() != &DataType::Utf8View {
            return Err(Self::error("diagnostic binding requires Utf8View"));
        }
        Ok(())
    }
    fn decode(column: &ArrayRef, _: bool, _: &mut ()) -> Result<Self::Decoded, ArrowError> {
        column.as_any().downcast_ref::<StringViewArray>().cloned()
            .ok_or_else(|| Self::error("diagnostic Utf8View downcast failed"))
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> Result<bool, ArrowError> { Ok(true) }
    fn view(decoded: &Self::Decoded) -> Self::View<'_> { decoded }
}

pub struct ArrowBytes<K, const PREPARED: bool> {
    predicate: StringPredicate,
    marker: PhantomData<fn() -> K>,
}

impl<K, const PREPARED: bool> Clone for ArrowBytes<K, PREPARED> {
    fn clone(&self) -> Self { Self::new(self.predicate) }
}

impl<K, const PREPARED: bool> ArrowBytes<K, PREPARED> {
    pub fn new(predicate: StringPredicate) -> Self { Self { predicate, marker: PhantomData } }
}

impl<H, K, const PREPARED: bool> RowFn<H> for ArrowBytes<K, PREPARED>
where
    H: TypeBinding + InputBinding<K> + BooleanOutput,
    for<'a> K: RowKind<Value<'a> = &'a str>,
{
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["text", "pattern"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(&self, _: &NoOptions, _: &[H::NativeType], visitor: V)
        -> HostResult<H, V::VisitResult> {
        macro_rules! compare {
            ($apply:expr) => {
                if PREPARED {
                    visitor.visit_prepared::<(K, K), bool, _>(
                        |(_, pattern)| pattern.map(str::to_owned),
                        |constant, (text, pattern)| ($apply)(text, constant.as_deref().unwrap_or(pattern)),
                    )
                } else {
                    visitor.visit_bool::<(K, K), false>(|(text, pattern)| ($apply)(text, pattern))
                }
            };
        }
        match self.predicate {
            StringPredicate::StartsWith => compare!(|text: &str, pattern: &str| {
                text.len() >= pattern.len()
                    && text.as_bytes().iter().zip(pattern.as_bytes()).all(|(a, b)| a == b)
            }),
            StringPredicate::EndsWith => compare!(|text: &str, pattern: &str| {
                text.len() >= pattern.len()
                    && text.as_bytes().iter().rev().zip(pattern.as_bytes().iter().rev()).all(|(a, b)| a == b)
            }),
            StringPredicate::EqIgnoreAsciiCase => compare!(|text: &str, pattern: &str| text.eq_ignore_ascii_case(pattern)),
            StringPredicate::Contains => visitor.visit_prepared::<(K, K), bool, _>(
                |(_, pattern)| pattern.map(|pattern| memmem::Finder::new(pattern.as_bytes()).into_owned()),
                |finder, (text, pattern)| match finder {
                    Some(finder) => finder.find(text.as_bytes()).is_some(),
                    None => memmem::find(text.as_bytes(), pattern.as_bytes()).is_some(),
                },
            ),
        }
    }
}

/// Diagnostic row domain retaining the view header until the operation chooses which bytes it needs.
pub struct ViewHeader;
impl RowKind for ViewHeader { type Value<'a> = HeaderValue<'a>; }

pub struct HeaderValue<'a> { array: &'a StringViewArray, index: usize }
impl HeaderValue<'_> {
    fn text(&self) -> &str {
        if self.array.is_null(self.index) { "" } else { self.array.value(self.index) }
    }
    fn starts_with(&self, pattern: &str) -> bool {
        if self.array.is_null(self.index) { return pattern.is_empty(); }
        let view = self.array.views()[self.index];
        if (view as u32 as usize) < pattern.len() { return false; }
        if pattern.len() <= 4 {
            let prefix = ((view >> 32) as u32).to_le_bytes();
            return prefix[..pattern.len()].iter().zip(pattern.as_bytes()).all(|(a, b)| a == b);
        }
        self.text().as_bytes().iter().zip(pattern.as_bytes()).all(|(a, b)| a == b)
    }
}

// SAFETY: each value retains the validated Arrow owner and an index in its stable row domain.
// Payload access remains safe and sanitizes nulls inside HeaderValue.
unsafe impl<'a> RowView<'a, ViewHeader> for &'a StringViewArray {
    fn len(&self) -> usize { Array::len(*self) }
    unsafe fn get_unchecked(&self, index: usize) -> HeaderValue<'a> {
        HeaderValue { array: self, index }
    }
}
impl InputBinding<ViewHeader> for ArrowHost {
    type Decoded = StringViewArray;
    type View<'a> = &'a StringViewArray;
    const DENSE_SAFE: bool = true;
    const DECODE_INFALLIBLE: bool = true;
    fn validate(dtype: &Field) -> Result<(), ArrowError> { <Self as InputBinding<ExactView>>::validate(dtype) }
    fn decode(column: &ArrayRef, scalar: bool, ctx: &mut ()) -> Result<Self::Decoded, ArrowError> {
        <Self as InputBinding<ExactView>>::decode(column, scalar, ctx)
    }
    fn can_decode_null_tolerant(_: &ArrayRef) -> Result<bool, ArrowError> { Ok(true) }
    fn view(decoded: &Self::Decoded) -> Self::View<'_> { decoded }
}

#[derive(Clone)]
pub struct HeaderPrefix;
impl RowFn<ArrowHost> for HeaderPrefix {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["text", "pattern"];
    const INFALLIBLE: bool = true;
    fn dispatch<V: RowVisitor<ArrowHost>>(&self, _: &NoOptions, _: &[Field], visitor: V)
        -> Result<V::VisitResult, ArrowError> {
        visitor.visit_prepared::<(ViewHeader, ViewHeader), bool, _>(
            |(_, pattern)| pattern.map(|p| p.text().to_owned()),
            |constant, (text, pattern)| text.starts_with(constant.as_deref().unwrap_or_else(|| pattern.text())),
        )
    }
}
