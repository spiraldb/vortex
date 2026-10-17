// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Portable string functions compared with Arrow's existing scalar kernels.

use memchr::memmem;
use rowfn::BooleanOutput;
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
use crate::text_dispatch::dispatch_text_pair;

/// Literal string predicates with strict null propagation.
#[derive(Clone, Copy, Debug)]
pub enum StringPredicate {
    /// Match a literal prefix.
    StartsWith,
    /// Match a literal suffix.
    EndsWith,
    /// Find a literal substring, preparing a searcher for a scalar pattern.
    Contains,
    /// Compare strings while ignoring ASCII letter case.
    EqIgnoreAsciiCase,
}

impl<H> RowFn<H> for StringPredicate
where
    H: TypeBinding + TextBinding + BooleanOutput,
    for<'a> <H::Text as RowKind>::Value<'a>: TextValue,
{
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["text", "pattern"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        dispatch_text_pair!(H, &args[0], &args[1], dispatch_predicate, [V], *self, visitor)
    }
}

fn dispatch_predicate<H, Left, Right, V>(predicate: StringPredicate, visitor: V)
    -> HostResult<H, V::VisitResult>
where
    H: TypeBinding + InputBinding<Left> + InputBinding<Right> + BooleanOutput,
    Left: RowKind,
    Right: RowKind,
    for<'a> Left::Value<'a>: TextValue,
    for<'a> Right::Value<'a>: TextValue,
    V: RowVisitor<H>,
{
    match predicate {
        StringPredicate::StartsWith => visitor.visit_prepared::<(Left, Right), bool, _>(
            |(_, pattern)| pattern.map(|pattern| pattern.as_str().to_owned()),
            |constant, (text, pattern)| {
                let pattern = constant.as_deref().unwrap_or_else(|| pattern.as_str()).as_bytes();
                let bytes = text.prefix(pattern.len());
                bytes.len() == pattern.len() && bytes.iter().zip(pattern).all(|(a, b)| a == b)
            },
        ),
        StringPredicate::EndsWith => visitor.visit_prepared::<(Left, Right), bool, _>(
            |(_, pattern)| pattern.map(|pattern| pattern.as_str().to_owned()),
            |constant, (text, pattern)| {
                let pattern = constant.as_deref().unwrap_or_else(|| pattern.as_str()).as_bytes();
                let bytes = text.suffix(pattern.len());
                bytes.len() == pattern.len() && bytes.iter().zip(pattern).all(|(a, b)| a == b)
            },
        ),
        StringPredicate::EqIgnoreAsciiCase => visitor.visit_prepared::<(Left, Right), bool, _>(
            |(_, pattern)| pattern.map(|pattern| pattern.as_str().to_owned()),
            |constant, (text, pattern)| {
                text.as_str().eq_ignore_ascii_case(constant.as_deref().unwrap_or_else(|| pattern.as_str()))
            },
        ),
        StringPredicate::Contains => visitor.visit_prepared::<(Left, Right), bool, _>(
            |(_, pattern)| pattern.map(|pattern| memmem::Finder::new(pattern.as_str().as_bytes()).into_owned()),
            |finder, (text, pattern)| match finder {
                Some(finder) => finder.find(text.as_str().as_bytes()).is_some(),
                None => memmem::find(text.as_str().as_bytes(), pattern.as_str().as_bytes()).is_some(),
            },
        ),
    }
}

/// Concatenate two strings into independently owned output.
#[derive(Clone, Copy)]
pub struct Concat;

impl<H> RowFn<H> for Concat
where
    H: TypeBinding + TextBinding + Utf8Output,
    for<'a> <H::Sink as OutputSink<H>>::Row<'a>: WriteUtf8,
{
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["left", "right"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        dispatch_text_pair!(H, &args[0], &args[1], dispatch_concat, [V], visitor)
    }
}

fn dispatch_concat<H, Left, Right, V>(visitor: V) -> HostResult<H, V::VisitResult>
where
    H: TypeBinding + InputBinding<Left> + InputBinding<Right> + Utf8Output,
    Left: RowKind,
    Right: RowKind,
    for<'a> Left::Value<'a>: TextValue,
    for<'a> Right::Value<'a>: TextValue,
    for<'a> <H::Sink as OutputSink<H>>::Row<'a>: WriteUtf8,
    V: RowVisitor<H>,
{
    visitor.visit_into::<(Left, Right), H::Sink, ()>((), |(left, right), row| {
        row.write_parts(&[left.as_str(), right.as_str()]);
    })
}
