// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The same dispatch rules and callback definitions execute through both host bindings.

use std::mem::MaybeUninit;

use rowfn::BooleanOutput;
use rowfn::Host;
use rowfn::HostResult;
use rowfn::InputBinding;
use rowfn::OutputBinding;
use rowfn::RowFn;
use rowfn::RowKind;
use rowfn::RowVisitor;
use rowfn::TextBinding;
use rowfn::TextValue;
use rowfn::TypeBinding;
use rowfn::sink::{FixedSizeListSink, InitializedElement, InitializedRow, ListOutput, OutputSink, UninitElementSink, Utf8Output, WriteUtf8};

use crate::{FloatWidth, Integer, IntegerHost, ListHost, NoOptions, TimestampHost, TimestampTicks};
use crate::text_dispatch::dispatch_text;

macro_rules! integer_dispatch {
    ($kind:expr, $t:ident, $body:expr) => {
        match $kind {
            Integer::I8 => { type $t = i8; $body },
            Integer::I16 => { type $t = i16; $body },
            Integer::I32 => { type $t = i32; $body },
            Integer::I64 => { type $t = i64; $body },
            Integer::U8 => { type $t = u8; $body },
            Integer::U16 => { type $t = u16; $body },
            Integer::U32 => { type $t = u32; $body },
            Integer::U64 => { type $t = u64; $body },
        }
    };
}

/// Wrapping or checked integer addition, dispatched over all eight integer widths.
#[derive(Clone, Copy)]
pub struct Add<const CHECKED: bool>;
impl<H: IntegerHost, const CHECKED: bool> RowFn<H> for Add<CHECKED> {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = !CHECKED;
    fn dispatch<V: RowVisitor<H>>(&self, _: &NoOptions, args: &[H::NativeType], visitor: V) -> HostResult<H, V::VisitResult> {
        integer_dispatch!(H::integer(&args[0])?, T, {
            if CHECKED {
                visitor.visit_deferred::<(T, T), T, bool>(|(a, b)| a.overflowing_add(b),
                    |failed| if failed { Err(row_error::<H>("integer overflow in checked add")) } else { Ok(()) })
            } else {
                visitor.visit::<(T, T), T>(|(a, b)| a.wrapping_add(b))
            }
        })
    }
}

/// Checked integer multiplication with deferred overflow evidence.
#[derive(Clone, Copy)]
pub struct Multiply;

impl<H: IntegerHost> RowFn<H> for Multiply {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = false;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        integer_dispatch!(H::integer(&args[0])?, T, {
            visitor.visit_deferred::<(T, T), T, bool>(
                |(a, b)| a.overflowing_mul(b),
                |failed| {
                    if failed {
                        Err(row_error::<H>("integer overflow in checked multiply"))
                    } else {
                        Ok(())
                    }
                },
            )
        })
    }
}

/// Checked integer division with immediate errors and exact scalar-slot initialization.
#[derive(Clone, Copy)]
pub struct Divide;
impl<H: IntegerHost> RowFn<H> for Divide {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = false;
    fn dispatch<V: RowVisitor<H>>(&self, _: &NoOptions, args: &[H::NativeType], visitor: V) -> HostResult<H, V::VisitResult> {
        integer_dispatch!(H::integer(&args[0])?, T, {
            visitor.visit_into::<(T, T), UninitElementSink<H, T>, _>((), |(a, b), row| {
                let value = a.checked_div(b).ok_or_else(|| row_error::<H>("integer division by zero or overflow"))?;
                // SAFETY: this is the exact scalar slot supplied to this callback. It remains
                // initialized until the callback immediately returns its evidence.
                Ok::<_, H::Error>(unsafe { InitializedElement::write(row, value) })
            })
        })
    }
}

/// A positive-sum predicate with checked addition and direct deferred Boolean packing.
#[derive(Clone, Copy)]
pub struct PositiveSum<const MULTIVERSIONED: bool>;
impl<H: IntegerHost, const MULTIVERSIONED: bool> RowFn<H> for PositiveSum<MULTIVERSIONED> {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = false;
    fn dispatch<V: RowVisitor<H>>(&self, _: &NoOptions, args: &[H::NativeType], visitor: V) -> HostResult<H, V::VisitResult> {
        integer_dispatch!(H::integer(&args[0])?, T, {
            visitor.visit_deferred_bool::<(T, T), bool, MULTIVERSIONED>(|(a, b)| {
                let (sum, failed) = a.overflowing_add(b);
                (sum > 0, failed)
            }, |failed| if failed { Err(row_error::<H>("integer overflow in positive sum")) } else { Ok(()) })
        })
    }
}

/// Trim Unicode whitespace into independently owned UTF-8 output.
#[derive(Clone, Copy)]
pub struct Trim;

impl<H> RowFn<H> for Trim
where
    H: TypeBinding + TextBinding + Utf8Output,
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
        dispatch_text!(H, &args[0], dispatch_trim, [V], visitor)
    }
}

fn dispatch_trim<H, K, V>(visitor: V) -> HostResult<H, V::VisitResult>
where
    H: TypeBinding + InputBinding<K> + Utf8Output,
    K: RowKind,
    for<'a> K::Value<'a>: TextValue,
    for<'a> <H::Sink as OutputSink<H>>::Row<'a>: WriteUtf8,
    V: RowVisitor<H>,
{
    visitor.visit_into::<(K,), H::Sink, ()>((), |(value,), row| row.write(value.as_str().trim()))
}

/// Scale non-null float list children, preparing the current invocation's constant factor.
#[derive(Clone, Copy)]
pub struct Scale;
impl<H: ListHost> RowFn<H> for Scale {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["list", "factor"];
    const INFALLIBLE: bool = true;
    fn dispatch<V: RowVisitor<H>>(&self, _: &NoOptions, args: &[H::NativeType], visitor: V) -> HostResult<H, V::VisitResult> {
        let (child, width) = H::list_shape(&args[0])?;
        macro_rules! scale {
            ($t:ty) => {{
                <H as ListOutput<$t>>::list_type(width)?;
                // Common vector widths use a fixed array in the callback, so the inner loop does
                // not repeat runtime bounds and trip-count handling for each parent row.
                macro_rules! fixed {
                    ($n:literal) => {
                        visitor.visit_prepared_into::<(rowfn::FixedSizeList<$t>, $t),
                            FixedSizeListSink<H, $t>, Option<$t>, InitializedRow>(
                            $n, |(_, factor)| factor, |constant, (values, factor), row| {
                                let values: &[$t; $n] = values.try_into()
                                    .expect("dispatch validated the fixed input width");
                                let row: &mut [MaybeUninit<$t>; $n] = row.try_into()
                                    .expect("the sink allocated the dispatched output width");
                                let factor = constant.unwrap_or(factor);
                                // SAFETY: conversion preserves the entire exact callback row,
                                // and fill initializes every slot before returning its token.
                                unsafe { InitializedRow::fill(row, |index| values[index] * factor) }
                            })
                    };
                }
                match width {
                    0 => return fixed!(0),
                    1 => return fixed!(1),
                    2 => return fixed!(2),
                    3 => return fixed!(3),
                    4 => return fixed!(4),
                    _ => {}
                }
                visitor.visit_prepared_into::<(rowfn::FixedSizeList<$t>, $t), FixedSizeListSink<H, $t>, Option<$t>, InitializedRow>(
                    width, |(_, factor)| factor, |constant, (values, factor), row| {
                        let factor = constant.unwrap_or(factor);
                        // SAFETY: row is the entire supplied fixed-size row. The input binding
                        // validated the same width, and every child remains initialized on return.
                        unsafe { InitializedRow::fill(row, |index| values[index] * factor) }
                    })
            }};
        }
        match child { FloatWidth::F32 => scale!(f32), FloatWidth::F64 => scale!(f64) }
    }
}

/// Adjust timestamp ticks with checked integer addition, without rescaling or timezone conversion.
#[derive(Clone, Copy)]
pub struct AdjustTicks;
impl<H: TimestampHost> RowFn<H> for AdjustTicks {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["timestamp", "ticks"];
    const INFALLIBLE: bool = false;
    fn dispatch<V: RowVisitor<H>>(&self, _: &NoOptions, args: &[H::NativeType], visitor: V) -> HostResult<H, V::VisitResult> {
        let output = H::timestamp_label(&args[0])?;
        visitor.with_output_type(output).visit_deferred::<(TimestampTicks, i64), i64, bool>(
            |(value, ticks)| value.overflowing_add(ticks),
            |failed| if failed { Err(row_error::<H>("timestamp tick adjustment overflow")) } else { Ok(()) })
    }
}

/// Nullary external function used to demonstrate registration without a built-in case.
#[derive(Clone, Copy)]
pub struct Seven;
impl<H: Host + OutputBinding<i64>> RowFn<H> for Seven {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &[];
    const INFALLIBLE: bool = true;
    fn dispatch<V: RowVisitor<H>>(&self, _: &NoOptions, _: &[H::NativeType], visitor: V) -> HostResult<H, V::VisitResult> {
        visitor.visit::<(), i64>(|()| 7)
    }
}

/// Boolean input proof, including non-byte-aligned slices.
#[derive(Clone, Copy)]
pub struct Not;
impl<H: Host + InputBinding<bool> + BooleanOutput> RowFn<H> for Not {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["value"];
    const INFALLIBLE: bool = true;
    fn dispatch<V: RowVisitor<H>>(&self, _: &NoOptions, _: &[H::NativeType], visitor: V) -> HostResult<H, V::VisitResult> {
        visitor.visit_bool::<(bool,), false>(|(value,)| !value)
    }
}

#[cold]
#[inline(never)]
fn row_error<H: Host>(message: &str) -> H::Error { H::error(message) }
