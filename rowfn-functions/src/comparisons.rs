// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Strict primitive and string comparisons shared by the Vortex and Arrow hosts.
//!
//! Each comparison uses the same runtime width dispatch and row operation on both hosts. Null
//! propagation belongs to the shared executor. Floating values use IEEE totalOrder as Arrow does,
//! so signed zero and distinct NaN bit patterns retain their ordering. String comparison uses
//! borrowed UTF-8 bytes and rejects incompatible host string types during dispatch.

use std::cmp::Ordering;

use rowfn::HostResult;
use rowfn::InputBinding;
use rowfn::RowFn;
use rowfn::RowKind;
use rowfn::RowVisitor;
use rowfn::TextValue;

use crate::ComparisonHost;
use crate::ComparisonKind;
use crate::Integer;
use crate::NoOptions;
use crate::TextComparisonHost;
use crate::text_dispatch::dispatch_text_pair;

macro_rules! float_comparison {
    ($lhs:ident, $rhs:ident, ==) => { $lhs.to_bits() == $rhs.to_bits() };
    ($lhs:ident, $rhs:ident, !=) => { $lhs.to_bits() != $rhs.to_bits() };
    ($lhs:ident, $rhs:ident, <) => { $lhs.total_cmp(&$rhs) < Ordering::Equal };
    ($lhs:ident, $rhs:ident, <=) => { $lhs.total_cmp(&$rhs) <= Ordering::Equal };
    ($lhs:ident, $rhs:ident, >) => { $lhs.total_cmp(&$rhs) > Ordering::Equal };
    ($lhs:ident, $rhs:ident, >=) => { $lhs.total_cmp(&$rhs) >= Ordering::Equal };
}

fn text_equal(lhs: &impl TextValue, rhs: &impl TextValue) -> bool {
    if let (Some(left), Some(right)) = (lhs.inline_eq_key(), rhs.inline_eq_key()) {
        return left == right;
    }

    let len = lhs.byte_len();
    if len != rhs.byte_len() || lhs.prefix(len.min(4)) != rhs.prefix(len.min(4)) {
        return false;
    }

    len <= 4 || lhs.as_str() == rhs.as_str()
}

fn text_order(lhs: &impl TextValue, rhs: &impl TextValue) -> Ordering {
    if let (Some(left), Some(right)) = (lhs.inline_key(), rhs.inline_key()) {
        return left.cmp(&right);
    }

    let lhs_len = lhs.byte_len();
    let rhs_len = rhs.byte_len();
    let prefix_len = lhs_len.min(rhs_len).min(4);
    let prefix_order = lhs.prefix(prefix_len).cmp(rhs.prefix(prefix_len));
    if prefix_order != Ordering::Equal {
        return prefix_order;
    }
    if prefix_len == lhs_len.min(rhs_len) {
        return lhs_len.cmp(&rhs_len);
    }

    lhs.as_str().cmp(rhs.as_str())
}

macro_rules! text_comparison {
    ($lhs:ident, $rhs:ident, ==) => { text_equal(&$lhs, &$rhs) };
    ($lhs:ident, $rhs:ident, !=) => { !text_equal(&$lhs, &$rhs) };
    ($lhs:ident, $rhs:ident, <) => { text_order(&$lhs, &$rhs) < Ordering::Equal };
    ($lhs:ident, $rhs:ident, <=) => { text_order(&$lhs, &$rhs) <= Ordering::Equal };
    ($lhs:ident, $rhs:ident, >) => { text_order(&$lhs, &$rhs) > Ordering::Equal };
    ($lhs:ident, $rhs:ident, >=) => { text_order(&$lhs, &$rhs) >= Ordering::Equal };
}

fn visit_text<H, Left, Right, V>(
    visitor: V,
    apply: impl Fn(Left::Value<'_>, Right::Value<'_>) -> bool,
) -> HostResult<H, V::VisitResult>
where
    H: TextComparisonHost + InputBinding<Left> + InputBinding<Right>,
    Left: RowKind,
    Right: RowKind,
    V: RowVisitor<H>,
{
    visitor.visit_bool::<(Left, Right), false>(|(left, right)| apply(left, right))
}

macro_rules! define_comparison {
    ($name:ident, $description:literal, $operator:tt) => {
        #[doc = $description]
        #[derive(Clone, Copy)]
        pub struct $name;

        impl<H: ComparisonHost> RowFn<H> for $name {
            type Options = NoOptions;

            const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
            const INFALLIBLE: bool = true;

            fn dispatch<V: RowVisitor<H>>(
                &self,
                _: &NoOptions,
                args: &[H::NativeType],
                visitor: V,
            ) -> HostResult<H, V::VisitResult> {
                match H::comparison_kind(&args[0])? {
                    ComparisonKind::Integer(kind) => match kind {
                        Integer::I8 => visitor.visit_bool::<(i8, i8), false>(|(a, b)| a $operator b),
                        Integer::I16 => visitor.visit_bool::<(i16, i16), false>(|(a, b)| a $operator b),
                        Integer::I32 => visitor.visit_bool::<(i32, i32), false>(|(a, b)| a $operator b),
                        Integer::I64 => visitor.visit_bool::<(i64, i64), false>(|(a, b)| a $operator b),
                        Integer::U8 => visitor.visit_bool::<(u8, u8), false>(|(a, b)| a $operator b),
                        Integer::U16 => visitor.visit_bool::<(u16, u16), false>(|(a, b)| a $operator b),
                        Integer::U32 => visitor.visit_bool::<(u32, u32), false>(|(a, b)| a $operator b),
                        Integer::U64 => visitor.visit_bool::<(u64, u64), false>(|(a, b)| a $operator b),
                    },
                    ComparisonKind::Float32 => visitor.visit_bool::<(f32, f32), false>(|(a, b)| {
                        float_comparison!(a, b, $operator)
                    }),
                    ComparisonKind::Float64 => visitor.visit_bool::<(f64, f64), false>(|(a, b)| {
                        float_comparison!(a, b, $operator)
                    }),
                }
            }
        }
    };
}

define_comparison!(Equal, "Strict numeric equality.", ==);
define_comparison!(NotEqual, "Strict numeric inequality.", !=);
define_comparison!(LessThan, "Strict numeric less-than comparison.", <);
define_comparison!(LessThanOrEqual, "Strict numeric less-than-or-equal comparison.", <=);
define_comparison!(GreaterThan, "Strict numeric greater-than comparison.", >);
define_comparison!(GreaterThanOrEqual, "Strict numeric greater-than-or-equal comparison.", >=);

macro_rules! define_text_comparison {
    ($name:ident, $description:literal, $operator:tt) => {
        #[doc = $description]
        #[derive(Clone, Copy)]
        pub struct $name;

        impl<H> RowFn<H> for $name
        where
            H: TextComparisonHost,
            for<'a> <H::Text as RowKind>::Value<'a>: TextValue,
        {
            type Options = NoOptions;

            const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
            const INFALLIBLE: bool = true;

            fn dispatch<V: RowVisitor<H>>(
                &self,
                _: &NoOptions,
                args: &[H::NativeType],
                visitor: V,
            ) -> HostResult<H, V::VisitResult> {
                H::validate_text_pair(&args[0], &args[1])?;

                dispatch_text_pair!(
                    H, &args[0], &args[1], visit_text, [V], visitor,
                    |left, right| text_comparison!(left, right, $operator),
                )
            }
        }
    };
}

define_text_comparison!(StringEqual, "Strict UTF-8 equality.", ==);
define_text_comparison!(StringNotEqual, "Strict UTF-8 inequality.", !=);
define_text_comparison!(StringLessThan, "Strict UTF-8 less-than comparison.", <);
define_text_comparison!(StringLessThanOrEqual, "Strict UTF-8 less-than-or-equal comparison.", <=);
define_text_comparison!(StringGreaterThan, "Strict UTF-8 greater-than comparison.", >);
define_text_comparison!(StringGreaterThanOrEqual, "Strict UTF-8 greater-than-or-equal comparison.", >=);
