// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Strict integer kernels shared by the Arrow and Vortex bindings.
//!
//! These functions dispatch once for each invocation. They use the integer domain defined by
//! [`IntegerHost`], which excludes unrelated native types with integer storage.

use rowfn::{Host, HostResult, RowFn, RowVisitor};

use crate::{Integer, IntegerHost, NoOptions};

macro_rules! dispatch_integer {
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

/// Subtract integers, reporting overflow when `CHECKED` is true and wrapping otherwise.
#[derive(Clone, Copy)]
pub struct Subtract<const CHECKED: bool>;

impl<H: IntegerHost, const CHECKED: bool> RowFn<H> for Subtract<CHECKED> {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = !CHECKED;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        dispatch_integer!(H::integer(&args[0])?, T, {
            if CHECKED {
                visitor.visit_deferred::<(T, T), T, bool>(
                    |(lhs, rhs)| lhs.overflowing_sub(rhs),
                    |failed| finish_overflow::<H>(failed, "integer overflow in checked subtract"),
                )
            } else {
                visitor.visit::<(T, T), T>(|(lhs, rhs)| lhs.wrapping_sub(rhs))
            }
        })
    }
}

/// Multiply integers with wrapping overflow semantics.
#[derive(Clone, Copy)]
pub struct MultiplyWrapping;

impl<H: IntegerHost> RowFn<H> for MultiplyWrapping {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        dispatch_integer!(H::integer(&args[0])?, T, {
            visitor.visit::<(T, T), T>(|(lhs, rhs)| lhs.wrapping_mul(rhs))
        })
    }
}

/// Compute integer remainder, failing on zero and returning zero for signed `MIN % -1`.
#[derive(Clone, Copy)]
pub struct Remainder;

impl<H: IntegerHost> RowFn<H> for Remainder {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = false;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        dispatch_integer!(H::integer(&args[0])?, T, {
            visitor.visit_deferred::<(T, T), T, bool>(
                |(lhs, rhs)| {
                    if rhs == 0 {
                        (0, true)
                    } else {
                        (lhs.wrapping_rem(rhs), false)
                    }
                },
                |failed| {
                    if failed {
                        Err(H::error("integer remainder by zero"))
                    } else {
                        Ok(())
                    }
                },
            )
        })
    }
}

/// Negate integers, reporting overflow when `CHECKED` is true and wrapping otherwise.
///
/// Checked negation accepts signed integers only, matching Arrow's integer domain.
#[derive(Clone, Copy)]
pub struct Negate<const CHECKED: bool>;

impl<H: IntegerHost> RowFn<H> for Negate<true> {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["value"];
    const INFALLIBLE: bool = false;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        macro_rules! signed {
            ($t:ty) => {
                visitor.visit_deferred::<($t,), $t, bool>(
                    |(value,)| value.overflowing_neg(),
                    |failed| finish_overflow::<H>(failed, "integer overflow in checked negate"),
                )
            };
        }
        match H::integer(&args[0])? {
            Integer::I8 => signed!(i8),
            Integer::I16 => signed!(i16),
            Integer::I32 => signed!(i32),
            Integer::I64 => signed!(i64),
            Integer::U8 | Integer::U16 | Integer::U32 | Integer::U64 => {
                Err(H::error("checked negation requires a signed integer"))
            }
        }
    }
}

impl<H: IntegerHost> RowFn<H> for Negate<false> {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["value"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        dispatch_integer!(H::integer(&args[0])?, T, {
            visitor.visit::<(T,), T>(|(value,)| value.wrapping_neg())
        })
    }
}

macro_rules! bitwise_binary {
    ($(#[$doc:meta])* $name:ident, $operator:tt) => {
        $(#[$doc])*
        #[derive(Clone, Copy)]
        pub struct $name;

        impl<H: IntegerHost> RowFn<H> for $name {
            type Options = NoOptions;
            const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
            const INFALLIBLE: bool = true;

            fn dispatch<V: RowVisitor<H>>(
                &self,
                _: &NoOptions,
                args: &[H::NativeType],
                visitor: V,
            ) -> HostResult<H, V::VisitResult> {
                dispatch_integer!(H::integer(&args[0])?, T, {
                    visitor.visit::<(T, T), T>(|(lhs, rhs)| lhs $operator rhs)
                })
            }
        }
    };
}

bitwise_binary!(/// Apply bitwise AND to strict integer rows.
    BitwiseAnd, &);
bitwise_binary!(/// Apply bitwise OR to strict integer rows.
    BitwiseOr, |);
bitwise_binary!(/// Apply bitwise XOR to strict integer rows.
    BitwiseXor, ^);

/// Apply bitwise AND to the first integer and the complement of the second.
#[derive(Clone, Copy)]
pub struct BitwiseAndNot;

impl<H: IntegerHost> RowFn<H> for BitwiseAndNot {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        dispatch_integer!(H::integer(&args[0])?, T, {
            visitor.visit::<(T, T), T>(|(lhs, rhs)| lhs & !rhs)
        })
    }
}

macro_rules! bitwise_shift {
    ($(#[$doc:meta])* $name:ident, $method:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy)]
        pub struct $name;

        impl<H: IntegerHost> RowFn<H> for $name {
            type Options = NoOptions;
            const ARG_NAMES: &'static [&'static str] = &["lhs", "rhs"];
            const INFALLIBLE: bool = true;

            fn dispatch<V: RowVisitor<H>>(
                &self,
                _: &NoOptions,
                args: &[H::NativeType],
                visitor: V,
            ) -> HostResult<H, V::VisitResult> {
                dispatch_integer!(H::integer(&args[0])?, T, {
                    visitor.visit::<(T, T), T>(|(lhs, rhs)| lhs.$method(rhs as u32))
                })
            }
        }
    };
}

bitwise_shift!(/// Shift each integer left, wrapping the shift count by its bit width.
    ShiftLeft, wrapping_shl);
bitwise_shift!(/// Shift each integer right, wrapping the shift count by its bit width.
    ShiftRight, wrapping_shr);

/// Complement the bits of each integer.
#[derive(Clone, Copy)]
pub struct BitwiseNot;

impl<H: IntegerHost> RowFn<H> for BitwiseNot {
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["value"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        dispatch_integer!(H::integer(&args[0])?, T, {
            visitor.visit::<(T,), T>(|(value,)| !value)
        })
    }
}

#[cold]
#[inline(never)]
fn finish_overflow<H: Host>(failed: bool, message: &str) -> HostResult<H, ()> {
    if failed { Err(H::error(message)) } else { Ok(()) }
}
