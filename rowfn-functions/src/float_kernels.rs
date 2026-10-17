// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Strict floating-point arithmetic shared by the Arrow and Vortex bindings.
//!
//! The host selects binary32 or binary64 once. Each valid row then uses the same native
//! floating-point operator as Arrow. Infinity and NaN are values, not row errors or nulls.

use rowfn::{HostResult, OutputBinding, RowFn, RowVisitor};

use crate::{ComparisonHost, ComparisonKind, NoOptions};

macro_rules! visit_float {
    ($operation:expr, $visitor:expr, $t:ty) => {
        match $operation {
            FloatArithmetic::Add => $visitor.visit::<($t, $t), $t>(|(lhs, rhs)| lhs + rhs),
            FloatArithmetic::Subtract => $visitor.visit::<($t, $t), $t>(|(lhs, rhs)| lhs - rhs),
            FloatArithmetic::Multiply => $visitor.visit::<($t, $t), $t>(|(lhs, rhs)| lhs * rhs),
            FloatArithmetic::Divide => $visitor.visit::<($t, $t), $t>(|(lhs, rhs)| lhs / rhs),
            FloatArithmetic::Remainder => $visitor.visit::<($t, $t), $t>(|(lhs, rhs)| lhs % rhs),
        }
    };
}

/// A binary floating-point operation for two inputs of the same width.
#[derive(Clone, Copy, Debug)]
pub enum FloatArithmetic {
    /// Add both values.
    Add,
    /// Subtract the right value from the left value.
    Subtract,
    /// Multiply both values.
    Multiply,
    /// Divide the left value by the right value, including zero denominators.
    Divide,
    /// Compute floating-point remainder. A zero denominator produces NaN.
    Remainder,
}

impl<H> RowFn<H> for FloatArithmetic
where
    H: ComparisonHost + OutputBinding<f32> + OutputBinding<f64>,
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
        match H::comparison_kind(&args[0])? {
            ComparisonKind::Float32 => visit_float!(*self, visitor, f32),
            ComparisonKind::Float64 => visit_float!(*self, visitor, f64),
            ComparisonKind::Integer(_) => {
                Err(H::error("floating-point arithmetic requires Float32 or Float64"))
            }
        }
    }
}

/// Negate each binary32 or binary64 value, including signed zero and NaN.
#[derive(Clone, Copy)]
pub struct FloatNegate;

impl<H> RowFn<H> for FloatNegate
where
    H: ComparisonHost + OutputBinding<f32> + OutputBinding<f64>,
{
    type Options = NoOptions;
    const ARG_NAMES: &'static [&'static str] = &["value"];
    const INFALLIBLE: bool = true;

    fn dispatch<V: RowVisitor<H>>(
        &self,
        _: &NoOptions,
        args: &[H::NativeType],
        visitor: V,
    ) -> HostResult<H, V::VisitResult> {
        match H::comparison_kind(&args[0])? {
            ComparisonKind::Float32 => visitor.visit::<(f32,), f32>(|(value,)| -value),
            ComparisonKind::Float64 => visitor.visit::<(f64,), f64>(|(value,)| -value),
            ComparisonKind::Integer(_) => {
                Err(H::error("floating-point negation requires Float32 or Float64"))
            }
        }
    }
}
