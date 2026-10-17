// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Small semantic capabilities used by the proof functions, without a universal runtime type.

use std::fmt;

use rowfn::{BooleanOutput, FixedSizeList, HostResult, InputBinding, OutputBinding, RowKind, TextBinding, TypeBinding};
use rowfn::sink::ListOutput;

/// Empty function options that also satisfy the Vortex registry's display contract.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub struct NoOptions;
impl fmt::Display for NoOptions {
    fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result { Ok(()) }
}

/// Integer widths accepted by the arithmetic proof functions.
#[derive(Clone, Copy, Debug)]
pub enum Integer {
    /// Signed 8-bit integer.
    I8,
    /// Signed 16-bit integer.
    I16,
    /// Signed 32-bit integer.
    I32,
    /// Signed 64-bit integer.
    I64,
    /// Unsigned 8-bit integer.
    U8,
    /// Unsigned 16-bit integer.
    U16,
    /// Unsigned 32-bit integer.
    U32,
    /// Unsigned 64-bit integer.
    U64,
}

/// Semantic integer mapping plus the compiled bindings selected by shared dispatch.
pub trait IntegerHost: TypeBinding + BooleanOutput
    + InputBinding<i8> + OutputBinding<i8> + InputBinding<i16> + OutputBinding<i16>
    + InputBinding<i32> + OutputBinding<i32> + InputBinding<i64> + OutputBinding<i64>
    + InputBinding<u8> + OutputBinding<u8> + InputBinding<u16> + OutputBinding<u16>
    + InputBinding<u32> + OutputBinding<u32> + InputBinding<u64> + OutputBinding<u64> {
    /// Identify semantic integers. Extensions with integer storage must be rejected.
    fn integer(dtype: &Self::NativeType) -> HostResult<Self, Integer>;
}

/// Numeric domains accepted by strict comparisons.
pub enum ComparisonKind {
    /// One of the eight signed or unsigned integer widths.
    Integer(Integer),
    /// IEEE 754 binary32 with total-order comparison.
    Float32,
    /// IEEE 754 binary64 with total-order comparison.
    Float64,
}

/// Semantic comparison mapping plus compiled bindings for both floating widths.
pub trait ComparisonHost: IntegerHost + InputBinding<f32> + InputBinding<f64> {
    /// Identify numeric semantics without treating an extension as its storage type.
    fn comparison_kind(dtype: &Self::NativeType) -> HostResult<Self, ComparisonKind>;
}

/// Text bindings and the host's rules for comparing its native string types.
pub trait TextComparisonHost: TypeBinding + TextBinding + BooleanOutput {
    /// Check the host's physical string type rule for a pair of text inputs.
    fn validate_text_pair(lhs: &Self::NativeType, rhs: &Self::NativeType) -> HostResult<Self, ()>;
}

/// Floating child widths accepted by fixed-size-list scaling.
pub enum FloatWidth {
    /// 32-bit children.
    F32,
    /// 64-bit children.
    F64,
}

/// Non-null primitive list semantics and native bindings for both supported float widths.
pub trait ListHost: TypeBinding + InputBinding<FixedSizeList<f32>> + InputBinding<f32> + ListOutput<f32>
    + InputBinding<FixedSizeList<f64>> + InputBinding<f64> + ListOutput<f64> {
    /// Return child width and row shape. Reject nested nullability and unknown extensions.
    fn list_shape(dtype: &Self::NativeType) -> HostResult<Self, (FloatWidth, usize)>;
}

/// An extension row domain defined downstream of both adapters and the framework.
pub struct TimestampTicks;
impl RowKind for TimestampTicks { type Value<'a> = i64; }

/// Explicit timestamp tick semantics, preserving the host's unit and timezone metadata.
pub trait TimestampHost: TypeBinding + InputBinding<TimestampTicks> + InputBinding<i64> + OutputBinding<i64> {
    /// Validate timestamp meaning and return its non-nullable, unchanged semantic output label.
    fn timestamp_label(dtype: &Self::NativeType) -> HostResult<Self, Self::NativeType>;
}
