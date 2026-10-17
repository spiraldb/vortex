// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Row value families and host-owned decoded storage.

use std::marker::PhantomData;

use crate::Host;
use crate::HostResult;

/// A row value family independent of any array representation.
pub trait RowKind: 'static {
    /// Value borrowed for one callback.
    type Value<'a>;
}

macro_rules! primitive_kind {
    ($($t:ty),+ $(,)?) => {
        $(
            impl RowKind for $t {
                type Value<'a> = $t;
            }
        )+
    };
}
primitive_kind!(i8, i16, i32, i64, u8, u16, u32, u64, f32, f64, bool);

/// Borrowed, validated UTF-8 row values.
pub struct Utf8;

impl RowKind for Utf8 {
    type Value<'a> = &'a str;
}

/// A runtime-width fixed-size list of non-null primitive children.
pub struct FixedSizeList<T>(PhantomData<T>);

impl<T: 'static> RowKind for FixedSizeList<T> {
    type Value<'a> = &'a [T];
}

/// Stable typed access to one retained decoded column.
///
/// # Safety
/// Length must remain stable across moves and shared access. Every index below `len` must remain
/// addressable with the same mapping for the view's lifetime. Every in-range read must return a
/// valid value of `K::Value<'a>`, including reads at a null input slot after successful decoding.
/// Values must borrow only storage retained for `'a`. Interior mutation must preserve these facts.
pub unsafe trait RowView<'a, K: RowKind> {
    /// Addressable row count.
    fn len(&self) -> usize;
    /// Whether this view has no addressable rows.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Read a row without checking its bounds.
    ///
    /// # Safety
    /// `index` must be below this view's stable length.
    unsafe fn get_unchecked(&self, index: usize) -> K::Value<'a>;
}

// SAFETY: a borrowed slice retains a stable length and contains initialized values.
unsafe impl<'a, T: Copy + RowKind<Value<'a> = T>> RowView<'a, T> for &'a [T] {
    fn len(&self) -> usize {
        <[T]>::len(self)
    }

    unsafe fn get_unchecked(&self, index: usize) -> T {
        // SAFETY: the caller supplies an index below the slice length.
        unsafe { *<[T]>::get_unchecked(self, index) }
    }
}

/// Typed decoding implemented on a local host marker, including generic primitive bindings.
pub trait InputBinding<K: RowKind>: Host {
    /// Retained storage. Views and prepared values cannot outlive this owner.
    type Decoded: 'static;
    /// A cheap view borrowed once before traversal.
    type View<'a>: RowView<'a, K>;
    /// Whether ordinary decoding can safely expose null payloads without semantic decode errors.
    ///
    /// A successful decoded view must always satisfy [`RowView`]. When this is false, nullable
    /// execution uses null-tolerant decoding or filters to valid rows before ordinary decoding.
    const DENSE_SAFE: bool;
    /// Whether legal input can produce semantic decoding errors, excluding infrastructure errors.
    const DECODE_INFALLIBLE: bool;
    /// Whether primitive output collection should use a straight loop for this binding.
    const PREFER_LINEAR_OUTPUT: bool = false;
    /// Validate semantic type, including nested constraints and extension identity.
    fn validate(dtype: &Self::NativeType) -> HostResult<Self, ()>;
    /// Decode once. A scalar decode must contain exactly one addressable row.
    fn decode(
        column: &Self::Column,
        scalar: bool,
        ctx: &mut Self::Context,
    ) -> HostResult<Self, Self::Decoded>;
    /// Whether a safe representation exists for valid-only reads of the original nullable input.
    fn can_decode_null_tolerant(column: &Self::Column) -> HostResult<Self, bool>;
    /// Decode safe placeholders for null payloads. A decoder error is always terminal.
    fn decode_null_tolerant(
        column: &Self::Column,
        scalar: bool,
        ctx: &mut Self::Context,
    ) -> HostResult<Self, Self::Decoded> {
        Self::decode(column, scalar, ctx)
    }
    /// Borrow stable typed access while retaining the owner.
    fn view(decoded: &Self::Decoded) -> Self::View<'_>;
}
