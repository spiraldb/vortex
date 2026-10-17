// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Portable UTF-8 byte and bit lengths with host-native integer widths.
//!
//! Both functions read the row's byte count through the text binding. A view binding can return
//! the count from its header without reading the string bytes. The host maps `LargeUtf8` to a
//! 64-bit result and ordinary UTF-8 storage to a 32-bit result.

use rowfn::HostResult;
use rowfn::InputBinding;
use rowfn::OutputBinding;
use rowfn::RowFn;
use rowfn::RowKind;
use rowfn::RowVisitor;
use rowfn::TextBinding;
use rowfn::TextValue;
use rowfn::TypeBinding;

use crate::NoOptions;
use crate::text_dispatch::dispatch_text;

/// Select the result width for a semantic UTF-8 input.
///
/// The host must reject unrelated or unknown extension types. A 64-bit result is used for
/// `LargeUtf8`; all other supported string layouts use 32 bits.
pub trait TextLengthHost: TypeBinding + TextBinding + OutputBinding<i32> + OutputBinding<i64> {
    /// Return whether `dtype` needs a 64-bit length result.
    fn large_utf8(dtype: &Self::NativeType) -> HostResult<Self, bool>;
}

/// Return the number of UTF-8 bytes in each valid string.
#[derive(Clone, Copy, Debug)]
pub struct ByteLength;

impl<H> RowFn<H> for ByteLength
where
    H: TextLengthHost,
    for<'a> <H::Text as RowKind>::Value<'a>: TextValue,
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
        dispatch_length::<H, V, false>(args, visitor)
    }
}

/// Return the number of bits in each valid UTF-8 byte sequence.
#[derive(Clone, Copy, Debug)]
pub struct BitLength;

impl<H> RowFn<H> for BitLength
where
    H: TextLengthHost,
    for<'a> <H::Text as RowKind>::Value<'a>: TextValue,
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
        dispatch_length::<H, V, true>(args, visitor)
    }
}

fn dispatch_length<H, V, const BITS: bool>(args: &[H::NativeType], visitor: V)
    -> HostResult<H, V::VisitResult>
where
    H: TextLengthHost,
    for<'a> <H::Text as RowKind>::Value<'a>: TextValue,
    V: RowVisitor<H>,
{
    let large = H::large_utf8(&args[0])?;
    dispatch_text!(H, &args[0], visit_length, [V, BITS], large, visitor)
}

fn visit_length<H, K, V, const BITS: bool>(large: bool, visitor: V)
    -> HostResult<H, V::VisitResult>
where
    H: TextLengthHost + InputBinding<K>,
    K: RowKind,
    for<'a> K::Value<'a>: TextValue,
    V: RowVisitor<H>,
{
    if large {
        visitor.visit::<(K,), i64>(|(value,)| {
            let length = value.byte_len() as i64;
            if BITS { length.wrapping_mul(8) } else { length }
        })
    } else {
        visitor.visit::<(K,), i32>(|(value,)| {
            let length = value.byte_len() as i32;
            if BITS { length.wrapping_mul(8) } else { length }
        })
    }
}
