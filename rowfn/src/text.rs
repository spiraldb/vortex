// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Borrowed text operations that can retain a host's string-view metadata.

use crate::InputBinding;
use crate::RowKind;
use crate::Utf8;

/// A valid UTF-8 value whose binding can use stored lengths and prefixes.
pub trait TextValue {
    /// Borrow the complete string.
    fn as_str(&self) -> &str;

    /// Return the UTF-8 byte length without reading referenced storage when possible.
    fn byte_len(&self) -> usize {
        self.as_str().len()
    }

    /// Return an equality key for a complete inline value, when the binding has one.
    /// Keys compare equal exactly when the strings do. Implementations that return keys must
    /// use the same encoding for values that can be compared with each other.
    fn inline_eq_key(&self) -> Option<u128> {
        None
    }

    /// Return a sort key for a complete inline value, when the binding has one.
    /// Key order must match UTF-8 byte order, including equality. Implementations that return
    /// keys must use the same ordering for values that can be compared with each other.
    fn inline_key(&self) -> Option<u128> {
        None
    }

    /// Borrow the first `len` bytes, or an empty slice when the string is shorter.
    /// The byte range need not end at a character boundary.
    fn prefix(&self, len: usize) -> &[u8] {
        self.as_str().as_bytes().get(..len).unwrap_or_default()
    }

    /// Borrow the last `len` bytes, or an empty slice when the string is shorter.
    /// The byte range need not start at a character boundary.
    fn suffix(&self, len: usize) -> &[u8] {
        let bytes = self.as_str().as_bytes();
        bytes.get(bytes.len().wrapping_sub(len)..).unwrap_or_default()
    }
}

impl TextValue for &str {
    fn as_str(&self) -> &str {
        self
    }
}

/// Physical text layouts that a semantic binder can select before traversal.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextLayout {
    /// Strings addressed by 32-bit offsets.
    Offset32,
    /// Strings addressed by 64-bit offsets.
    Offset64,
    /// Inline or buffer-referencing string views.
    View,
}

/// A host's text row families, retaining metadata until the function requests bytes.
///
/// Each family can use a concrete decoded layout. Hosts without offset storage can map the offset
/// families to their view family. The ordinary [`Utf8`] binding remains available for a fixed `&str`
/// signature that does not specialize physical storage.
pub trait TextBinding:
    InputBinding<Self::Text>
    + InputBinding<Self::Offset32>
    + InputBinding<Self::Offset64>
    + InputBinding<Utf8>
{
    /// The row kind for view storage.
    type Text: for<'a> RowKind<Value<'a>: TextValue>;
    /// The row kind for 32-bit offset storage.
    type Offset32: for<'a> RowKind<Value<'a>: TextValue>;
    /// The row kind for 64-bit offset storage.
    type Offset64: for<'a> RowKind<Value<'a>: TextValue>;

    /// Validate native string semantics and select its concrete layout before the row loop.
    fn text_layout(dtype: &Self::NativeType) -> crate::HostResult<Self, TextLayout>;
}
