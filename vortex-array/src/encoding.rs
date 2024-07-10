use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};

use vortex_error::VortexResult;

use crate::canonical::{Canonical, IntoCanonical};
use crate::ArrayDef;
use crate::{Array, ArrayTrait};

// TODO(robert): Outline how you create a well known encoding id
/// EncodingId is a unique name and numerical code of the array
///
/// 0x0000 - reserved marker encoding
/// 0x0001 - 0x04FF - vortex internal encodings
/// 0x0401 - 0x7FFF - well known extension encodings
/// 0x8000 - 0xFFFF - custom extension encodings
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct EncodingId(&'static str, u16);

impl EncodingId {
    pub const fn new(id: &'static str, code: u16) -> Self {
        Self(id, code)
    }

    pub const fn code(&self) -> u16 {
        self.1
    }
}

impl Display for EncodingId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}({:#04x})", self.0, self.1)
    }
}

impl AsRef<str> for EncodingId {
    fn as_ref(&self) -> &str {
        self.0
    }
}

pub type EncodingRef = &'static dyn ArrayEncoding;

/// Object-safe encoding trait for an array.
pub trait ArrayEncoding: 'static + Sync + Send + Debug {
    fn id(&self) -> EncodingId;

    /// Flatten the given array.
    fn canonicalize(&self, array: Array) -> VortexResult<Canonical>;

    /// Unwrap the provided array into an implementation of ArrayTrait
    fn with_dyn(
        &self,
        array: &Array,
        f: &mut dyn for<'b> FnMut(&'b (dyn ArrayTrait + 'b)) -> VortexResult<()>,
    ) -> VortexResult<()>;
}

impl PartialEq for dyn ArrayEncoding + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl Eq for dyn ArrayEncoding + '_ {}
impl Hash for dyn ArrayEncoding + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }
}

/// Non-object-safe extensions to the ArrayEncoding trait.
pub trait ArrayEncodingExt {
    type D: ArrayDef;

    fn into_canonical(array: Array) -> VortexResult<Canonical> {
        let typed = <<Self::D as ArrayDef>::Array as TryFrom<Array>>::try_from(array)?;
        IntoCanonical::into_canonical(typed)
    }

    fn with_dyn<R, F>(array: &Array, mut f: F) -> R
    where
        F: for<'b> FnMut(&'b (dyn ArrayTrait + 'b)) -> R,
    {
        let typed =
            <<Self::D as ArrayDef>::Array as TryFrom<Array>>::try_from(array.clone()).unwrap();
        f(&typed)
    }
}

pub trait ArrayEncodingRef {
    fn encoding(&self) -> EncodingRef;
}
