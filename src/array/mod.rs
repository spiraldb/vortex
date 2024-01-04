mod binary;
mod bool;
mod primitive;

use crate::types::DType;
use std::any::Any;
use std::str::FromStr;
use strum_macros::EnumString;

#[derive(Debug, PartialEq, EnumString)]
pub enum ArrayKind {
    #[strum(serialize = "enc.bool")]
    Bool,
    #[strum(serialize = "enc.chunked")]
    Chunked,
    #[strum(serialize = "enc.constant")]
    Constant,
    #[strum(serialize = "enc.dictionary")]
    Dictionary,
    #[strum(serialize = "enc.patched")]
    Patched,
    #[strum(serialize = "enc.primitive")]
    Primitive,
    #[strum(serialize = "enc.roaring_bool")]
    RoaringBool,
    #[strum(serialize = "enc.roaring_uint")]
    RoaringUint,
    #[strum(serialize = "enc.struct")]
    Struct,
    #[strum(serialize = "enc.varbinary")]
    VarBinary,
}

/// An Enc Array is the base object representing all arrays in enc.
///
/// Arrays have a dtype and an encoding. DTypes represent the logical type of the
/// values stored in an enc array. Encodings represent the physical layout of the
/// array.
///
/// This differs from Apache Arrow where logical and physical are combined in
/// the data type, e.g. LargeString, RunEndEncoded.
pub trait Array: dyn_clone::DynClone {
    /// Converts itself to a reference of [`Any`], which enables downcasting to concrete types.
    fn as_any(&self) -> &dyn Any;

    /// Converts itself to a mutable reference of [`Any`], which enables mutable downcasting to concrete types.
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// Get the length of the array
    fn len(&self) -> usize;

    /// Check if array is empty
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the `DType` of the array
    fn dtype(&self) -> &DType;

    /// Return stable identifier of the array kind.
    fn kind(&self) -> &str;

    /// Enum variant of kind for builtin array types.
    #[inline]
    fn enum_kind(&self) -> Option<ArrayKind> {
        ArrayKind::from_str(self.kind()).ok()
    }

    /// Clone a `&dyn Array` to an owned `Box<dyn Array>`.
    fn to_boxed(&self) -> Box<dyn Array>;
}

dyn_clone::clone_trait_object!(Array);

macro_rules! impl_array {
    () => {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
            self
        }

        fn to_boxed(&self) -> Box<dyn Array> {
            Box::new(self.clone())
        }
    };
}

pub(crate) use impl_array;
