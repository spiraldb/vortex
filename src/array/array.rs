//! An Enc Array is the base object representing all arrays in enc.
//!
//! Arrays have a dtype and an encoding. DTypes represent the logical type of the
//! values stored in an enc array. Encodings represent the physical layout of the
//! array.
//!
//! This differs from Apache Arrow where logical and physical are combined in
//! the data type, e.g. LargeString, RunEndEncoded.
//!
//! Arrays are reference counted and immutable whenever the refcnt > 1.
use crate::types::dtype::DType;
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
}

pub fn id_to_array_kind(id: &str) -> Option<ArrayKind> {
    ArrayKind::from_str(id).ok()
}

pub trait Array: dyn_clone::DynClone {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn datatype(&self) -> DType;
}

dyn_clone::clone_trait_object!(Array);
