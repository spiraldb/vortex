use std::any::Any;
use std::fmt::Debug;

use arrow::array::ArrayRef as ArrowArrayRef;

use crate::array::bool::BoolArray;
use crate::array::chunked::ChunkedArray;
use crate::array::constant::ConstantArray;
use crate::array::patched::PatchedArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::ree::REEArray;
use crate::array::stats::Stats;
use crate::array::struct_::StructArray;
use crate::array::typed::TypedArray;
use crate::array::varbin::VarBinArray;
use crate::array::varbinview::VarBinViewArray;
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

pub mod bool;
pub mod chunked;
pub mod constant;
pub mod encode;
pub mod patched;
pub mod primitive;
pub mod ree;
pub mod stats;
pub mod struct_;
pub mod typed;
pub mod varbin;
pub mod varbinview;

pub type ArrowIterator = dyn Iterator<Item = ArrowArrayRef>;
pub type ArrayRef = Box<dyn Array>;

/// An Enc Array is the base object representing all arrays in enc.
///
/// Arrays have a dtype and an encoding. DTypes represent the logical type of the
/// values stored in an pyenc array. Encodings represent the physical layout of the
/// array.
///
/// This differs from Apache Arrow where logical and physical are combined in
/// the data type, e.g. LargeString, RunEndEncoded.
pub trait Array: Debug + Send + Sync + dyn_clone::DynClone + 'static {
    /// Converts itself to a reference of [`Any`], which enables downcasting to concrete types.
    fn as_any(&self) -> &dyn Any;
    /// Move an owned array to `ArrayRef`
    fn boxed(self) -> ArrayRef;
    /// Convert boxed array into `Box<dyn Any>`
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    /// Get the length of the array
    fn len(&self) -> usize;
    /// Check whether the array is empty
    fn is_empty(&self) -> bool;
    /// Get the dtype of the array
    fn dtype(&self) -> &DType;
    /// Get statistics for the array
    fn stats(&self) -> Stats;
    /// Get scalar value at given index
    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>>;
    /// Produce arrow batches from the encoding
    fn iter_arrow(&self) -> Box<ArrowIterator>;
    /// Limit array to start..stop range
    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef>;
    /// Encoding kind of the array
    fn encoding(&self) -> &'static dyn Encoding;
    /// Approximate size in bytes of the array. Only takes into account variable size portion of the array
    fn nbytes(&self) -> usize;
    /// Wrap array into corresponding array kind enum. Useful for dispatching functions over arrays
    fn kind(&self) -> ArrayKind;
    // /// Convert array into corresponding array kind enum. Useful for dispatching functions over arrays
    // fn into_kind(self) -> ArrayKind;

    fn check_slice_bounds(&self, start: usize, stop: usize) -> EncResult<()> {
        if start > self.len() {
            return Err(EncError::OutOfBounds(start, 0, self.len()));
        }
        if stop > self.len() {
            return Err(EncError::OutOfBounds(stop, 0, self.len()));
        }
        Ok(())
    }
}

dyn_clone::clone_trait_object!(Array);

impl<'a> AsRef<(dyn Array + 'a)> for dyn Array {
    fn as_ref(&self) -> &(dyn Array + 'a) {
        self
    }
}

#[derive(Debug, Eq, PartialEq, Hash)]
pub struct EncodingId(&'static str);

pub trait Encoding: Debug + Send + Sync + 'static {
    fn id(&self) -> &EncodingId;
}

pub type EncodingRef = &'static dyn Encoding;

#[derive(Debug, Clone)]
pub enum ArrayKind<'a> {
    Bool(&'a BoolArray),
    Chunked(&'a ChunkedArray),
    Patched(&'a PatchedArray),
    Constant(&'a ConstantArray),
    Primitive(&'a PrimitiveArray),
    REE(&'a REEArray),
    Struct(&'a StructArray),
    Typed(&'a TypedArray),
    VarBin(&'a VarBinArray),
    VarBinView(&'a VarBinViewArray),
    Other(&'a dyn Array),
}
