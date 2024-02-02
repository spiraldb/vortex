use std::any::Any;
use std::fmt::{Debug, Display, Formatter};

use arrow::array::ArrayRef as ArrowArrayRef;

use crate::array::bool::{BoolArray, BOOL_ENCODING};
use crate::array::chunked::{ChunkedArray, CHUNKED_ENCODING};
use crate::array::constant::{ConstantArray, CONSTANT_ENCODING};
use crate::array::patched::{PatchedArray, PATCHED_ENCODING};
use crate::array::primitive::{PrimitiveArray, PRIMITIVE_ENCODING};
use crate::array::ree::{REEArray, REE_ENCODING};
use crate::array::struct_::{StructArray, STRUCT_ENCODING};
use crate::array::typed::{TypedArray, TYPED_ENCODING};
use crate::array::varbin::{VarBinArray, VARBIN_ENCODING};
use crate::array::varbinview::{VarBinViewArray, VARBINVIEW_ENCODING};
use crate::array::zigzag::{ZigZagArray, ZIGZAG_ENCODING};
use crate::dtype::DType;
use crate::error::{EncError, EncResult};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::Scalar;
use crate::stats::Stats;

pub mod bool;
pub mod chunked;
pub mod constant;
pub mod patched;
pub mod primitive;
pub mod ree;
pub mod struct_;
pub mod typed;
pub mod varbin;
pub mod varbinview;
pub mod zigzag;

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
pub trait Array: ArrayDisplay + Debug + Send + Sync + dyn_clone::DynClone + 'static {
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
}

dyn_clone::clone_trait_object!(Array);

pub(crate) fn check_slice_bounds(array: &dyn Array, start: usize, stop: usize) -> EncResult<()> {
    if start > array.len() {
        return Err(EncError::OutOfBounds(start, 0, array.len()));
    }
    if stop > array.len() {
        return Err(EncError::OutOfBounds(stop, 0, array.len()));
    }
    Ok(())
}

pub(crate) fn check_index_bounds(array: &dyn Array, index: usize) -> EncResult<()> {
    if index >= array.len() {
        return Err(EncError::OutOfBounds(index, 0, array.len()));
    }
    Ok(())
}

impl<'a> AsRef<(dyn Array + 'a)> for dyn Array {
    fn as_ref(&self) -> &(dyn Array + 'a) {
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EncodingId(&'static str);

impl Display for EncodingId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

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
    ZigZag(&'a ZigZagArray),
    Other(&'a dyn Array),
}

impl<'a> From<&'a dyn Array> for ArrayKind<'a> {
    fn from(value: &'a dyn Array) -> Self {
        match *value.encoding().id() {
            BOOL_ENCODING => ArrayKind::Bool(value.as_any().downcast_ref::<BoolArray>().unwrap()),
            CHUNKED_ENCODING => {
                ArrayKind::Chunked(value.as_any().downcast_ref::<ChunkedArray>().unwrap())
            }
            CONSTANT_ENCODING => {
                ArrayKind::Constant(value.as_any().downcast_ref::<ConstantArray>().unwrap())
            }
            PATCHED_ENCODING => {
                ArrayKind::Patched(value.as_any().downcast_ref::<PatchedArray>().unwrap())
            }
            PRIMITIVE_ENCODING => {
                ArrayKind::Primitive(value.as_any().downcast_ref::<PrimitiveArray>().unwrap())
            }
            REE_ENCODING => ArrayKind::REE(value.as_any().downcast_ref::<REEArray>().unwrap()),
            STRUCT_ENCODING => {
                ArrayKind::Struct(value.as_any().downcast_ref::<StructArray>().unwrap())
            }
            TYPED_ENCODING => {
                ArrayKind::Typed(value.as_any().downcast_ref::<TypedArray>().unwrap())
            }
            VARBIN_ENCODING => {
                ArrayKind::VarBin(value.as_any().downcast_ref::<VarBinArray>().unwrap())
            }
            VARBINVIEW_ENCODING => {
                ArrayKind::VarBinView(value.as_any().downcast_ref::<VarBinViewArray>().unwrap())
            }
            ZIGZAG_ENCODING => {
                ArrayKind::ZigZag(value.as_any().downcast_ref::<ZigZagArray>().unwrap())
            }
            _ => ArrayKind::Other(value),
        }
    }
}

impl Display for dyn Array {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        ArrayFormatter::new(f, "".to_string(), self.nbytes()).array(self)
    }
}
