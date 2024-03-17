use crate::array::bool::BoolArray;
use crate::array::chunked::ChunkedArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::typed::TypedArray;
use crate::array::varbin::VarBinArray;
use crate::array::{Array, ArrayRef};
use crate::error::{VortexError, VortexResult};
use crate::ptype::PType;

pub trait FlattenFn {
    fn flatten(&self) -> VortexResult<FlattenedArray>;
}

/// The set of encodings that can be converted to Arrow with zero-copy.
pub enum FlattenedArray {
    Bool(BoolArray),
    Chunked(ChunkedArray),
    Primitive(PrimitiveArray),
    Struct(StructArray),
    Typed(TypedArray),
    VarBin(VarBinArray),
}

impl FlattenedArray {
    pub fn into_array(self) -> ArrayRef {
        match self {
            FlattenedArray::Bool(array) => array.boxed(),
            FlattenedArray::Chunked(array) => array.boxed(),
            FlattenedArray::Primitive(array) => array.boxed(),
            FlattenedArray::Struct(array) => array.boxed(),
            FlattenedArray::Typed(array) => array.boxed(),
            FlattenedArray::VarBin(array) => array.boxed(),
        }
    }
}

/// Flatten an array into one of the flat encodings.
/// This does not guarantee that the array is recursively flattened.
pub fn flatten(array: &dyn Array) -> VortexResult<FlattenedArray> {
    if let Some(f) = array.flatten_bool() {
        return f.flatten_bool().map(FlattenedArray::Bool);
    }
    if let Some(f) = array.flatten_primitive() {
        return f.flatten_primitive().map(FlattenedArray::Primitive);
    }
    if let Some(f) = array.flatten_struct() {
        return f.flatten_struct().map(FlattenedArray::Struct);
    }
    array.flatten().map(|f| f.flatten()).unwrap_or_else(|| {
        Err(VortexError::NotImplemented(
            "flatten",
            array.encoding().id(),
        ))
    })
}

pub trait FlattenBoolFn {
    fn flatten_bool(&self) -> VortexResult<BoolArray>;
}

pub fn flatten_bool(array: &dyn Array) -> VortexResult<BoolArray> {
    array
        .flatten_bool()
        .map(|t| t.flatten_bool())
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "flatten_bool",
                array.encoding().id(),
            ))
        })
}

pub trait FlattenPrimitiveFn {
    fn flatten_primitive(&self) -> VortexResult<PrimitiveArray>;
}

pub fn flatten_primitive(array: &dyn Array) -> VortexResult<PrimitiveArray> {
    PType::try_from(array.dtype()).map_err(|_| VortexError::InvalidDType(array.dtype().clone()))?;
    array
        .flatten_primitive()
        .map(|t| t.flatten_primitive())
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "flatten_primitive",
                array.encoding().id(),
            ))
        })
}

pub trait FlattenStructFn {
    fn flatten_struct(&self) -> VortexResult<StructArray>;
}

pub fn flatten_struct(array: &dyn Array) -> VortexResult<StructArray> {
    array
        .flatten_struct()
        .map(|t| t.flatten_struct())
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "flatten_struct",
                array.encoding().id(),
            ))
        })
}
