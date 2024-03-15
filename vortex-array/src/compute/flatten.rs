use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::{Array, ArrayRef};
use crate::error::{VortexError, VortexResult};
use crate::ptype::PType;

pub trait FlattenFn {
    fn flatten(&self) -> VortexResult<ArrayRef>;
}

/// Flatten an array into only flat encodings. This is the set of encodings that can be converted
/// to Arrow with zero-copy. Each DType has a canonical flattened representation.
pub fn flatten(array: &dyn Array) -> VortexResult<ArrayRef> {
    if let Some(f) = array.flatten_bool() {
        return f.flatten_bool().map(Array::boxed);
    }
    if let Some(f) = array.flatten_primitive() {
        return f.flatten_primitive().map(Array::boxed);
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
