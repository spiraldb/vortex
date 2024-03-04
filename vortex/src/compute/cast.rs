use crate::array::bool::BoolArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::Array;
use crate::error::{VortexError, VortexResult};
use crate::ptype::PType;

pub trait CastPrimitiveFn {
    fn cast_primitive(&self, ptype: &PType) -> VortexResult<PrimitiveArray>;
}

pub fn cast_primitive(array: &dyn Array, ptype: &PType) -> VortexResult<PrimitiveArray> {
    PType::try_from(array.dtype()).map_err(|_| VortexError::InvalidDType(array.dtype().clone()))?;
    array
        .cast_primitive()
        .map(|t| t.cast_primitive(ptype))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "cast_primitive",
                array.encoding().id(),
            ))
        })
}

pub trait CastBoolFn {
    fn cast_bool(&self) -> VortexResult<BoolArray>;
}

pub fn cast_bool(array: &dyn Array) -> VortexResult<BoolArray> {
    array.cast_bool().map(|t| t.cast_bool()).unwrap_or_else(|| {
        Err(VortexError::NotImplemented(
            "cast_bool",
            array.encoding().id(),
        ))
    })
}
