use vortex::scalar::Scalar;
use vortex_error::{vortex_err, VortexResult};

use crate::primitive::PrimitiveData;
use crate::{Array, WithArray};

pub trait ArrayCompute {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        None
    }
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        None
    }
}

pub trait ScalarAtFn {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar>;
}

pub fn scalar_at(array: &Array, index: usize) -> VortexResult<Scalar> {
    array.with_array(|a| {
        a.scalar_at()
            .ok_or_else(|| vortex_err!("Not implemented: scalar_at"))?
            .scalar_at(index)
    })
}

pub trait FlattenFn {
    fn flatten(&self) -> VortexResult<FlattenedArray>;
}

pub enum FlattenedArray {
    Primitive(PrimitiveData),
    // Just to introduce a second variant for now
    Other(String),
}

pub fn flatten(array: &Array) -> VortexResult<FlattenedArray> {
    array.with_array(|a| {
        a.flatten()
            .ok_or_else(|| vortex_err!("Not implemented: flatten"))?
            .flatten()
    })
}

pub fn flatten_primitive(array: &Array) -> VortexResult<PrimitiveData> {
    if let FlattenedArray::Primitive(p) = flatten(array)? {
        Ok(p)
    } else {
        Err(vortex_err!(
            "Cannot flatten array {:?} into primitive",
            array
        ))
    }
}
