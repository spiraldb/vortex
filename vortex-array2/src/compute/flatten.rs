use vortex_error::{vortex_err, VortexResult};

use crate::array::bool::{BoolArray, BoolData};
use crate::array::primitive::{PrimitiveArray, PrimitiveData};
use crate::array::r#struct::{StructArray, StructData};
use crate::{Array, ArrayData, IntoArray, IntoArrayData, OwnedArray, WithArray};

pub trait FlattenFn {
    fn flatten(&self) -> VortexResult<FlattenedArray>;
}

/// The set of encodings that can be converted to Arrow with zero-copy.
pub enum FlattenedArray<'a> {
    Bool(BoolArray<'a>),
    Primitive(PrimitiveArray<'a>),
    Struct(StructArray<'a>),
}

// impl IntoArray<'static> for FlattenedArray {
//     fn into_array(self) -> OwnedArray {
//         Array::Data(self.into_array_data())
//     }
// }
//
// impl IntoArrayData for FlattenedArray {
//     fn into_array_data(self) -> ArrayData {
//         match self {
//             FlattenedArray::Bool(array) => array.into_array_data(),
//             FlattenedArray::Primitive(array) => array.into_array_data(),
//             FlattenedArray::Struct(array) => array.into_array_data(),
//         }
//     }
// }

/// Flatten an array into one of the flat encodings.
/// This does not guarantee that the array is recursively flattened.
pub fn flatten<'a>(array: &'a Array<'a>) -> VortexResult<FlattenedArray<'a>> {
    array.with_array(|a| {
        a.flatten().map(|f| f.flatten()).unwrap_or_else(|| {
            Err(vortex_err!(NotImplemented: "flatten", array.encoding().id().name()))
        })
    })
}

pub fn flatten_bool<'a>(array: &Array) -> VortexResult<BoolArray<'a>> {
    if let FlattenedArray::Bool(b) = flatten(array)? {
        Ok(b)
    } else {
        Err(vortex_err!("Cannot flatten array {} into bool", array))
    }
}

pub fn flatten_primitive(array: &Array) -> VortexResult<PrimitiveArray> {
    if let FlattenedArray::Primitive(p) = flatten(array)? {
        Ok(p)
    } else {
        Err(vortex_err!("Cannot flatten array {} into primitive", array))
    }
}

pub fn flatten_struct(array: &Array) -> VortexResult<StructArray> {
    if let FlattenedArray::Struct(s) = flatten(array)? {
        Ok(s)
    } else {
        Err(vortex_err!("Cannot flatten array {} into struct", array))
    }
}
