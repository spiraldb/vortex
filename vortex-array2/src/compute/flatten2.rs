use vortex_error::{vortex_err, VortexResult};

use crate::array::bool::BoolArray2;
use crate::array::primitive::PrimitiveArray2;
use crate::array::r#struct::StructArray2;
use crate::{Array, WithArray};

pub trait Flatten2Fn {
    fn flatten2(&self) -> VortexResult<Flattened>;
}

/// The set of encodings that can be converted to Arrow with zero-copy.
pub enum Flattened<'a> {
    Bool(BoolArray2<'a>),
    Primitive(PrimitiveArray2<'a>),
    Struct(StructArray2<'a>),
}

/// Flatten an array into one of the flat encodings. We restrict the result lifetime to allow
/// the implementation to shortcut if it is already correctly flattened. The caller should
/// use ToStatic if they wish to get hold of an OwnedArray.
pub fn flatten<'a>(array: &'a Array<'a>) -> VortexResult<Flattened<'a>> {
    array.with_array::<VortexResult<Flattened<'a>>, _>(|a| {
        a.flatten2().map(|f| f.flatten2()).unwrap_or_else(|| {
            Err(vortex_err!(NotImplemented: "flatten2", array.encoding().id().name()))
        })
    })
}
