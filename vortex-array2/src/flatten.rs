use vortex_error::{vortex_err, VortexResult};

use crate::array::bool::BoolArray2;
use crate::array::primitive::PrimitiveArray2;
use crate::array::r#struct::StructArray2;
use crate::encoding::ArrayEncoding;
use crate::Array;

/// The set of encodings that can be converted to Arrow with zero-copy.
pub enum Flattened<'a> {
    Bool(BoolArray2<'a>),
    Primitive(PrimitiveArray2<'a>),
    Struct(StructArray2<'a>),
}

pub trait ArrayFlatten {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a;
}

impl<'a> Array<'a> {
    pub fn flatten(self) -> VortexResult<Flattened<'a>> {
        ArrayEncoding::flatten(self.encoding(), self)
    }

    pub fn flatten_primitive(self) -> VortexResult<PrimitiveArray2<'a>> {
        let id = self.encoding().id();
        self.flatten().and_then(|f| match f {
            Flattened::Primitive(p) => Ok(p),
            _ => Err(vortex_err!("{} does not flatten into primitive", id)),
        })
    }
}
