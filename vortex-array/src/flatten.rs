use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::array::chunked::ChunkedArray;
use crate::array::extension::ExtensionArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::r#struct::StructArray;
use crate::array::varbin::VarBinArray;
use crate::array::varbinview::VarBinViewArray;
use crate::encoding::ArrayEncoding;
use crate::{Array, IntoArray};

/// The set of encodings that can be converted to Arrow with zero-copy.
pub enum Flattened<'a> {
    Bool(BoolArray<'a>),
    Chunked(ChunkedArray<'a>),
    Primitive(PrimitiveArray<'a>),
    Struct(StructArray<'a>),
    VarBin(VarBinArray<'a>),
    VarBinView(VarBinViewArray<'a>),
    Extension(ExtensionArray<'a>),
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

    pub fn flatten_bool(self) -> VortexResult<BoolArray<'a>> {
        BoolArray::try_from(self.flatten()?.into_array())
    }

    pub fn flatten_primitive(self) -> VortexResult<PrimitiveArray<'a>> {
        PrimitiveArray::try_from(self.flatten()?.into_array())
    }

    pub fn flatten_varbin(self) -> VortexResult<VarBinArray<'a>> {
        VarBinArray::try_from(self.flatten()?.into_array())
    }
}

impl<'a> IntoArray<'a> for Flattened<'a> {
    fn into_array(self) -> Array<'a> {
        match self {
            Flattened::Bool(a) => a.into_array(),
            Flattened::Primitive(a) => a.into_array(),
            Flattened::Struct(a) => a.into_array(),
            Flattened::Chunked(a) => a.into_array(),
            Flattened::VarBin(a) => a.into_array(),
            Flattened::Extension(a) => a.into_array(),
            Flattened::VarBinView(a) => a.into_array(),
        }
    }
}
