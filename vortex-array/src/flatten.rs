use vortex_error::{vortex_err, VortexResult};

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
pub enum Flattened {
    Bool(BoolArray),
    Chunked(ChunkedArray),
    Primitive(PrimitiveArray),
    Struct(StructArray),
    VarBin(VarBinArray),
    VarBinView(VarBinViewArray),
    Extension(ExtensionArray),
}

pub trait ArrayFlatten {
    fn flatten(self) -> VortexResult<Flattened>;
}

impl Array {
    pub fn flatten(self) -> VortexResult<Flattened> {
        ArrayEncoding::flatten(self.encoding(), self)
    }

    pub fn flatten_bool(self) -> VortexResult<BoolArray> {
        BoolArray::try_from(self.flatten()?.into_array())
    }

    pub fn flatten_primitive(self) -> VortexResult<PrimitiveArray> {
        PrimitiveArray::try_from(
            self.flatten()
                .map_err(|e| vortex_err!("Array doesn't flatten into primitive {}", e))?
                .into_array(),
        )
    }

    pub fn flatten_varbin(self) -> VortexResult<VarBinArray> {
        VarBinArray::try_from(self.flatten()?.into_array())
    }
}

impl IntoArray for Flattened {
    fn into_array(self) -> Array {
        match self {
            Self::Bool(a) => a.into_array(),
            Self::Primitive(a) => a.into_array(),
            Self::Struct(a) => a.into_array(),
            Self::Chunked(a) => a.into_array(),
            Self::VarBin(a) => a.into_array(),
            Self::Extension(a) => a.into_array(),
            Self::VarBinView(a) => a.into_array(),
        }
    }
}
