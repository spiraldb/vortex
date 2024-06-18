use arrow_array::ArrayRef;
use vortex_error::VortexResult;

use crate::array::bool::BoolArray;
use crate::array::extension::ExtensionArray;
use crate::array::null::NullArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::r#struct::StructArray;
use crate::array::varbin::VarBinArray;
use crate::array::varbinview::VarBinViewArray;
use crate::compute::as_arrow::AsArrowArray;
use crate::encoding::ArrayEncoding;
use crate::{Array, IntoArray};

/// The set of encodings that can be converted to Arrow with zero-copy.
pub enum Flattened {
    Null(NullArray),
    Bool(BoolArray),
    Primitive(PrimitiveArray),
    Struct(StructArray),
    VarBin(VarBinArray),
    VarBinView(VarBinViewArray),
    Extension(ExtensionArray),
}

impl Flattened {
    /// Convert a flat array into its equivalent [ArrayRef](Arrow array).
    ///
    /// Scalar arrays such as Bool and Primitive flattened arrays though should convert with
    /// zero copies, while more complex variants such as Struct may require allocations if its child
    /// arrays require decompression.
    pub fn into_arrow(self) -> ArrayRef {
        match self {
            Flattened::Null(a) => a.as_arrow(),
            Flattened::Bool(a) => a.as_arrow(),
            Flattened::Primitive(a) => a.as_arrow(),
            Flattened::Struct(a) => a.as_arrow(),
            Flattened::VarBin(a) => a.as_arrow(),
            Flattened::VarBinView(a) => a.as_arrow(),
            Flattened::Extension(a) => a.as_arrow(),
        }
        .expect("flat array must convert to ArrayRef")
    }
}

/// Support trait for transmuting an array into its [vortex_dtype::DType]'s canonical encoding.
///
/// Flattening an Array ensures that the array's encoding matches one of the builtin canonical
/// encodings, each of which has a corresponding [Flattened] variant.
///
/// **Important**: DType remains the same before and after a flatten operation.
pub trait ArrayFlatten {
    fn flatten(self) -> VortexResult<Flattened>;
}

impl Array {
    pub fn flatten(self) -> VortexResult<Flattened> {
        ArrayEncoding::flatten(self.encoding(), self)
    }

    pub fn flatten_extension(self) -> VortexResult<ExtensionArray> {
        ExtensionArray::try_from(self.flatten()?.into_array())
    }

    pub fn flatten_bool(self) -> VortexResult<BoolArray> {
        BoolArray::try_from(self.flatten()?.into_array())
    }

    pub fn flatten_primitive(self) -> VortexResult<PrimitiveArray> {
        PrimitiveArray::try_from(self.flatten()?.into_array())
    }

    pub fn flatten_varbin(self) -> VortexResult<VarBinArray> {
        VarBinArray::try_from(self.flatten()?.into_array())
    }
}

impl IntoArray for Flattened {
    fn into_array(self) -> Array {
        match self {
            Self::Null(a) => a.into_array(),
            Self::Bool(a) => a.into_array(),
            Self::Primitive(a) => a.into_array(),
            Self::Struct(a) => a.into_array(),
            Self::VarBin(a) => a.into_array(),
            Self::Extension(a) => a.into_array(),
            Self::VarBinView(a) => a.into_array(),
        }
    }
}
