use std::sync::Arc;

use vortex_buffer::{Buffer, BufferString};
use vortex_error::{vortex_err, VortexResult};

use crate::pvalue::PValue;

/// Represents the internal data of a scalar value. Must be interpreted by wrapping
/// up with a DType to make a Scalar.
///
/// Note that these values can be deserialized from JSON or other formats. So a PValue may not
/// have the correct width for what the DType expects. This means primitive values must be
/// cast on-read.
#[derive(Debug, Clone, PartialEq, PartialOrd)]
pub enum ScalarValue {
    Bool(bool),
    Primitive(PValue),
    Buffer(Buffer),
    BufferString(BufferString),
    List(Arc<[ScalarValue]>),
    // It's significant that Null is last in this list. As a result generated PartialOrd sorts Scalar
    // values such that Nulls are last (greatest)
    Null,
}

impl ScalarValue {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn as_bool(&self) -> VortexResult<Option<bool>> {
        match self {
            Self::Null => Ok(None),
            Self::Bool(b) => Ok(Some(*b)),
            _ => Err(vortex_err!("Expected a bool scalar, found {:?}", self)),
        }
    }

    pub fn as_pvalue(&self) -> VortexResult<Option<PValue>> {
        match self {
            Self::Null => Ok(None),
            Self::Primitive(p) => Ok(Some(*p)),
            _ => Err(vortex_err!("Expected a primitive scalar, found {:?}", self)),
        }
    }

    pub fn as_buffer(&self) -> VortexResult<Option<Buffer>> {
        match self {
            Self::Null => Ok(None),
            Self::Buffer(b) => Ok(Some(b.clone())),
            _ => Err(vortex_err!("Expected a binary scalar, found {:?}", self)),
        }
    }

    pub fn as_buffer_string(&self) -> VortexResult<Option<BufferString>> {
        match self {
            Self::Null => Ok(None),
            Self::Buffer(b) => Ok(Some(BufferString::try_from(b.clone())?)),
            Self::BufferString(b) => Ok(Some(b.clone())),
            _ => Err(vortex_err!("Expected a string scalar, found {:?}", self)),
        }
    }

    pub fn as_list(&self) -> VortexResult<Option<&Arc<[Self]>>> {
        match self {
            Self::Null => Ok(None),
            Self::List(l) => Ok(Some(l)),
            _ => Err(vortex_err!("Expected a list scalar, found {:?}", self)),
        }
    }
}
