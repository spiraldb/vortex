use std::sync::Arc;

use vortex_buffer::Buffer;
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
    Null,
    Bool(bool),
    Primitive(PValue),
    Buffer(Buffer),
    List(Arc<[ScalarValue]>),
}

impl ScalarValue {
    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }

    pub fn as_bool(&self) -> VortexResult<Option<bool>> {
        match self {
            ScalarValue::Null => Ok(None),
            ScalarValue::Bool(b) => Ok(Some(*b)),
            _ => Err(vortex_err!("Expected a bool scalar, found {:?}", self)),
        }
    }

    pub fn as_pvalue(&self) -> VortexResult<Option<PValue>> {
        match self {
            ScalarValue::Null => Ok(None),
            ScalarValue::Primitive(p) => Ok(Some(*p)),
            _ => Err(vortex_err!("Expected a primitive scalar, found {:?}", self)),
        }
    }

    pub fn as_bytes(&self) -> VortexResult<Option<Buffer>> {
        match self {
            ScalarValue::Null => Ok(None),
            ScalarValue::Buffer(b) => Ok(Some(b.clone())),
            _ => Err(vortex_err!("Expected a binary scalar, found {:?}", self)),
        }
    }

    pub fn as_list(&self) -> VortexResult<Option<&Arc<[ScalarValue]>>> {
        match self {
            ScalarValue::List(l) => Ok(Some(l)),
            _ => Err(vortex_err!("Expected a list scalar, found {:?}", self)),
        }
    }
}
