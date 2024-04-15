use std::sync::Arc;

use vortex_error::VortexResult;

use crate::array::composite::CompositeMetadata;
use crate::{TryDeserializeArrayMetadata, TrySerializeArrayMetadata};

impl TrySerializeArrayMetadata for CompositeMetadata {
    fn try_serialize_metadata(&self) -> VortexResult<Arc<[u8]>> {
        todo!()
    }
}

impl TryDeserializeArrayMetadata<'_> for CompositeMetadata {
    fn try_deserialize_metadata(_metadata: Option<&[u8]>) -> VortexResult<Self> {
        todo!()
    }
}
