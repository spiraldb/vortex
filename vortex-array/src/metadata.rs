use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use flexbuffers::{FlexbufferSerializer, Reader};
use serde::{Deserialize, Serialize};
use vortex_error::{vortex_err, VortexResult};

/// Dynamic trait used to represent opaque owned Array metadata
/// Note that this allows us to restrict the ('static + Send + Sync) requirement to just the
/// metadata trait, and not the entire array trait. We require 'static so that we can downcast
/// use the Any trait.
pub trait ArrayMetadata: 'static + Send + Sync + Debug + TrySerializeArrayMetadata {
    fn as_any(&self) -> &dyn Any;
    fn as_any_arc(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
}

pub trait GetArrayMetadata {
    fn metadata(&self) -> Arc<dyn ArrayMetadata>;
}

pub trait TrySerializeArrayMetadata {
    fn try_serialize_metadata(&self) -> VortexResult<Arc<[u8]>>;
}

// TODO(ngates): move 'm lifetime into the function body since the result isn't tied to it.
//  Although maybe we should make the result tied to ti?
pub trait TryDeserializeArrayMetadata<'m>: Sized {
    // FIXME(ngates): we could push buffer/child validation into here.
    fn try_deserialize_metadata(metadata: Option<&'m [u8]>) -> VortexResult<Self>;
}

/// Provide default implementation for metadata serialization based on flexbuffers serde.
impl<M: Serialize> TrySerializeArrayMetadata for M {
    fn try_serialize_metadata(&self) -> VortexResult<Arc<[u8]>> {
        let mut ser = FlexbufferSerializer::new();
        self.serialize(&mut ser)?;
        Ok(ser.take_buffer().into())
    }
}

impl<'de, M: Deserialize<'de>> TryDeserializeArrayMetadata<'de> for M {
    fn try_deserialize_metadata(metadata: Option<&'de [u8]>) -> VortexResult<Self> {
        let bytes = metadata.ok_or_else(|| vortex_err!("Array requires metadata bytes"))?;
        Ok(M::deserialize(Reader::get_root(bytes)?)?)
    }
}
