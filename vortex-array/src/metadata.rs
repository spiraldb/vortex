use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use vortex_error::VortexResult;

/// Dynamic trait used to represent opaque owned Array metadata
///
/// Note that this allows us to restrict the ('static + Send + Sync) requirement to just the
/// metadata trait, and not the entire array trait. We require 'static so that we can downcast
/// use the Any trait.
/// TODO(ngates): add Display
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

pub trait TryDeserializeArrayMetadata<'m>: Sized {
    fn try_deserialize_metadata(metadata: Option<&'m [u8]>) -> VortexResult<Self>;
}

#[macro_export]
macro_rules! packed_struct_serialize_metadata {
    ($id:ident) => {
        impl $crate::TrySerializeArrayMetadata for $id {
            fn try_serialize_metadata(&self) -> VortexResult<std::sync::Arc<[u8]>> {
                let bytes = packed_struct::PackedStruct::pack(self)?;
                Ok(bytes.into())
            }
        }

        impl<'m> $crate::TryDeserializeArrayMetadata<'m> for $id {
            fn try_deserialize_metadata(metadata: Option<&'m [u8]>) -> VortexResult<Self> {
                let bytes = metadata.ok_or(vortex_error::vortex_err!(
                    "fastlanes bit packed metadata must be present"
                ))?;
                let x = packed_struct::PackedStruct::unpack(bytes.try_into()?)?;
                Ok(x)
            }
        }
    };
}

#[macro_export]
macro_rules! flexbuffer_serialize_metadata {
    ($id:ident) => {
        impl $crate::TrySerializeArrayMetadata for $id {
            fn try_serialize_metadata(&self) -> VortexResult<std::sync::Arc<[u8]>> {
                let mut ser = flexbuffers::FlexbufferSerializer::new();
                self.serialize(&mut ser)?;
                Ok(ser.take_buffer().into())
            }
        }

        impl<'m> $crate::TryDeserializeArrayMetadata<'m> for $id {
            fn try_deserialize_metadata(metadata: Option<&'m [u8]>) -> VortexResult<Self> {
                let bytes = metadata
                    .ok_or_else(|| vortex_error::vortex_err!("Array requires metadata bytes"))?;
                Ok(Self::deserialize(flexbuffers::Reader::get_root(bytes)?)?)
            }
        }
    };
}
