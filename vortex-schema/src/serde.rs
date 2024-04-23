#![cfg(feature = "serde")]

use flatbuffers::root;
use serde::de::{DeserializeSeed, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vortex_flatbuffers::{FlatBufferToBytes, ReadFlatBuffer};

use crate::DType;
use crate::{flatbuffers as fb, DTypeSerdeContext};

impl Serialize for DType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.with_flatbuffer_bytes(|bytes| serializer.serialize_bytes(bytes))
    }
}

struct DTypeDeserializer(DTypeSerdeContext);

impl<'de> Visitor<'de> for DTypeDeserializer {
    type Value = DType;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a vortex dtype")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let fb = root::<fb::DType>(v).map_err(E::custom)?;
        DType::read_flatbuffer(&self.0, &fb).map_err(E::custom)
    }
}

impl<'de> DeserializeSeed<'de> for DTypeSerdeContext {
    type Value = DType;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bytes(DTypeDeserializer(self))
    }
}

// TODO(ngates): Remove this trait in favour of storing e.g. IdxType which doesn't require
//  the context for composite types.
impl<'de> Deserialize<'de> for DType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ctx = DTypeSerdeContext::new(vec![]);
        deserializer.deserialize_bytes(DTypeDeserializer(ctx))
    }
}
