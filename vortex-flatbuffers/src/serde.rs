#![cfg(feature = "serde")]

use flatbuffers::{root, FlatBufferBuilder};
use serde::de::{DeserializeSeed, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vortex_flatbuffers::{ReadFlatBuffer, WriteFlatBuffer};

use crate::{flatbuffers as fb, DTypeSerdeContext, ReadFlatBuffer};
use crate::{DType, WriteFlatBuffer};

/// Implement the `Serialize` trait by writing to a byte array.
impl<F: WriteFlatBuffer> Serialize for F {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fbb = FlatBufferBuilder::new();
        let root = self.write_flatbuffer(&mut fbb);
        fbb.finish_minimal(root);
        serializer.serialize_bytes(fbb.finished_data())
    }
}

impl<'de, F: ReadFlatBuffer> Deserialize<'de> for F {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ctx = DTypeSerdeContext::new(vec![]);
        deserializer.deserialize_bytes(DTypeDeserializer(ctx))
    }
}
