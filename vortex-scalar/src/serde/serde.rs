#![cfg(feature = "serde")]

use std::fmt::Formatter;

use serde::de::{Error, SeqAccess, Visitor};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::value::{ScalarData, ScalarValue, ScalarView};

impl Serialize for ScalarValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ScalarValue::Data(d) => d.serialize(serializer),
            ScalarValue::View(v) => v.serialize(serializer),
        }
    }
}

impl Serialize for ScalarData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ScalarData::None => ().serialize(serializer),
            ScalarData::Bool(b) => b.serialize(serializer),
            ScalarData::Buffer(buffer) => buffer.as_ref().serialize(serializer),
            ScalarData::List(l) => l.serialize(serializer),
        }
    }
}

impl Serialize for ScalarView {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.0.flexbuffer_type().is_null() {
            return serializer.serialize_unit();
        }

        if self.0.flexbuffer_type().is_bool() {
            return serializer.serialize_bool(self.0.as_bool());
        }

        if self.0.flexbuffer_type().is_blob() {
            return serializer.serialize_bytes(self.0.as_blob().0.as_ref());
        }

        if self.0.flexbuffer_type().is_vector() {
            let vec = self.0.as_vector();
            let mut seq = serializer.serialize_seq(Some(vec.len()))?;
            for v in vec.iter() {
                seq.serialize_element(&ScalarView(v))?;
            }
            return seq.end();
        }

        // Flexbuffer widens to 64 bits for integers and floats.
        if !self.0.flexbuffer_type().is_int() {
            return serializer.serialize_i64(self.0.as_i64());
        }
        if self.0.flexbuffer_type().is_uint() {
            return serializer.serialize_u64(self.0.as_u64());
        }
        if self.0.flexbuffer_type().is_float() {
            return serializer.serialize_f64(self.0.as_f64());
        }

        panic!(
            "Unsupported flexbuffer for scalar: {:?}",
            self.0.flexbuffer_type()
        );
    }
}

impl<'de> Deserialize<'de> for ScalarValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        ScalarData::deserialize(deserializer).map(ScalarValue::Data)
    }
}

impl<'de> Deserialize<'de> for ScalarData {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ScalarDataVisitor;
        impl<'v> Visitor<'v> for ScalarDataVisitor {
            type Value = ScalarData;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                write!(formatter, "a scalar data value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Bool(v))
            }

            fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.as_bytes().to_vec().into()))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::Buffer(v.to_vec().into()))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarData::None)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'v>,
            {
                let mut elems = vec![];
                while let Some(e) = seq.next_element::<ScalarData>()? {
                    elems.push(e);
                }
                Ok(ScalarData::List(elems.into()))
            }
        }

        deserializer.deserialize_any(ScalarDataVisitor)
    }
}
