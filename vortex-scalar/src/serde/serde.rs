#![cfg(feature = "serde")]

use std::fmt::Formatter;

use serde::de::{Error, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::value::ScalarValue;

impl Serialize for ScalarValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            ScalarValue::Null => ().serialize(serializer),
            ScalarValue::Bool(b) => b.serialize(serializer),
            ScalarValue::Buffer(buffer) => buffer.as_ref().serialize(serializer),
            ScalarValue::List(l) => l.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for ScalarValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct ScalarValueVisitor;
        impl<'v> Visitor<'v> for ScalarValueVisitor {
            type Value = ScalarValue;

            fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                write!(formatter, "a scalar data value")
            }

            fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Bool(v))
            }

            fn visit_i8<E>(self, v: i8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_i16<E>(self, v: i16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_i32<E>(self, v: i32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_u8<E>(self, v: u8) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_u16<E>(self, v: u16) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_u32<E>(self, v: u32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_f32<E>(self, v: f32) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_le_bytes().to_vec().into()))
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.as_bytes().to_vec().into()))
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Buffer(v.to_vec().into()))
            }

            fn visit_unit<E>(self) -> Result<Self::Value, E>
            where
                E: Error,
            {
                Ok(ScalarValue::Null)
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'v>,
            {
                let mut elems = vec![];
                while let Some(e) = seq.next_element::<ScalarValue>()? {
                    elems.push(e);
                }
                Ok(ScalarValue::List(elems.into()))
            }
        }

        deserializer.deserialize_any(ScalarValueVisitor)
    }
}
