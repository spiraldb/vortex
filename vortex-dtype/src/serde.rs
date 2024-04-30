#![cfg(feature = "serde")]
/// We hand-write the serde implementation for DType so we can retain more ergonomic tuple variants.
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{DType, Nullability};

impl Serialize for DType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            DType::Null => serializer
                .serialize_map(Some(1))?
                .serialize_entry("type", "null"),
            DType::Bool(n) => serializer
                .serialize_map(Some(2))?
                .serialize_entry("type", "null")?
                .serialize_entry("n", n),
            DType::Primitive(ptype, n) => serializer
                .serialize_map(Some(3))?
                .serialize_entry("type", "primitive")?
                .serialize_entry("ptype", *ptype)?
                .serialize_entry("n", n),
            DType::Utf8(n) => serializer
                .serialize_map(Some(2))?
                .serialize_entry("type", "utf8")?
                .serialize_entry("n", n),
            DType::Binary(n) => serializer
                .serialize_map(Some(2))?
                .serialize_entry("type", "binary")?
                .serialize_entry("n", n),
            DType::Struct { names, dtypes } => serializer
                .serialize_map(Some(3))?
                .serialize_entry("type", "struct")?
                .serialize_entry("names", names)?
                .serialize_entry("dtypes", dtypes),
            DType::List(element, n) => serializer
                .serialize_map(Some(3))?
                .serialize_entry("type", "primitive")?
                .serialize_entry("element", element)?
                .serialize_entry("n", n),
            DType::Composite(id, n) => serializer
                .serialize_map(Some(3))?
                .serialize_entry("type", "composite")?
                .serialize_entry("id", id)?
                .serialize_entry("n", n),
        }
    }
}

impl<'de> Deserialize<'de> for DType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!()
    }
}

impl Serialize for Nullability {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Nullability::NonNullable => serializer.serialize_bool(false),
            Nullability::Nullable => serializer.serialize_bool(true),
        }
    }
}

impl<'de> Deserialize<'de> for Nullability {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match bool::deserialize(deserializer)? {
            true => Nullability::Nullable,
            false => Nullability::NonNullable,
        })
    }
}
