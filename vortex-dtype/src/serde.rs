#![cfg(feature = "serde")]
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{CompositeID, Nullability};

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

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for CompositeID {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        CompositeID::try_from(<&'de str>::deserialize(deserializer)?)
            .map_err(serde::de::Error::custom)
    }
}
