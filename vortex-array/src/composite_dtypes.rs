use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::dtype::{DType, IntWidth, Nullability, Signedness};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum TimeUnit {
    Ns,
    Us,
    Ms,
    S,
}

impl Display for TimeUnit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeUnit::Ns => write!(f, "ns"),
            TimeUnit::Us => write!(f, "us"),
            TimeUnit::Ms => write!(f, "ms"),
            TimeUnit::S => write!(f, "s"),
        }
    }
}

pub struct TimeUnitSerializer;

impl TimeUnitSerializer {
    pub fn serialize(unit: TimeUnit) -> Vec<u8> {
        vec![unit as u8]
    }

    pub fn deserialize(bytes: &[u8]) -> TimeUnit {
        match bytes[0] {
            0x00 => TimeUnit::Ns,
            0x01 => TimeUnit::Us,
            0x02 => TimeUnit::Ms,
            0x03 => TimeUnit::S,
            _ => panic!("Unknown timeunit variant"),
        }
    }
}

const LOCALTIME_DTYPE: &str = "localtime";

pub fn localtime(unit: TimeUnit, width: IntWidth, nullability: Nullability) -> DType {
    DType::Composite(
        Arc::new(LOCALTIME_DTYPE.to_string()),
        Box::new(DType::Int(width, Signedness::Signed, nullability)),
        TimeUnitSerializer::serialize(unit),
    )
}

const LOCALDATE_DTYPE: &str = "localdate";

pub fn localdate(width: IntWidth, nullability: Nullability) -> DType {
    DType::Composite(
        Arc::new(LOCALDATE_DTYPE.to_string()),
        Box::new(DType::Int(width, Signedness::Signed, nullability)),
        vec![],
    )
}

const INSTANT_DTYPE: &str = "instant";

pub fn instant(unit: TimeUnit, nullability: Nullability) -> DType {
    DType::Composite(
        Arc::new(INSTANT_DTYPE.to_string()),
        Box::new(DType::Int(IntWidth::_64, Signedness::Signed, nullability)),
        TimeUnitSerializer::serialize(unit),
    )
}

const ZONEDDATETIME_DTYPE: &str = "zoneddatetime";

pub fn zoneddatetime(unit: TimeUnit, nullability: Nullability) -> DType {
    DType::Composite(
        Arc::new(ZONEDDATETIME_DTYPE.to_string()),
        Box::new(DType::Struct(
            vec![
                Arc::new("instant".to_string()),
                Arc::new("timezone".to_string()),
            ],
            vec![
                DType::Int(IntWidth::_64, Signedness::Signed, nullability),
                DType::Utf8(nullability),
            ],
        )),
        TimeUnitSerializer::serialize(unit),
    )
}

const MAP_DTYPE: &str = "map";

pub fn map(key_type: DType, value_type: DType) -> DType {
    DType::Composite(
        Arc::new(MAP_DTYPE.to_string()),
        Box::new(DType::Struct(
            vec![Arc::new("key".to_string()), Arc::new("value".to_string())],
            vec![key_type, value_type],
        )),
        vec![],
    )
}
