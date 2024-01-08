use crate::error::{EncError, EncResult};
use arrow2::datatypes::DataType;
use std::fmt::{Debug, Display, Formatter};

use super::PType;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IntWidth {
    Unknown,
    _8,
    _16,
    _32,
    _64,
}

impl Display for IntWidth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IntWidth::Unknown => write!(f, "Unknown"),
            IntWidth::_8 => write!(f, "8"),
            IntWidth::_16 => write!(f, "16"),
            IntWidth::_32 => write!(f, "32"),
            IntWidth::_64 => write!(f, "64"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FloatWidth {
    Unknown,
    _16,
    _32,
    _64,
}

impl Display for FloatWidth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FloatWidth::Unknown => write!(f, "Unknown"),
            FloatWidth::_16 => write!(f, "16"),
            FloatWidth::_32 => write!(f, "32"),
            FloatWidth::_64 => write!(f, "64"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DType {
    Null,
    Nullable(Box<DType>),
    Bool,
    Int(IntWidth),
    UInt(IntWidth),
    Decimal(u8, u8),
    Float(FloatWidth),
    Utf8,
    Binary,
    LocalTime(TimeUnit),
    LocalDate,
    Instant(TimeUnit),
    ZonedDateTime(TimeUnit),
    Struct(Vec<String>, Vec<DType>),
    List(Box<DType>),
    Map(Box<DType>, Box<DType>),
}

impl DType {
    pub fn is_primitive(&self) -> bool {
        matches!(self, DType::Int(_) | DType::UInt(_) | DType::Float(_))
    }
}

impl Display for DType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DType::Null => write!(f, "Null"),
            DType::Nullable(n) => write!(f, "Nullable({})", n),
            DType::Bool => write!(f, "Bool"),
            DType::Int(w) => write!(f, "Int({})", w),
            DType::UInt(w) => write!(f, "UInt({})", w),
            DType::Decimal(p, s) => write!(f, "Decimal({}, {})", p, s),
            DType::Float(w) => write!(f, "Float({})", w),
            DType::Utf8 => write!(f, "Utf8"),
            DType::Binary => write!(f, "Binary"),
            DType::LocalTime(u) => write!(f, "LocalTime({})", u),
            DType::LocalDate => write!(f, "LocalDate"),
            DType::Instant(u) => write!(f, "Instant({})", u),
            DType::ZonedDateTime(u) => write!(f, "ZonedDateTime({})", u),
            DType::Struct(n, fs) => write!(
                f,
                "Struct(names=[{}], fields=[{}]",
                n.join(", "),
                fs.iter()
                    .map(|dt| format!("{}", dt))
                    .collect::<Vec<String>>()
                    .join(", ")
            ),
            DType::List(c) => write!(f, "List({})", c),
            DType::Map(k, v) => write!(f, "Map({}, {})", k, v),
        }
    }
}

impl From<PType> for DType {
    fn from(item: PType) -> Self {
        match item {
            PType::I8 => DType::Int(IntWidth::_8),
            PType::I16 => DType::Int(IntWidth::_16),
            PType::I32 => DType::Int(IntWidth::_32),
            PType::I64 => DType::Int(IntWidth::_64),
            PType::U8 => DType::UInt(IntWidth::_8),
            PType::U16 => DType::UInt(IntWidth::_16),
            PType::U32 => DType::UInt(IntWidth::_32),
            PType::U64 => DType::UInt(IntWidth::_64),
            PType::F16 => DType::Float(FloatWidth::_16),
            PType::F32 => DType::Float(FloatWidth::_32),
            PType::F64 => DType::Float(FloatWidth::_64),
        }
    }
}

impl TryFrom<&DataType> for DType {
    type Error = EncError;

    fn try_from(value: &DataType) -> EncResult<Self> {
        match value {
            DataType::Null => Ok(DType::Null),
            DataType::Boolean => Ok(DType::Bool),
            DataType::Int8 => Ok(DType::Int(IntWidth::_8)),
            DataType::Int16 => Ok(DType::Int(IntWidth::_16)),
            DataType::Int32 => Ok(DType::Int(IntWidth::_32)),
            DataType::Int64 => Ok(DType::Int(IntWidth::_64)),
            DataType::UInt8 => Ok(DType::UInt(IntWidth::_8)),
            DataType::UInt16 => Ok(DType::UInt(IntWidth::_16)),
            DataType::UInt32 => Ok(DType::UInt(IntWidth::_32)),
            DataType::UInt64 => Ok(DType::UInt(IntWidth::_64)),
            DataType::Float16 => Ok(DType::Float(FloatWidth::_16)),
            DataType::Float32 => Ok(DType::Float(FloatWidth::_32)),
            DataType::Float64 => Ok(DType::Float(FloatWidth::_64)),
            _ => Err(EncError::InvalidArrowDataType(value.clone())),
        }
    }
}
