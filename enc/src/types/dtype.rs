use arrow2::datatypes::DataType;

use super::PType;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IntWidth {
    Unknown,
    _8,
    _16,
    _32,
    _64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FloatWidth {
    Unknown,
    _16,
    _32,
    _64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TimeUnit {
    Ns,
    Us,
    Ms,
    S,
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
    type Error = ();

    fn try_from(value: &DataType) -> Result<Self, Self::Error> {
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
            _ => Err(()),
        }
    }
}
