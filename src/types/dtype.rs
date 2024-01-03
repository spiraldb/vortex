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
    Struct(Vec<String>, Vec<Box<DType>>),
    List(Box<DType>),
    Map(Box<DType>, Box<DType>),
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
