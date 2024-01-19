use std::borrow::Borrow;
use std::fmt::{Debug, Display, Formatter};
use std::iter::zip;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit as ArrowTimeUnit};
use itertools::Itertools;

use crate::error::{EncError, EncResult};

use super::PType;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum IntWidth {
    Unknown,
    _8,
    _16,
    _32,
    _64,
}

impl From<i8> for IntWidth {
    fn from(item: i8) -> Self {
        match item {
            8 => IntWidth::_8,
            16 => IntWidth::_16,
            32 => IntWidth::_32,
            64 => IntWidth::_64,
            _ => IntWidth::Unknown,
        }
    }
}

impl Display for IntWidth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IntWidth::Unknown => write!(f, "_"),
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

impl From<i8> for FloatWidth {
    fn from(item: i8) -> Self {
        match item {
            16 => FloatWidth::_16,
            32 => FloatWidth::_32,
            64 => FloatWidth::_64,
            _ => FloatWidth::Unknown,
        }
    }
}

impl Display for FloatWidth {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FloatWidth::Unknown => write!(f, "_"),
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

pub type FieldNames = Vec<Arc<String>>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DType {
    Null,
    Nullable(Box<DType>),
    Bool,
    Int(IntWidth),
    UInt(IntWidth),
    Decimal(u8, i8),
    Float(FloatWidth),
    Utf8,
    Binary,
    LocalTime(TimeUnit),
    LocalDate,
    Instant(TimeUnit),
    ZonedDateTime(TimeUnit),
    Struct(FieldNames, Vec<DType>),
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
            DType::Null => write!(f, "null"),
            DType::Nullable(n) => write!(f, "{}?", n),
            DType::Bool => write!(f, "bool"),
            DType::Int(w) => write!(f, "int({})", w),
            DType::UInt(w) => write!(f, "uint({})", w),
            DType::Decimal(p, s) => write!(f, "decimal({}, {})", p, s),
            DType::Float(w) => write!(f, "float({})", w),
            DType::Utf8 => write!(f, "utf8"),
            DType::Binary => write!(f, "binary"),
            DType::LocalTime(u) => write!(f, "localtime({})", u),
            DType::LocalDate => write!(f, "localdate"),
            DType::Instant(u) => write!(f, "instant({})", u),
            DType::ZonedDateTime(u) => write!(f, "zoned_date_time({})", u),
            DType::Struct(n, dt) => write!(
                f,
                "{{{}}}",
                n.iter()
                    .zip(dt.iter())
                    .map(|(n, dt)| format!("{}={}", n, dt))
                    .join(", ")
            ),
            DType::List(c) => write!(f, "List({})", c),
            DType::Map(k, v) => write!(f, "Map({}, {})", k, v),
        }
    }
}

impl From<PType> for &DType {
    fn from(item: PType) -> Self {
        match item {
            PType::I8 => &DType::Int(IntWidth::_8),
            PType::I16 => &DType::Int(IntWidth::_16),
            PType::I32 => &DType::Int(IntWidth::_32),
            PType::I64 => &DType::Int(IntWidth::_64),
            PType::U8 => &DType::UInt(IntWidth::_8),
            PType::U16 => &DType::UInt(IntWidth::_16),
            PType::U32 => &DType::UInt(IntWidth::_32),
            PType::U64 => &DType::UInt(IntWidth::_64),
            PType::F16 => &DType::Float(FloatWidth::_16),
            PType::F32 => &DType::Float(FloatWidth::_32),
            PType::F64 => &DType::Float(FloatWidth::_64),
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

impl TryFrom<DataType> for DType {
    type Error = EncError;

    fn try_from(value: DataType) -> Result<Self, Self::Error> {
        value.borrow().try_into()
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
            DataType::Utf8 | DataType::LargeUtf8 => Ok(DType::Utf8),
            DataType::Binary | DataType::LargeBinary => Ok(DType::Binary),
            _ => Err(EncError::InvalidArrowDataType(value.clone())),
        }
    }
}

impl From<DType> for DataType {
    fn from(value: DType) -> Self {
        value.borrow().into()
    }
}

impl From<&DType> for DataType {
    fn from(value: &DType) -> Self {
        match value {
            DType::Nullable(inner) => _dtype_to_datatype(inner.as_ref()),
            _ => _dtype_to_datatype(value),
        }
    }
}

fn _dtype_to_datatype(dtype: &DType) -> DataType {
    match dtype {
        DType::Null => DataType::Null,
        DType::Nullable(_) => panic!("Nullable DType should have been handled earlier"),
        DType::Bool => DataType::Boolean,
        DType::Int(w) => match w {
            IntWidth::Unknown => DataType::Int64,
            IntWidth::_8 => DataType::Int8,
            IntWidth::_16 => DataType::Int16,
            IntWidth::_32 => DataType::Int32,
            IntWidth::_64 => DataType::Int64,
        },
        DType::UInt(w) => match w {
            IntWidth::Unknown => DataType::UInt64,
            IntWidth::_8 => DataType::UInt8,
            IntWidth::_16 => DataType::UInt16,
            IntWidth::_32 => DataType::UInt32,
            IntWidth::_64 => DataType::UInt64,
        },
        // TODO(robert): Decimal256?
        DType::Decimal(p, w) => DataType::Decimal128(*p, *w),
        DType::Float(w) => match w {
            FloatWidth::Unknown => DataType::Float64,
            FloatWidth::_16 => DataType::Float16,
            FloatWidth::_32 => DataType::Float32,
            FloatWidth::_64 => DataType::Float64,
        },
        // TODO(robert): LargeUtf8/LargeBinary?
        DType::Utf8 => DataType::Utf8,
        DType::Binary => DataType::Binary,
        DType::LocalTime(u) => DataType::Time64(match u {
            TimeUnit::Ns => ArrowTimeUnit::Nanosecond,
            TimeUnit::Us => ArrowTimeUnit::Microsecond,
            TimeUnit::Ms => ArrowTimeUnit::Millisecond,
            TimeUnit::S => ArrowTimeUnit::Second,
        }),
        DType::LocalDate => DataType::Date64,
        DType::Instant(u) => DataType::Timestamp(
            match u {
                TimeUnit::Ns => ArrowTimeUnit::Nanosecond,
                TimeUnit::Us => ArrowTimeUnit::Microsecond,
                TimeUnit::Ms => ArrowTimeUnit::Millisecond,
                TimeUnit::S => ArrowTimeUnit::Second,
            },
            None,
        ),
        DType::ZonedDateTime(_) => {
            unimplemented!("Converting ZoneDateTime to arrow datatype is not supported")
        }
        DType::Struct(names, dtypes) => DataType::Struct(
            zip(names, dtypes)
                .map(|(n, dt)| {
                    Field::new((**n).clone(), dt.into(), matches!(dt, DType::Nullable(_)))
                })
                .collect(),
        ),
        // TODO(robert): LargeList?
        DType::List(c) => DataType::List(Arc::new(Field::new(
            "element",
            c.as_ref().into(),
            matches!(c.as_ref(), DType::Nullable(_)),
        ))),
        DType::Map(k, v) => DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", k.as_ref().into(), false),
                    Field::new(
                        "value",
                        v.as_ref().into(),
                        matches!(v.as_ref(), DType::Nullable(_)),
                    ),
                ])),
                false,
            )),
            false,
        ),
    }
}

impl From<&DType> for Fields {
    fn from(value: &DType) -> Self {
        match value {
            DType::Struct(n, f) => Fields::from(
                n.iter()
                    .zip(f.iter())
                    .map(|(name, dtype)| {
                        Field::new(
                            (**name).clone(),
                            dtype.into(),
                            matches!(dtype, DType::Nullable(_)),
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
            _ => panic!("DType was not a struct {}", value),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dtype_to_datatype() {
        let dtype = DType::Int(IntWidth::_32);
        let data_type: DataType = dtype.into();
        assert_eq!(data_type, DataType::Int32);
    }
}
