use std::borrow::Borrow;
use std::fmt::{Debug, Display, Formatter};
use std::iter::zip;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit as ArrowTimeUnit};
use itertools::Itertools;

use crate::error::{EncError, EncResult};

use super::PType;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Signedness {
    Unknown,
    Unsigned,
    Signed,
}

impl From<bool> for Signedness {
    fn from(value: bool) -> Self {
        if value {
            Signedness::Signed
        } else {
            Signedness::Unsigned
        }
    }
}

impl Display for Signedness {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Signedness::Unknown => write!(f, "unknown"),
            Signedness::Unsigned => write!(f, "unsigned"),
            Signedness::Signed => write!(f, "signed"),
        }
    }
}

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
    Int(IntWidth, Signedness),
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
        matches!(self, DType::Int(_, _) | DType::Float(_))
    }
}

impl Display for DType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Signedness::*;
        match self {
            DType::Null => write!(f, "null"),
            DType::Nullable(n) => write!(f, "{}?", n),
            DType::Bool => write!(f, "bool"),
            DType::Int(w, s) => match s {
                Unknown => write!(f, "int({})", w),
                Unsigned => write!(f, "uint({})", w),
                Signed => write!(f, "sint({})", w),
            },
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
            DType::List(c) => write!(f, "list({})", c),
            DType::Map(k, v) => write!(f, "map({}, {})", k, v),
        }
    }
}

impl From<PType> for &DType {
    fn from(item: PType) -> Self {
        use Signedness::*;
        match item {
            PType::I8 => &DType::Int(IntWidth::_8, Signed),
            PType::I16 => &DType::Int(IntWidth::_16, Signed),
            PType::I32 => &DType::Int(IntWidth::_32, Signed),
            PType::I64 => &DType::Int(IntWidth::_64, Signed),
            PType::U8 => &DType::Int(IntWidth::_8, Unsigned),
            PType::U16 => &DType::Int(IntWidth::_16, Unsigned),
            PType::U32 => &DType::Int(IntWidth::_32, Unsigned),
            PType::U64 => &DType::Int(IntWidth::_64, Unsigned),
            PType::F16 => &DType::Float(FloatWidth::_16),
            PType::F32 => &DType::Float(FloatWidth::_32),
            PType::F64 => &DType::Float(FloatWidth::_64),
        }
    }
}

impl From<PType> for DType {
    fn from(item: PType) -> Self {
        use Signedness::*;
        match item {
            PType::I8 => DType::Int(IntWidth::_8, Signed),
            PType::I16 => DType::Int(IntWidth::_16, Signed),
            PType::I32 => DType::Int(IntWidth::_32, Signed),
            PType::I64 => DType::Int(IntWidth::_64, Signed),
            PType::U8 => DType::Int(IntWidth::_8, Unsigned),
            PType::U16 => DType::Int(IntWidth::_16, Unsigned),
            PType::U32 => DType::Int(IntWidth::_32, Unsigned),
            PType::U64 => DType::Int(IntWidth::_64, Unsigned),
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
        use Signedness::*;
        match value {
            DataType::Null => Ok(DType::Null),
            DataType::Boolean => Ok(DType::Bool),
            DataType::Int8 => Ok(DType::Int(IntWidth::_8, Signed)),
            DataType::Int16 => Ok(DType::Int(IntWidth::_16, Signed)),
            DataType::Int32 => Ok(DType::Int(IntWidth::_32, Signed)),
            DataType::Int64 => Ok(DType::Int(IntWidth::_64, Signed)),
            DataType::UInt8 => Ok(DType::Int(IntWidth::_8, Unsigned)),
            DataType::UInt16 => Ok(DType::Int(IntWidth::_16, Unsigned)),
            DataType::UInt32 => Ok(DType::Int(IntWidth::_32, Unsigned)),
            DataType::UInt64 => Ok(DType::Int(IntWidth::_64, Unsigned)),
            DataType::Float16 => Ok(DType::Float(FloatWidth::_16)),
            DataType::Float32 => Ok(DType::Float(FloatWidth::_32)),
            DataType::Float64 => Ok(DType::Float(FloatWidth::_64)),
            DataType::Utf8 | DataType::LargeUtf8 => Ok(DType::Utf8),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Ok(DType::Binary)
            }
            // TODO(robert): what to do about this timezone?
            DataType::Timestamp(u, _) => Ok(DType::ZonedDateTime(u.into())),
            DataType::Date32 | DataType::Date64 => Ok(DType::LocalDate),
            DataType::Time32(u) | DataType::Time64(u) => Ok(DType::LocalTime(u.into())),
            DataType::List(e) | DataType::FixedSizeList(e, _) | DataType::LargeList(e) => {
                Ok(DType::List(Box::new(e.data_type().try_into()?)))
            }
            DataType::Struct(f) => Ok(DType::Struct(
                f.iter().map(|f| Arc::new(f.name().clone())).collect(),
                f.iter()
                    .map(|f| f.data_type().try_into().unwrap())
                    .collect(),
            )),
            DataType::Dictionary(_, v) => v.as_ref().try_into(),
            DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => Ok(DType::Decimal(*p, *s)),
            DataType::Map(e, _) => match e.data_type() {
                DataType::Struct(f) => Ok(DType::Map(
                    Box::new(f[0].data_type().try_into().unwrap()),
                    Box::new(f[1].data_type().try_into().unwrap()),
                )),
                _ => Err(EncError::InvalidArrowDataType(e.data_type().clone())),
            },
            DataType::RunEndEncoded(_, v) => v.data_type().try_into(),
            DataType::Duration(_) | DataType::Interval(_) | DataType::Union(_, _) => {
                Err(EncError::InvalidArrowDataType(value.clone()))
            }
        }
    }
}

impl From<&ArrowTimeUnit> for TimeUnit {
    fn from(value: &ArrowTimeUnit) -> Self {
        match value {
            ArrowTimeUnit::Second => TimeUnit::S,
            ArrowTimeUnit::Millisecond => TimeUnit::Ms,
            ArrowTimeUnit::Microsecond => TimeUnit::Us,
            ArrowTimeUnit::Nanosecond => TimeUnit::Ns,
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
    use Signedness::*;
    match dtype {
        DType::Null => DataType::Null,
        DType::Nullable(_) => panic!("Nullable DType should have been handled earlier"),
        DType::Bool => DataType::Boolean,
        DType::Int(w, s) => match w {
            IntWidth::Unknown => match s {
                Unknown => DataType::Int64,
                Unsigned => DataType::UInt64,
                Signed => DataType::Int64,
            },
            IntWidth::_8 => match s {
                Unknown => DataType::Int8,
                Unsigned => DataType::UInt8,
                Signed => DataType::Int8,
            },
            IntWidth::_16 => match s {
                Unknown => DataType::Int16,
                Unsigned => DataType::UInt16,
                Signed => DataType::Int16,
            },
            IntWidth::_32 => match s {
                Unknown => DataType::Int32,
                Unsigned => DataType::UInt32,
                Signed => DataType::Int32,
            },
            IntWidth::_64 => match s {
                Unknown => DataType::Int64,
                Unsigned => DataType::UInt64,
                Signed => DataType::Int64,
            },
        },
        DType::Decimal(p, w) => DataType::Decimal128(*p, *w),
        DType::Float(w) => match w {
            FloatWidth::Unknown => DataType::Float64,
            FloatWidth::_16 => DataType::Float16,
            FloatWidth::_32 => DataType::Float32,
            FloatWidth::_64 => DataType::Float64,
        },
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
        let dtype = DType::Int(IntWidth::_32, Signedness::Signed);
        let data_type: DataType = dtype.into();
        assert_eq!(data_type, DataType::Int32);
    }
}
