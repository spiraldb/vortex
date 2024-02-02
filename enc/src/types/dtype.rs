use std::borrow::Borrow;
use std::fmt::{Debug, Display, Formatter};
use std::iter::zip;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Fields, TimeUnit as ArrowTimeUnit};
use itertools::Itertools;

use crate::error::{EncError, EncResult};

use super::PType;
use DType::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum Nullability {
    NonNullable,
    Nullable,
}

impl From<bool> for Nullability {
    fn from(value: bool) -> Self {
        if value {
            Nullability::Nullable
        } else {
            Nullability::NonNullable
        }
    }
}

impl Display for Nullability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Nullability::NonNullable => write!(f, ""),
            Nullability::Nullable => write!(f, "?"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum DType {
    Null,
    Bool(Nullability),
    Int(IntWidth, Signedness, Nullability),
    Decimal(u8, i8, Nullability),
    Float(FloatWidth, Nullability),
    Utf8(Nullability),
    Binary(Nullability),
    LocalTime(TimeUnit, Nullability),
    LocalDate(Nullability),
    Instant(TimeUnit, Nullability),
    ZonedDateTime(TimeUnit, Nullability),
    Struct(FieldNames, Vec<DType>),
    List(Box<DType>, Nullability),
    Map(Box<DType>, Box<DType>, Nullability),
}

impl DType {
    pub fn is_nullable(&self) -> bool {
        use Nullability::*;

        match self {
            Null => true,
            Bool(n) => matches!(n, Nullable),
            Int(_, _, n) => matches!(n, Nullable),
            Decimal(_, _, n) => matches!(n, Nullable),
            Float(_, n) => matches!(n, Nullable),
            Utf8(n) => matches!(n, Nullable),
            Binary(n) => matches!(n, Nullable),
            LocalTime(_, n) => matches!(n, Nullable),
            LocalDate(n) => matches!(n, Nullable),
            Instant(_, n) => matches!(n, Nullable),
            ZonedDateTime(_, n) => matches!(n, Nullable),
            Struct(_, fs) => fs.iter().all(|f| f.is_nullable()),
            List(_, n) => matches!(n, Nullable),
            Map(_, _, n) => matches!(n, Nullable),
        }
    }

    pub fn as_nullable(&self) -> Self {
        use Nullability::*;
        match self {
            Null => Null,
            Bool(_) => Bool(Nullable),
            Int(w, s, _) => Int(w.clone(), s.clone(), Nullable),
            Decimal(s, p, _) => Decimal(*s, *p, Nullable),
            Float(w, _) => Float(w.clone(), Nullable),
            Utf8(_) => Utf8(Nullable),
            Binary(_) => Binary(Nullable),
            LocalTime(u, _) => LocalTime(u.clone(), Nullable),
            LocalDate(_) => LocalDate(Nullable),
            Instant(u, _) => Instant(u.clone(), Nullable),
            ZonedDateTime(u, _) => ZonedDateTime(u.clone(), Nullable),
            Struct(_, _) => self.clone(),
            List(c, _) => List(c.clone(), Nullable),
            Map(k, v, _) => Map(k.clone(), v.clone(), Nullable),
        }
    }
}

impl Display for DType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        use Signedness::*;
        match self {
            Null => write!(f, "null"),
            Bool(n) => write!(f, "bool{}", n),
            Int(w, s, n) => match s {
                Unknown => write!(f, "int({}){}", w, n),
                Unsigned => write!(f, "uint({}){}", w, n),
                Signed => write!(f, "sint({}){}", w, n),
            },
            Decimal(p, s, n) => write!(f, "decimal({}, {}){}", p, s, n),
            Float(w, n) => write!(f, "float({}){}", w, n),
            Utf8(n) => write!(f, "utf8{}", n),
            Binary(n) => write!(f, "binary{}", n),
            LocalTime(u, n) => write!(f, "localtime({}){}", u, n),
            LocalDate(n) => write!(f, "localdate{}", n),
            Instant(u, n) => write!(f, "instant({}){}", u, n),
            ZonedDateTime(u, n) => write!(f, "zoned_date_time({}){}", u, n),
            Struct(n, dt) => write!(
                f,
                "{{{}}}",
                n.iter()
                    .zip(dt.iter())
                    .map(|(n, dt)| format!("{}={}", n, dt))
                    .join(", ")
            ),
            List(c, n) => write!(f, "list({}){}", c, n),
            Map(k, v, n) => write!(f, "map({}, {}){}", k, v, n),
        }
    }
}

impl From<PType> for &DType {
    fn from(item: PType) -> Self {
        use Nullability::*;
        use Signedness::*;

        match item {
            PType::I8 => &Int(IntWidth::_8, Signed, NonNullable),
            PType::I16 => &Int(IntWidth::_16, Signed, NonNullable),
            PType::I32 => &Int(IntWidth::_32, Signed, NonNullable),
            PType::I64 => &Int(IntWidth::_64, Signed, NonNullable),
            PType::U8 => &Int(IntWidth::_8, Unsigned, NonNullable),
            PType::U16 => &Int(IntWidth::_16, Unsigned, NonNullable),
            PType::U32 => &Int(IntWidth::_32, Unsigned, NonNullable),
            PType::U64 => &Int(IntWidth::_64, Unsigned, NonNullable),
            PType::F16 => &Float(FloatWidth::_16, NonNullable),
            PType::F32 => &Float(FloatWidth::_32, NonNullable),
            PType::F64 => &Float(FloatWidth::_64, NonNullable),
        }
    }
}

impl From<PType> for DType {
    fn from(item: PType) -> Self {
        use Nullability::*;
        use Signedness::*;

        match item {
            PType::I8 => Int(IntWidth::_8, Signed, NonNullable),
            PType::I16 => Int(IntWidth::_16, Signed, NonNullable),
            PType::I32 => Int(IntWidth::_32, Signed, NonNullable),
            PType::I64 => Int(IntWidth::_64, Signed, NonNullable),
            PType::U8 => Int(IntWidth::_8, Unsigned, NonNullable),
            PType::U16 => Int(IntWidth::_16, Unsigned, NonNullable),
            PType::U32 => Int(IntWidth::_32, Unsigned, NonNullable),
            PType::U64 => Int(IntWidth::_64, Unsigned, NonNullable),
            PType::F16 => Float(FloatWidth::_16, NonNullable),
            PType::F32 => Float(FloatWidth::_32, NonNullable),
            PType::F64 => Float(FloatWidth::_64, NonNullable),
        }
    }
}

impl TryFrom<DataType> for DType {
    type Error = EncError;

    fn try_from(value: DataType) -> Result<Self, Self::Error> {
        (&value).try_into()
    }
}

impl TryFrom<&DataType> for DType {
    type Error = EncError;

    fn try_from(value: &DataType) -> EncResult<Self> {
        use Nullability::*;
        use Signedness::*;
        match value {
            DataType::Null => Ok(Null),
            DataType::Boolean => Ok(Bool(NonNullable)),
            DataType::Int8 => Ok(Int(IntWidth::_8, Signed, NonNullable)),
            DataType::Int16 => Ok(Int(IntWidth::_16, Signed, NonNullable)),
            DataType::Int32 => Ok(Int(IntWidth::_32, Signed, NonNullable)),
            DataType::Int64 => Ok(Int(IntWidth::_64, Signed, NonNullable)),
            DataType::UInt8 => Ok(Int(IntWidth::_8, Unsigned, NonNullable)),
            DataType::UInt16 => Ok(Int(IntWidth::_16, Unsigned, NonNullable)),
            DataType::UInt32 => Ok(Int(IntWidth::_32, Unsigned, NonNullable)),
            DataType::UInt64 => Ok(Int(IntWidth::_64, Unsigned, NonNullable)),
            DataType::Float16 => Ok(Float(FloatWidth::_16, NonNullable)),
            DataType::Float32 => Ok(Float(FloatWidth::_32, NonNullable)),
            DataType::Float64 => Ok(Float(FloatWidth::_64, NonNullable)),
            DataType::Utf8 | DataType::LargeUtf8 => Ok(Utf8(NonNullable)),
            DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
                Ok(Binary(NonNullable))
            }
            // TODO(robert): what to do about this timezone?
            DataType::Timestamp(u, _) => Ok(ZonedDateTime(u.into(), NonNullable)),
            DataType::Date32 | DataType::Date64 => Ok(LocalDate(NonNullable)),
            DataType::Time32(u) | DataType::Time64(u) => Ok(LocalTime(u.into(), NonNullable)),
            DataType::List(e) | DataType::FixedSizeList(e, _) | DataType::LargeList(e) => {
                Ok(List(Box::new(e.data_type().try_into()?), NonNullable))
            }
            DataType::Struct(f) => Ok(Struct(
                f.iter().map(|f| Arc::new(f.name().clone())).collect(),
                f.iter()
                    .map(|f| f.data_type().try_into().unwrap())
                    .collect(),
            )),
            DataType::Dictionary(_, v) => v.as_ref().try_into(),
            DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => {
                Ok(Decimal(*p, *s, NonNullable))
            }
            DataType::Map(e, _) => match e.data_type() {
                DataType::Struct(f) => Ok(Map(
                    Box::new(f[0].data_type().try_into()?),
                    Box::new(f[1].data_type().try_into()?),
                    Nullable,
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
        use Signedness::*;
        match value {
            Null => DataType::Null,
            Bool(_) => DataType::Boolean,
            Int(w, s, _) => match w {
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
            Decimal(p, w, _) => DataType::Decimal128(*p, *w),
            Float(w, _) => match w {
                FloatWidth::Unknown => DataType::Float64,
                FloatWidth::_16 => DataType::Float16,
                FloatWidth::_32 => DataType::Float32,
                FloatWidth::_64 => DataType::Float64,
            },
            Utf8(_) => DataType::Utf8,
            Binary(_) => DataType::Binary,
            LocalTime(u, _) => DataType::Time64(match u {
                TimeUnit::Ns => ArrowTimeUnit::Nanosecond,
                TimeUnit::Us => ArrowTimeUnit::Microsecond,
                TimeUnit::Ms => ArrowTimeUnit::Millisecond,
                TimeUnit::S => ArrowTimeUnit::Second,
            }),
            LocalDate(_) => DataType::Date64,
            Instant(u, _) => DataType::Timestamp(
                match u {
                    TimeUnit::Ns => ArrowTimeUnit::Nanosecond,
                    TimeUnit::Us => ArrowTimeUnit::Microsecond,
                    TimeUnit::Ms => ArrowTimeUnit::Millisecond,
                    TimeUnit::S => ArrowTimeUnit::Second,
                },
                None,
            ),
            ZonedDateTime(_, _) => {
                unimplemented!("Converting ZoneDateTime to arrow datatype is not supported")
            }
            Struct(names, dtypes) => DataType::Struct(
                zip(names, dtypes)
                    .map(|(n, dt)| Field::new((**n).clone(), dt.into(), dt.is_nullable()))
                    .collect(),
            ),
            List(c, _) => DataType::List(Arc::new(Field::new(
                "element",
                c.as_ref().into(),
                c.is_nullable(),
            ))),
            Map(k, v, _) => DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", k.as_ref().into(), false),
                        Field::new("value", v.as_ref().into(), v.is_nullable()),
                    ])),
                    false,
                )),
                false,
            ),
        }
    }
}

impl From<&DType> for Fields {
    fn from(value: &DType) -> Self {
        match value {
            Struct(n, f) => Fields::from(
                n.iter()
                    .zip(f.iter())
                    .map(|(name, dtype)| {
                        Field::new((**name).clone(), dtype.into(), dtype.is_nullable())
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
        let dtype = Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable);
        let data_type: DataType = dtype.into();
        assert_eq!(data_type, DataType::Int32);
    }
}
