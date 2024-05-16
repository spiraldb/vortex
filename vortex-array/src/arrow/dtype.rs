use std::sync::Arc;

use arrow_schema::TimeUnit as ArrowTimeUnit;
use arrow_schema::{DataType, Field, SchemaRef};
use itertools::Itertools;
use vortex_dtype::{DType, Nullability};
use vortex_dtype::{PType, StructDType};
use vortex_error::{vortex_err, VortexResult};

use crate::array::datetime::{LocalDateTimeArray, TimeUnit};
use crate::arrow::{FromArrowType, TryFromArrowType};

impl TryFromArrowType<&DataType> for PType {
    fn try_from_arrow(value: &DataType) -> VortexResult<Self> {
        match value {
            DataType::Int8 => Ok(Self::I8),
            DataType::Int16 => Ok(Self::I16),
            DataType::Int32 => Ok(Self::I32),
            DataType::Int64 => Ok(Self::I64),
            DataType::UInt8 => Ok(Self::U8),
            DataType::UInt16 => Ok(Self::U16),
            DataType::UInt32 => Ok(Self::U32),
            DataType::UInt64 => Ok(Self::U64),
            DataType::Float16 => Ok(Self::F16),
            DataType::Float32 => Ok(Self::F32),
            DataType::Float64 => Ok(Self::F64),
            _ => Err(vortex_err!(
                "Arrow datatype {:?} cannot be converted to ptype",
                value
            )),
        }
    }
}

impl FromArrowType<SchemaRef> for DType {
    fn from_arrow(value: SchemaRef) -> Self {
        Self::Struct(
            StructDType::new(
                value
                    .fields()
                    .iter()
                    .map(|f| f.name().as_str().into())
                    .collect_vec()
                    .into(),
                value
                    .fields()
                    .iter()
                    .map(|f| Self::from_arrow(f.as_ref()))
                    .collect_vec(),
            ),
            Nullability::NonNullable,
        )
    }
}

impl FromArrowType<&Field> for DType {
    fn from_arrow(field: &Field) -> Self {
        use vortex_dtype::DType::*;

        let nullability: Nullability = field.is_nullable().into();

        if let Ok(ptype) = PType::try_from_arrow(field.data_type()) {
            return Primitive(ptype, nullability);
        }

        match field.data_type() {
            DataType::Null => Null,
            DataType::Boolean => Bool(nullability),
            DataType::Utf8 | DataType::LargeUtf8 => Utf8(nullability),
            DataType::Binary | DataType::LargeBinary => Binary(nullability),
            DataType::Timestamp(time_unit, tz) => match tz {
                None => Extension(LocalDateTimeArray::ext_dtype(time_unit.into()), nullability),
                Some(_) => unimplemented!("Timezone not yet supported"),
            },
            // DataType::Date32 => localdate(IntWidth::_32, nullability),
            // DataType::Date64 => localdate(IntWidth::_64, nullability),
            // DataType::Time32(u) => localtime(u.into(), IntWidth::_32, nullability),
            // DataType::Time64(u) => localtime(u.into(), IntWidth::_64, nullability),
            DataType::List(e) | DataType::LargeList(e) => {
                List(Arc::new(Self::from_arrow(e.as_ref())), nullability)
            }
            DataType::Struct(f) => Struct(
                StructDType::new(
                    f.iter()
                        .map(|f| f.name().as_str().into())
                        .collect_vec()
                        .into(),
                    f.iter().map(|f| Self::from_arrow(f.as_ref())).collect_vec(),
                ),
                nullability,
            ),
            _ => unimplemented!("Arrow data type not yet supported: {:?}", field.data_type()),
        }
    }
}

impl From<&ArrowTimeUnit> for TimeUnit {
    fn from(value: &ArrowTimeUnit) -> Self {
        match value {
            ArrowTimeUnit::Second => Self::S,
            ArrowTimeUnit::Millisecond => Self::Ms,
            ArrowTimeUnit::Microsecond => Self::Us,
            ArrowTimeUnit::Nanosecond => Self::Ns,
        }
    }
}

impl From<TimeUnit> for ArrowTimeUnit {
    fn from(value: TimeUnit) -> Self {
        match value {
            TimeUnit::S => Self::Second,
            TimeUnit::Ms => Self::Millisecond,
            TimeUnit::Us => Self::Microsecond,
            TimeUnit::Ns => Self::Nanosecond,
        }
    }
}
