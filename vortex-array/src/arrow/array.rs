use arrow_array::array::{
    Array as ArrowArray, ArrayRef as ArrowArrayRef, BooleanArray as ArrowBooleanArray,
    GenericByteArray, NullArray as ArrowNullArray, PrimitiveArray as ArrowPrimitiveArray,
    StructArray as ArrowStructArray,
};
use arrow_array::array::{ArrowPrimitiveType, OffsetSizeTrait};
use arrow_array::cast::{as_null_array, AsArray};
use arrow_array::types::{
    ByteArrayType, ByteViewType, Date32Type, Date64Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{BinaryViewArray, GenericByteViewArray, StringViewArray};
use arrow_buffer::buffer::{NullBuffer, OffsetBuffer};
use arrow_buffer::{ArrowNativeType, Buffer, ScalarBuffer};
use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};
use itertools::Itertools;
use vortex_dtype::NativePType;
use vortex_dtype::{DType, PType};

use crate::array::bool::BoolArray;
use crate::array::datetime::temporal::TemporalArray;
use crate::array::datetime::TimeUnit;
use crate::array::null::NullArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::varbin::VarBinArray;
use crate::array::varbinview::VarBinViewArray;
use crate::arrow::FromArrowArray;
use crate::stats::{Stat, Statistics};
use crate::validity::Validity;
use crate::{ArrayData, IntoArray, IntoArrayData};

impl IntoArrayData for Buffer {
    fn into_array_data(self) -> ArrayData {
        PrimitiveArray::new(self.into(), PType::U8, Validity::NonNullable).into_array_data()
    }
}

impl IntoArrayData for NullBuffer {
    fn into_array_data(self) -> ArrayData {
        BoolArray::try_new(self.into_inner(), Validity::NonNullable)
            .unwrap()
            .into_array_data()
    }
}

impl<T: ArrowNativeType + NativePType> IntoArrayData for ScalarBuffer<T> {
    fn into_array_data(self) -> ArrayData {
        PrimitiveArray::new(self.into_inner().into(), T::PTYPE, Validity::NonNullable)
            .into_array_data()
    }
}

impl<O: NativePType + OffsetSizeTrait> IntoArrayData for OffsetBuffer<O> {
    fn into_array_data(self) -> ArrayData {
        let array = PrimitiveArray::new(
            self.into_inner().into_inner().into(),
            O::PTYPE,
            Validity::NonNullable,
        )
        .into_array_data();
        array.set(Stat::IsSorted, true.into());
        array.set(Stat::IsStrictSorted, true.into());
        array
    }
}

impl<T: ArrowPrimitiveType> FromArrowArray<&ArrowPrimitiveArray<T>> for ArrayData
where
    <T as ArrowPrimitiveType>::Native: NativePType,
{
    fn from_arrow(value: &ArrowPrimitiveArray<T>, nullable: bool) -> Self {
        let arr = PrimitiveArray::new(
            value.values().clone().into_inner().into(),
            T::Native::PTYPE,
            nulls(value.nulls(), nullable),
        )
        .into_array_data();

        if T::DATA_TYPE.is_numeric() {
            return arr;
        }

        match T::DATA_TYPE {
            DataType::Timestamp(time_unit, tz) => {
                let tz = tz.clone().map(|s| s.to_string());
                TemporalArray::new_timestamp(arr.into_array(), from_arrow_time_unit(time_unit), tz)
                    .into_array_data()
            }
            DataType::Time32(time_unit) => {
                TemporalArray::new_time(arr.into_array(), from_arrow_time_unit(time_unit))
                    .into_array_data()
            }
            DataType::Time64(time_unit) => {
                TemporalArray::new_time(arr.into_array(), from_arrow_time_unit(time_unit))
                    .into_array_data()
            }
            DataType::Date32 => {
                TemporalArray::new_date(arr.into_array(), TimeUnit::D).into_array_data()
            }
            DataType::Date64 => {
                TemporalArray::new_date(arr.into_array(), TimeUnit::Ms).into_array_data()
            }
            DataType::Duration(_) => todo!(),
            DataType::Interval(_) => todo!(),
            _ => panic!("Invalid data type for PrimitiveArray"),
        }
    }
}

impl<T: ByteArrayType> FromArrowArray<&GenericByteArray<T>> for ArrayData
where
    <T as ByteArrayType>::Offset: NativePType,
{
    fn from_arrow(value: &GenericByteArray<T>, nullable: bool) -> Self {
        let dtype = match T::DATA_TYPE {
            DataType::Binary | DataType::LargeBinary => DType::Binary(nullable.into()),
            DataType::Utf8 | DataType::LargeUtf8 => DType::Utf8(nullable.into()),
            _ => panic!("Invalid data type for ByteArray"),
        };
        VarBinArray::try_new(
            value.offsets().clone().into_array_data().into_array(),
            value.values().clone().into_array_data().into_array(),
            dtype,
            nulls(value.nulls(), nullable),
        )
        .unwrap()
        .into_array_data()
    }
}

impl<T: ByteViewType> FromArrowArray<&GenericByteViewArray<T>> for ArrayData {
    fn from_arrow(value: &GenericByteViewArray<T>, nullable: bool) -> Self {
        let dtype = match T::DATA_TYPE {
            DataType::BinaryView => DType::Binary(nullable.into()),
            DataType::Utf8View => DType::Utf8(nullable.into()),
            _ => panic!("Invalid data type for ByteViewArray"),
        };
        VarBinViewArray::try_new(
            value.views().inner().clone().into_array_data().into_array(),
            value
                .data_buffers()
                .iter()
                .map(|b| b.clone().into_array_data().into_array())
                .collect::<Vec<_>>(),
            dtype,
            nulls(value.nulls(), nullable),
        )
        .unwrap()
        .into_array_data()
    }
}

impl FromArrowArray<&ArrowBooleanArray> for ArrayData {
    fn from_arrow(value: &ArrowBooleanArray, nullable: bool) -> Self {
        BoolArray::try_new(value.values().clone(), nulls(value.nulls(), nullable))
            .unwrap()
            .into_array_data()
    }
}

impl FromArrowArray<&ArrowStructArray> for ArrayData {
    fn from_arrow(value: &ArrowStructArray, nullable: bool) -> Self {
        // TODO(ngates): how should we deal with Arrow "logical nulls"?
        assert!(!nullable);
        StructArray::try_new(
            value
                .column_names()
                .iter()
                .map(|s| (*s).into())
                .collect_vec()
                .into(),
            value
                .columns()
                .iter()
                .zip(value.fields())
                .map(|(c, field)| Self::from_arrow(c.clone(), field.is_nullable()).into_array())
                .collect(),
            value.len(),
            nulls(value.nulls(), nullable),
        )
        .unwrap()
        .into_array_data()
    }
}

impl FromArrowArray<&ArrowNullArray> for ArrayData {
    fn from_arrow(value: &ArrowNullArray, nullable: bool) -> Self {
        assert!(nullable);
        NullArray::new(value.len()).into_array_data()
    }
}

fn nulls(nulls: Option<&NullBuffer>, nullable: bool) -> Validity {
    if nullable {
        nulls
            .map(|nulls| {
                if nulls.null_count() == nulls.len() {
                    Validity::AllInvalid
                } else {
                    Validity::from(nulls.inner().clone())
                }
            })
            .unwrap_or_else(|| Validity::AllValid)
    } else {
        assert!(nulls.is_none());
        Validity::NonNullable
    }
}

impl FromArrowArray<ArrowArrayRef> for ArrayData {
    fn from_arrow(array: ArrowArrayRef, nullable: bool) -> Self {
        match array.data_type() {
            DataType::Boolean => Self::from_arrow(array.as_boolean(), nullable),
            DataType::UInt8 => Self::from_arrow(array.as_primitive::<UInt8Type>(), nullable),
            DataType::UInt16 => Self::from_arrow(array.as_primitive::<UInt16Type>(), nullable),
            DataType::UInt32 => Self::from_arrow(array.as_primitive::<UInt32Type>(), nullable),
            DataType::UInt64 => Self::from_arrow(array.as_primitive::<UInt64Type>(), nullable),
            DataType::Int8 => Self::from_arrow(array.as_primitive::<Int8Type>(), nullable),
            DataType::Int16 => Self::from_arrow(array.as_primitive::<Int16Type>(), nullable),
            DataType::Int32 => Self::from_arrow(array.as_primitive::<Int32Type>(), nullable),
            DataType::Int64 => Self::from_arrow(array.as_primitive::<Int64Type>(), nullable),
            DataType::Float16 => Self::from_arrow(array.as_primitive::<Float16Type>(), nullable),
            DataType::Float32 => Self::from_arrow(array.as_primitive::<Float32Type>(), nullable),
            DataType::Float64 => Self::from_arrow(array.as_primitive::<Float64Type>(), nullable),
            DataType::Utf8 => Self::from_arrow(array.as_string::<i32>(), nullable),
            DataType::LargeUtf8 => Self::from_arrow(array.as_string::<i64>(), nullable),
            DataType::Binary => Self::from_arrow(array.as_binary::<i32>(), nullable),
            DataType::LargeBinary => Self::from_arrow(array.as_binary::<i64>(), nullable),
            DataType::BinaryView => Self::from_arrow(
                array.as_any().downcast_ref::<BinaryViewArray>().unwrap(),
                nullable,
            ),
            DataType::Utf8View => Self::from_arrow(
                array.as_any().downcast_ref::<StringViewArray>().unwrap(),
                nullable,
            ),
            DataType::Struct(_) => Self::from_arrow(array.as_struct(), nullable),
            DataType::Null => Self::from_arrow(as_null_array(&array), nullable),
            DataType::Timestamp(u, _) => match u {
                ArrowTimeUnit::Second => {
                    Self::from_arrow(array.as_primitive::<TimestampSecondType>(), nullable)
                }
                ArrowTimeUnit::Millisecond => {
                    Self::from_arrow(array.as_primitive::<TimestampMillisecondType>(), nullable)
                }
                ArrowTimeUnit::Microsecond => {
                    Self::from_arrow(array.as_primitive::<TimestampMicrosecondType>(), nullable)
                }
                ArrowTimeUnit::Nanosecond => {
                    Self::from_arrow(array.as_primitive::<TimestampNanosecondType>(), nullable)
                }
            },
            DataType::Date32 => Self::from_arrow(array.as_primitive::<Date32Type>(), nullable),
            DataType::Date64 => Self::from_arrow(array.as_primitive::<Date64Type>(), nullable),
            DataType::Time32(u) => match u {
                ArrowTimeUnit::Second => {
                    Self::from_arrow(array.as_primitive::<Time32SecondType>(), nullable)
                }
                ArrowTimeUnit::Millisecond => {
                    Self::from_arrow(array.as_primitive::<Time32MillisecondType>(), nullable)
                }
                _ => unreachable!(),
            },
            DataType::Time64(u) => match u {
                ArrowTimeUnit::Microsecond => {
                    Self::from_arrow(array.as_primitive::<Time64MicrosecondType>(), nullable)
                }
                ArrowTimeUnit::Nanosecond => {
                    Self::from_arrow(array.as_primitive::<Time64NanosecondType>(), nullable)
                }
                _ => unreachable!(),
            },
            DataType::Duration(u) => match u {
                ArrowTimeUnit::Second => {
                    Self::from_arrow(array.as_primitive::<DurationSecondType>(), nullable)
                }
                ArrowTimeUnit::Millisecond => {
                    Self::from_arrow(array.as_primitive::<DurationMillisecondType>(), nullable)
                }
                ArrowTimeUnit::Microsecond => {
                    Self::from_arrow(array.as_primitive::<DurationMicrosecondType>(), nullable)
                }
                ArrowTimeUnit::Nanosecond => {
                    Self::from_arrow(array.as_primitive::<DurationNanosecondType>(), nullable)
                }
            },
            _ => panic!(
                "TODO(robert): Missing array encoding for dtype {}",
                array.data_type().clone()
            ),
        }
    }
}

fn from_arrow_time_unit(time_unit: ArrowTimeUnit) -> TimeUnit {
    match time_unit {
        ArrowTimeUnit::Second => TimeUnit::S,
        ArrowTimeUnit::Millisecond => TimeUnit::Ms,
        ArrowTimeUnit::Microsecond => TimeUnit::Us,
        ArrowTimeUnit::Nanosecond => TimeUnit::Ns,
    }
}
