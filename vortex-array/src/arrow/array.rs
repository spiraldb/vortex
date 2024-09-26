use arrow_array::array::{
    Array as ArrowArray, ArrayRef as ArrowArrayRef, ArrowPrimitiveType,
    BooleanArray as ArrowBooleanArray, GenericByteArray, NullArray as ArrowNullArray,
    OffsetSizeTrait, PrimitiveArray as ArrowPrimitiveArray, StructArray as ArrowStructArray,
};
use arrow_array::cast::{as_null_array, AsArray};
use arrow_array::types::{
    ByteArrayType, ByteViewType, Date32Type, Date64Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Float16Type, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use arrow_array::{BinaryViewArray, GenericByteViewArray, StringViewArray};
use arrow_buffer::buffer::{NullBuffer, OffsetBuffer};
use arrow_buffer::{ArrowNativeType, Buffer, ScalarBuffer};
use arrow_schema::{DataType, TimeUnit as ArrowTimeUnit};
use itertools::Itertools;
use vortex_datetime_dtype::TimeUnit;
use vortex_dtype::{DType, NativePType, PType};
use vortex_error::{vortex_panic, VortexExpect as _};

use crate::array::{
    BoolArray, NullArray, PrimitiveArray, StructArray, TemporalArray, VarBinArray, VarBinViewArray,
};
use crate::arrow::FromArrowArray;
use crate::stats::{ArrayStatistics, Stat};
use crate::validity::Validity;
use crate::{Array, IntoArray};

impl From<Buffer> for Array {
    fn from(value: Buffer) -> Self {
        PrimitiveArray::new(value.into(), PType::U8, Validity::NonNullable).into_array()
    }
}

impl From<NullBuffer> for Array {
    fn from(value: NullBuffer) -> Self {
        BoolArray::try_new(value.into_inner(), Validity::NonNullable)
            .vortex_expect("Failed to convert null buffer to BoolArray")
            .into_array()
    }
}

impl<T> From<ScalarBuffer<T>> for Array
where
    T: ArrowNativeType + NativePType,
{
    fn from(value: ScalarBuffer<T>) -> Self {
        PrimitiveArray::new(value.into_inner().into(), T::PTYPE, Validity::NonNullable).into_array()
    }
}

impl<O> From<OffsetBuffer<O>> for Array
where
    O: NativePType + OffsetSizeTrait,
{
    fn from(value: OffsetBuffer<O>) -> Self {
        let primitive = PrimitiveArray::new(
            value.into_inner().into_inner().into(),
            O::PTYPE,
            Validity::NonNullable,
        );
        primitive.statistics().set(Stat::IsSorted, true.into());
        primitive
            .statistics()
            .set(Stat::IsStrictSorted, true.into());
        primitive.into_array()
    }
}

impl<T: ArrowPrimitiveType> FromArrowArray<&ArrowPrimitiveArray<T>> for Array
where
    <T as ArrowPrimitiveType>::Native: NativePType,
{
    fn from_arrow(value: &ArrowPrimitiveArray<T>, nullable: bool) -> Self {
        let arr = PrimitiveArray::new(
            value.values().clone().into_inner().into(),
            T::Native::PTYPE,
            nulls(value.nulls(), nullable),
        );

        if T::DATA_TYPE.is_numeric() {
            return arr.into();
        }

        match T::DATA_TYPE {
            DataType::Timestamp(time_unit, tz) => {
                let tz = tz.clone().map(|s| s.to_string());
                TemporalArray::new_timestamp(arr.into(), time_unit.into(), tz).into()
            }
            DataType::Time32(time_unit) => {
                TemporalArray::new_time(arr.into(), time_unit.into()).into()
            }
            DataType::Time64(time_unit) => {
                TemporalArray::new_time(arr.into(), time_unit.into()).into()
            }
            DataType::Date32 => TemporalArray::new_date(arr.into(), TimeUnit::D).into(),
            DataType::Date64 => TemporalArray::new_date(arr.into(), TimeUnit::Ms).into(),
            DataType::Duration(_) => unimplemented!(),
            DataType::Interval(_) => unimplemented!(),
            _ => vortex_panic!("Invalid data type for PrimitiveArray: {}", T::DATA_TYPE),
        }
    }
}

impl<T: ByteArrayType> FromArrowArray<&GenericByteArray<T>> for Array
where
    <T as ByteArrayType>::Offset: NativePType,
{
    fn from_arrow(value: &GenericByteArray<T>, nullable: bool) -> Self {
        let dtype = match T::DATA_TYPE {
            DataType::Binary | DataType::LargeBinary => DType::Binary(nullable.into()),
            DataType::Utf8 | DataType::LargeUtf8 => DType::Utf8(nullable.into()),
            _ => vortex_panic!("Invalid data type for ByteArray: {}", T::DATA_TYPE),
        };
        VarBinArray::try_new(
            value.offsets().clone().into(),
            value.values().clone().into(),
            dtype,
            nulls(value.nulls(), nullable),
        )
        .vortex_expect("Failed to convert Arrow GenericByteArray to Vortex VarBinArray")
        .into()
    }
}

impl<T: ByteViewType> FromArrowArray<&GenericByteViewArray<T>> for Array {
    fn from_arrow(value: &GenericByteViewArray<T>, nullable: bool) -> Self {
        let dtype = match T::DATA_TYPE {
            DataType::BinaryView => DType::Binary(nullable.into()),
            DataType::Utf8View => DType::Utf8(nullable.into()),
            _ => vortex_panic!("Invalid data type for ByteViewArray: {}", T::DATA_TYPE),
        };
        VarBinViewArray::try_new(
            value.views().inner().clone().into(),
            value
                .data_buffers()
                .iter()
                .map(|b| b.clone().into())
                .collect::<Vec<_>>(),
            dtype,
            nulls(value.nulls(), nullable),
        )
        .vortex_expect("Failed to convert Arrow GenericByteViewArray to Vortex VarBinViewArray")
        .into()
    }
}

impl FromArrowArray<&ArrowBooleanArray> for Array {
    fn from_arrow(value: &ArrowBooleanArray, nullable: bool) -> Self {
        BoolArray::try_new(value.values().clone(), nulls(value.nulls(), nullable))
            .vortex_expect("Failed to convert Arrow BooleanArray to Vortex BoolArray")
            .into()
    }
}

impl FromArrowArray<&ArrowStructArray> for Array {
    fn from_arrow(value: &ArrowStructArray, nullable: bool) -> Self {
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
                .map(|(c, field)| Self::from_arrow(c.clone(), field.is_nullable()))
                .collect(),
            value.len(),
            nulls(value.nulls(), nullable),
        )
        .vortex_expect("Failed to convert Arrow StructArray to Vortex StructArray")
        .into()
    }
}

impl FromArrowArray<&ArrowNullArray> for Array {
    fn from_arrow(value: &ArrowNullArray, nullable: bool) -> Self {
        assert!(nullable);
        NullArray::new(value.len()).into()
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

impl FromArrowArray<ArrowArrayRef> for Array {
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
                array
                    .as_any()
                    .downcast_ref::<BinaryViewArray>()
                    .vortex_expect("Expected Arrow BinaryViewArray for DataType::BinaryView"),
                nullable,
            ),
            DataType::Utf8View => Self::from_arrow(
                array
                    .as_any()
                    .downcast_ref::<StringViewArray>()
                    .vortex_expect("Expected Arrow StringViewArray for DataType::Utf8View"),
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
            _ => vortex_panic!(
                "Array encoding not implementedfor Arrow data type {}",
                array.data_type().clone()
            ),
        }
    }
}
