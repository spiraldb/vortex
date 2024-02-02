use arrow::array::cast::AsArray;
use arrow::array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow::array::{
    as_null_array, Array as ArrowArray, ArrayRef as ArrowArrayRef,
    BooleanArray as ArrowBooleanArray, GenericByteArray, NullArray as ArrowNullArray,
    PrimitiveArray as ArrowPrimitiveArray, StructArray as ArrowStructArray,
};
use arrow::array::{ArrowPrimitiveType, OffsetSizeTrait};
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::{
    ByteArrayType, DataType, Date32Type, Date64Type, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimeUnit,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};

use crate::array::bool::BoolArray;
use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::struct_::StructArray;
use crate::array::typed::TypedArray;
use crate::array::varbin::VarBinArray;
use crate::array::{Array, ArrayRef};
use crate::ptype::PType;
use crate::scalar::{NullScalar, Scalar};

impl From<&Buffer> for ArrayRef {
    fn from(value: &Buffer) -> Self {
        PrimitiveArray::new(PType::U8, value.to_owned()).boxed()
    }
}

impl<O: OffsetSizeTrait> From<&OffsetBuffer<O>> for ArrayRef {
    fn from(value: &OffsetBuffer<O>) -> Self {
        let ptype = if O::IS_LARGE { PType::I64 } else { PType::I32 };
        PrimitiveArray::new(ptype, value.inner().inner().to_owned()).boxed()
    }
}

impl<T: ArrowPrimitiveType> From<&ArrowPrimitiveArray<T>> for ArrayRef {
    fn from(value: &ArrowPrimitiveArray<T>) -> Self {
        let ptype: PType = (&T::DATA_TYPE).try_into().unwrap();
        let arr = PrimitiveArray::new(ptype, value.values().inner().to_owned()).boxed();
        if T::DATA_TYPE.is_numeric() {
            arr
        } else {
            TypedArray::new(arr, T::DATA_TYPE.try_into().unwrap()).boxed()
        }
    }
}

impl<T: ByteArrayType> From<&GenericByteArray<T>> for ArrayRef {
    fn from(value: &GenericByteArray<T>) -> Self {
        VarBinArray::new(
            value.offsets().into(),
            value.values().into(),
            T::DATA_TYPE.try_into().unwrap(),
        )
        .boxed()
    }
}

impl From<&ArrowBooleanArray> for ArrayRef {
    fn from(value: &ArrowBooleanArray) -> Self {
        BoolArray::new(value.values().to_owned()).boxed()
    }
}

impl From<&ArrowStructArray> for ArrayRef {
    fn from(value: &ArrowStructArray) -> Self {
        StructArray::new(
            value.column_names(),
            value
                .columns()
                .iter()
                .map(|c| (*c).to_owned().into())
                .collect(),
        )
        .boxed()
    }
}

impl From<&ArrowNullArray> for ArrayRef {
    fn from(value: &ArrowNullArray) -> Self {
        ConstantArray::new(NullScalar::new().boxed(), value.len()).boxed()
    }
}

impl From<ArrowArrayRef> for ArrayRef {
    fn from(array: ArrowArrayRef) -> Self {
        match array.data_type() {
            DataType::Boolean => array.as_boolean().into(),
            DataType::UInt8 => array.as_primitive::<UInt8Type>().into(),
            DataType::UInt16 => array.as_primitive::<UInt16Type>().into(),
            DataType::UInt32 => array.as_primitive::<UInt32Type>().into(),
            DataType::UInt64 => array.as_primitive::<UInt64Type>().into(),
            DataType::Int8 => array.as_primitive::<Int8Type>().into(),
            DataType::Int16 => array.as_primitive::<Int16Type>().into(),
            DataType::Int32 => array.as_primitive::<Int32Type>().into(),
            DataType::Int64 => array.as_primitive::<Int64Type>().into(),
            DataType::Float16 => array.as_primitive::<Float16Type>().into(),
            DataType::Float32 => array.as_primitive::<Float32Type>().into(),
            DataType::Float64 => array.as_primitive::<Float64Type>().into(),
            DataType::Utf8 => array.as_string::<i32>().into(),
            DataType::LargeUtf8 => array.as_string::<i64>().into(),
            DataType::Binary => array.as_binary::<i32>().into(),
            DataType::LargeBinary => array.as_binary::<i64>().into(),
            DataType::Struct(_) => array.as_struct().into(),
            DataType::Null => as_null_array(array.as_ref()).into(),
            DataType::Timestamp(u, _) => match u {
                TimeUnit::Second => array.as_primitive::<TimestampSecondType>().into(),
                TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().into(),
                TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().into(),
                TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().into(),
            },
            DataType::Date32 => array.as_primitive::<Date32Type>().into(),
            DataType::Date64 => array.as_primitive::<Date64Type>().into(),
            DataType::Time32(u) => match u {
                TimeUnit::Second => array.as_primitive::<Time32SecondType>().into(),
                TimeUnit::Millisecond => array.as_primitive::<Time32MillisecondType>().into(),
                _ => unreachable!(),
            },
            DataType::Time64(u) => match u {
                TimeUnit::Microsecond => array.as_primitive::<Time64MicrosecondType>().into(),
                TimeUnit::Nanosecond => array.as_primitive::<Time64NanosecondType>().into(),
                _ => unreachable!(),
            },
            DataType::Duration(u) => match u {
                TimeUnit::Second => array.as_primitive::<DurationSecondType>().into(),
                TimeUnit::Millisecond => array.as_primitive::<DurationMillisecondType>().into(),
                TimeUnit::Microsecond => array.as_primitive::<DurationMicrosecondType>().into(),
                TimeUnit::Nanosecond => array.as_primitive::<DurationNanosecondType>().into(),
            },
            _ => panic!(
                "TODO(robert): Missing array encoding for dtype {}",
                array.data_type().clone()
            ),
        }
    }
}
