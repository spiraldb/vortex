use std::sync::Arc;

use arrow::array::cast::AsArray;
use arrow::array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow::array::{ArrayRef, ArrowPrimitiveType, BooleanArray, Datum, NullArray, PrimitiveArray};
use arrow::buffer::BooleanBuffer;
use arrow::datatypes::DataType;

macro_rules! repeat_primitive {
    ($arrow_type:ty, $arr:expr, $n:expr) => {{
        if $arr.is_null(0) {
            return repeat_primitive::<$arrow_type>(None, $n) as ArrayRef;
        }

        repeat_primitive::<$arrow_type>(Some($arr.as_primitive::<$arrow_type>().value(0)), $n)
            as ArrayRef
    }};
}

pub fn repeat(scalar: &dyn Datum, n: usize) -> ArrayRef {
    let (arr, is_scalar) = scalar.get();
    assert!(is_scalar, "Datum was not a scalar");
    match arr.data_type() {
        DataType::Null => Arc::new(NullArray::new(n)),
        DataType::Boolean => {
            if arr.is_valid(0) {
                if arr.as_boolean().value(0) {
                    Arc::new(BooleanArray::from(BooleanBuffer::new_set(n)))
                } else {
                    Arc::new(BooleanArray::from(BooleanBuffer::new_unset(n)))
                }
            } else {
                Arc::new(BooleanArray::new_null(n))
            }
        }
        DataType::UInt8 => repeat_primitive!(UInt8Type, arr, n),
        DataType::UInt16 => repeat_primitive!(UInt16Type, arr, n),
        DataType::UInt32 => repeat_primitive!(UInt32Type, arr, n),
        DataType::UInt64 => repeat_primitive!(UInt64Type, arr, n),
        DataType::Int8 => repeat_primitive!(Int8Type, arr, n),
        DataType::Int16 => repeat_primitive!(Int16Type, arr, n),
        DataType::Int32 => repeat_primitive!(Int32Type, arr, n),
        DataType::Int64 => repeat_primitive!(Int64Type, arr, n),
        DataType::Float16 => repeat_primitive!(Float16Type, arr, n),
        DataType::Float32 => repeat_primitive!(Float32Type, arr, n),
        DataType::Float64 => repeat_primitive!(Float64Type, arr, n),
        _ => todo!("Not implemented yet"),
    }
}

fn repeat_primitive<T: ArrowPrimitiveType>(
    value: Option<T::Native>,
    n: usize,
) -> Arc<PrimitiveArray<T>> {
    Arc::new(
        value
            .map(|v| PrimitiveArray::from_value(v, n))
            .unwrap_or_else(|| PrimitiveArray::new_null(n)),
    )
}

#[cfg(test)]
mod test {
    use crate::arrow::compute::repeat;
    use arrow::array::cast::AsArray;
    use arrow::array::types::UInt64Type;
    use arrow::array::{Scalar, UInt64Array};

    #[test]
    fn test_repeat() {
        let scalar = Scalar::new(UInt64Array::from(vec![47]));
        let array = repeat(&scalar, 100);
        assert_eq!(array.len(), 100);
        assert_eq!(array.as_primitive::<UInt64Type>().value(50), 47);
    }
}
