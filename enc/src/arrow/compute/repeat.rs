use std::iter;
use std::sync::Arc;

use arrow::array::builder::{BooleanBuilder, PrimitiveBuilder};
use arrow::array::cast::AsArray;
use arrow::array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow::array::{ArrayRef, ArrowPrimitiveType, Datum, NullArray, PrimitiveArray};
use arrow::datatypes::DataType;

use crate::array::bool;

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
            let mut boolbuilder = BooleanBuilder::with_capacity(n);
            if arr.is_valid(0) {
                boolbuilder.append_slice(
                    iter::repeat(arr.as_boolean().value(0))
                        .take(n)
                        .collect::<Vec<bool>>()
                        .as_slice(),
                );
            } else {
                boolbuilder.append_nulls(n);
            }
            Arc::new(boolbuilder.finish())
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
    let mut arr = PrimitiveBuilder::<T>::with_capacity(n);
    if let Some(v) = value {
        unsafe {
            arr.append_trusted_len_iter(iter::repeat(v).take(n));
        }
    } else {
        arr.append_nulls(n);
    }
    Arc::new(arr.finish())
}

#[cfg(test)]
mod test {
    use arrow::array::cast::AsArray;
    use arrow::array::types::UInt64Type;
    use arrow::array::{Scalar, UInt64Array};

    use super::*;

    #[test]
    fn test_repeat() {
        let scalar = Scalar::new(UInt64Array::from(vec![47]));
        let array = repeat(&scalar, 100);
        assert_eq!(array.len(), 100);
        assert_eq!(array.as_primitive::<UInt64Type>().value(50), 47);
    }
}
