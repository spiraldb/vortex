use std::borrow::Borrow;
use std::iter;
use std::sync::Arc;

use arrow::array::types::{
    Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow::array::{Array as ArrowArray, ArrayRef, Scalar as ArrowScalar};
use arrow::array::{ArrowPrimitiveType, PrimitiveArray as ArrowPrimitiveArray};
use arrow::datatypes::DataType;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::{DType, PType};

#[derive(Debug, Clone)]
pub struct PrimitiveArray {
    buffer: ArrayRef,
    ptype: PType,
    dtype: DType,
}

macro_rules! vec_to_primitive_array {
    ($arrow_type:ty, $values:expr) => {{
        unsafe {
            let casted_values: Vec<<$arrow_type as ArrowPrimitiveType>::Native> =
                std::mem::transmute($values);

            Self::new(Arc::new(Into::<ArrowPrimitiveArray<$arrow_type>>::into(
                casted_values,
            )))
        }
    }};
}

impl PrimitiveArray {
    pub fn new<T: ArrowPrimitiveType>(buffer: Arc<ArrowPrimitiveArray<T>>) -> Self {
        let ptype: PType = T::DATA_TYPE.borrow().try_into().unwrap();
        Self {
            buffer,
            ptype,
            dtype: ptype.into(),
        }
    }

    pub fn from_vec<T: ArrowPrimitiveType>(values: Vec<T::Native>) -> Self {
        match T::DATA_TYPE {
            DataType::Int8 => {
                unsafe {
                    let casted_values: Vec<<Int8Type as ArrowPrimitiveType>::Native> =
                        std::mem::transmute(values);
                    Self::new(Arc::new(Into::<ArrowPrimitiveArray<Int8Type>>::into(
                        casted_values,
                    )))
                }
                // vec_to_primitive_array!(Int8Type, values)
            }
            DataType::Int16 => vec_to_primitive_array!(Int16Type, values),
            DataType::Int32 => vec_to_primitive_array!(Int32Type, values),
            DataType::Int64 => vec_to_primitive_array!(Int64Type, values),
            DataType::UInt8 => vec_to_primitive_array!(UInt8Type, values),
            DataType::UInt16 => vec_to_primitive_array!(UInt16Type, values),
            DataType::UInt32 => vec_to_primitive_array!(UInt32Type, values),
            DataType::UInt64 => vec_to_primitive_array!(UInt64Type, values),
            DataType::Float16 => vec_to_primitive_array!(Float16Type, values),
            DataType::Float32 => vec_to_primitive_array!(Float32Type, values),
            DataType::Float64 => vec_to_primitive_array!(Float64Type, values),
            _ => panic!("Unsupported primitive array type"),
        }
    }
}

impl ArrayEncoding for PrimitiveArray {
    #[inline]
    fn len(&self) -> usize {
        self.buffer.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    #[inline]
    fn dtype(&self) -> DType {
        self.dtype.clone()
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        if index >= self.len() {
            Err(EncError::OutOfBounds(index, 0, self.len()))
        } else {
            Ok(ArrowScalar::new(self.buffer.as_ref().slice(index, 1)).into())
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(iter::once(self.buffer.clone()))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<Array> {
        self.check_slice_bounds(start, stop)?;

        Ok(Array::Primitive(Self {
            buffer: self.buffer.slice(start, stop - start),
            ptype: self.ptype,
            dtype: self.dtype.clone(),
        }))
    }
}

#[cfg(test)]
mod test {
    use crate::types::IntWidth;

    use super::*;

    #[test]
    fn from_arrow() {
        let arr = PrimitiveArray::from_vec::<Int32Type>(vec![1, 2, 3]);
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.ptype, PType::I32);
        assert_eq!(arr.dtype, DType::Int(IntWidth::_32));

        // Ensure we can fetch the scalar at the given index.
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(1));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(3));
    }

    #[test]
    fn slice() {
        let arr = PrimitiveArray::from_vec::<Int32Type>(vec![1, 2, 3, 4, 5])
            .slice(1, 4)
            .unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(3));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(4));
    }
}
