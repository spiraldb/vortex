use std::cmp::min;

use arrow2::array::MutablePrimitiveArray as ArrowMutablePrimitiveArray;
use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;
use arrow2::compute::cast::CastOptions;
use arrow2::datatypes::DataType;

use crate::array::primitive::PrimitiveArray;
use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::arrow::compat;
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::{DType, IntWidth};

#[derive(Debug, Clone, PartialEq)]
pub struct REEArray {
    ends: Box<Array>,
    values: Box<Array>,
    length: usize,
}

impl REEArray {
    pub fn new(ends: Array, values: Array) -> Self {
        let length = run_ends_logical_length(&ends);
        Self {
            ends: Box::new(ends),
            values: Box::new(values),
            length,
        }
    }

    pub fn find_physical_index(&self, index: usize) -> Option<usize> {
        find_physical_index(self.ends.as_ref(), index)
    }
}

impl ArrayEncoding for REEArray {
    #[inline]
    fn len(&self) -> usize {
        self.length
    }
    #[inline]
    fn is_empty(&self) -> bool {
        self.length == 0
    }
    #[inline]
    fn dtype(&self) -> DType {
        self.values.dtype()
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        self.find_physical_index(index)
            .ok_or(EncError::OutOfBounds(index, 0, self.length))
            .and_then(|run| self.values.scalar_at(run))
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!();
        // let mut last_offset: u64 = 0;
        // let ends_arrays = self
        //     .ends
        //     .iter_arrow()
        //     .map(|b| {
        //         arrow2::compute::cast::cast(b.as_ref(), &DataType::UInt64, CastOptions::default())
        //             .unwrap()
        //     })
        //     .flat_map(|v| {
        //         let primitive_array = v.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
        //         primitive_array.values_iter()
        //     })
        //     .map(|offset| {
        //         let run_length = offset - last_offset;
        //         last_offset = *offset;
        //         run_length
        //     });
        // let values_array = self.values.iter_arrow()
    }

    fn slice(&self, offset: usize, length: usize) -> EncResult<Array> {
        self.check_slice_bounds(offset, length)?;

        // TODO(robert): Make this 0 copy, and move most of this logic to iter arrow
        let physical_offset = self
            .find_physical_index(offset)
            .unwrap_or_else(|| panic!("Index {} not found in array", offset));
        let physical_length = self
            .find_physical_index(offset + length)
            .unwrap_or_else(|| {
                panic!(
                    "Length {} is larger than the array length {}",
                    length, self.length
                )
            });

        // TODO(robert): Do better? Compute? This is subtract with limiting
        let mut arrow_ends = ArrowMutablePrimitiveArray::<u64>::new();
        let mut left_to_skip = physical_offset;
        let mut current_size: usize = 0;
        for chunk in self.ends.iter_arrow() {
            let cast_result = arrow2::compute::cast::cast(
                chunk.as_ref(),
                &DataType::UInt64,
                CastOptions::default(),
            )
            .unwrap();
            let casted = cast_result
                .as_any()
                .downcast_ref::<ArrowPrimitiveArray<u64>>()
                .unwrap();
            let mut mapped_values = casted
                .values()
                .iter()
                .skip(min(casted.len(), left_to_skip))
                .map(|v| v - (offset as u64))
                .take_while(|v| {
                    let tmp_size = current_size;
                    current_size += *v as usize;
                    tmp_size <= length
                })
                .collect::<Vec<_>>();
            if mapped_values.is_empty() {
                break;
            }
            if current_size > length {
                if let Some(last_end) = mapped_values.last_mut() {
                    *last_end = length as u64;
                }
            }

            arrow_ends.extend_trusted_len_values(mapped_values.iter().copied());
            left_to_skip -= min(casted.len(), left_to_skip);
        }

        Ok(Array::REE(Self::new(
            PrimitiveArray::new(Box::new(arrow_ends.into())).into(),
            self.values
                .clone()
                .slice(physical_offset, physical_length)?,
        )))
    }
}

/// Gets the logical end of ends array of run end encoding.
fn run_ends_logical_length(ends: &Array) -> usize {
    ends.scalar_at(ends.len() - 1)
        .and_then(|end| end.as_ref().try_into())
        .unwrap_or_else(|_| panic!("Couldn't convert ends to usize"))
}

pub fn find_physical_index(array: &Array, index: usize) -> Option<usize> {
    use polars_core::prelude::*;
    use polars_ops::prelude::*;

    let ends_chunks: Vec<ArrayRef> = array
        .iter_arrow()
        .map(|chunk| compat::into_polars(chunk.as_ref()))
        .collect();
    let ends: Series = ("ends", ends_chunks).try_into().unwrap();

    let search: Series = match array.dtype() {
        DType::UInt(IntWidth::_32) => [index as u32].iter().collect(),
        DType::UInt(IntWidth::_64) => [index as u64].iter().collect(),
        DType::Int(IntWidth::_32) => [index as i32].iter().collect(),
        DType::Int(IntWidth::_64) => [index as i64].iter().collect(),
        _ => panic!("Unsupported array type for run ends, array of either u32, u64, i32 or i64 type must be used, found {}", array.dtype()),
    };

    let maybe_run = search_sorted(&ends, &search, SearchSortedSide::Right, false)
        .unwrap()
        .get(0);

    maybe_run.map(|run| run as usize)
}

#[cfg(test)]
mod test {
    use crate::array::primitive::PrimitiveArray;
    use crate::types::IntWidth;

    use super::*;

    #[test]
    fn new() {
        let arr = REEArray::new(
            PrimitiveArray::from_vec(vec![2, 5, 10]).into(),
            PrimitiveArray::from_vec(vec![1, 2, 3]).into(),
        );
        assert_eq!(arr.len(), 10);
        assert_eq!(arr.dtype(), DType::Int(IntWidth::_32));

        // 0, 1 => 1
        // 2, 3, 4 => 2
        // 5, 6, 7, 8, 9 => 3
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(1));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(5).unwrap().try_into(), Ok(3));
        assert_eq!(arr.scalar_at(9).unwrap().try_into(), Ok(3));
    }

    #[test]
    fn slice() {
        let arr = REEArray::new(
            PrimitiveArray::from_vec(vec![2, 5, 10]).into(),
            PrimitiveArray::from_vec(vec![1, 2, 3]).into(),
        )
        .slice(3, 5)
        .unwrap();
        assert_eq!(arr.dtype(), DType::Int(IntWidth::_32));

        assert_eq!(arr.len(), 5);
        assert_eq!(arr.scalar_at(0).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(1).unwrap().try_into(), Ok(2));
        assert_eq!(arr.scalar_at(2).unwrap().try_into(), Ok(3));
        assert_eq!(arr.scalar_at(3).unwrap().try_into(), Ok(3));
        assert_eq!(arr.scalar_at(4).unwrap().try_into(), Ok(3));
    }
}
