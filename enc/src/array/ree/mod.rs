use std::cmp::min;
use std::vec::IntoIter;

use arrow2::array::Array as ArrowArray;
use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;
use arrow2::compute::cast::CastOptions;
use arrow2::datatypes::DataType;
use arrow2::scalar::new_scalar;

use crate::array::{Array, ArrayEncoding, ArrowIterator};
use crate::arrow::compat;
use crate::arrow::compute::repeat;
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::{DType, IntWidth};

#[derive(Debug, Clone, PartialEq)]
pub struct REEArray {
    ends: Box<Array>,
    values: Box<Array>,
    length: usize,
    offset: usize,
}

impl REEArray {
    pub fn new(ends: Array, values: Array) -> Self {
        let length = run_ends_logical_length(&ends);
        Self {
            ends: Box::new(ends),
            values: Box::new(values),
            length,
            offset: 0,
        }
    }

    pub fn find_physical_index(&self, index: usize) -> Option<usize> {
        find_physical_index(self.ends.as_ref(), index + self.offset)
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
            .ok_or(EncError::OutOfBounds(index, self.offset, self.length))
            .and_then(|run| self.values.scalar_at(run))
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let physical_offset = self.find_physical_index(0).unwrap();

        // TODO(robert): Do better? Compute? This is subtract with limiting
        let mut ends = Vec::<usize>::new();
        let mut left_to_skip = physical_offset;
        for c in self.ends.iter_arrow() {
            let cast_res =
                arrow2::compute::cast::cast(c.as_ref(), &DataType::UInt64, CastOptions::default())
                    .unwrap();
            let casted = cast_res
                .as_any()
                .downcast_ref::<ArrowPrimitiveArray<u64>>()
                .unwrap();
            casted
                .values()
                .iter()
                .skip(min(casted.len(), left_to_skip))
                .map(|v| *v as usize)
                .map(|v| v - (self.offset))
                .map(|v| min(v, self.length))
                .take_while(|v| v <= &self.length)
                .for_each(|v| ends.push(v));

            left_to_skip -= min(casted.len(), left_to_skip);
        }

        Box::new(REEArrowIterator::new(
            ends.into_iter(),
            self.values.iter_arrow(),
        ))
    }

    fn slice(&self, offset: usize, length: usize) -> EncResult<Array> {
        self.check_slice_bounds(offset, length)?;
        let slice_begin = self.find_physical_index(offset).unwrap();
        let slice_end = self.find_physical_index(offset + length).unwrap();
        Ok(Array::REE(Self {
            ends: Box::new(
                self.ends
                    .slice(slice_begin, slice_end - slice_begin + 1)
                    .unwrap(),
            ),
            values: Box::new(
                self.values
                    .slice(slice_begin, slice_end - slice_begin + 1)
                    .unwrap(),
            ),
            length,
            offset,
        }))
    }
}

struct REEArrowIterator {
    ends: IntoIter<usize>,
    values: Box<ArrowIterator>,
    current_idx: usize,
    current_arrow_array: Option<Box<dyn ArrowArray>>,
    last_end: usize,
}

impl REEArrowIterator {
    pub fn new(ends: IntoIter<usize>, values: Box<ArrowIterator>) -> Self {
        Self {
            ends,
            values,
            current_idx: 0,
            current_arrow_array: None,
            last_end: 0,
        }
    }
}

impl Iterator for REEArrowIterator {
    type Item = Box<dyn ArrowArray>;

    fn next(&mut self) -> Option<Self::Item> {
        if self
            .current_arrow_array
            .as_ref()
            .map(|c| c.len() == self.current_idx)
            .unwrap_or(true)
        {
            self.current_arrow_array = self.values.next();
        }

        self.current_arrow_array
            .as_ref()
            .zip(self.ends.next())
            .map(|(carr, n)| {
                let new_scalar = new_scalar(carr.as_ref(), self.current_idx);
                let repeat_count = n - self.last_end;
                self.current_idx += 1;
                self.last_end = n;
                repeat(new_scalar.as_ref(), repeat_count)
            })
    }
}

/// Gets the logical end of ends array of run end encoding.
fn run_ends_logical_length(ends: &Array) -> usize {
    ends.scalar_at(ends.len() - 1)
        .and_then(|end| end.try_into())
        .unwrap_or_else(|_| panic!("Couldn't convert ends to usize"))
}

fn find_physical_index(array: &Array, index: usize) -> Option<usize> {
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
    use itertools::Itertools;

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

        arr.iter_arrow()
            .zip_eq([vec![2, 2], vec![3, 3, 3]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<i32>>()
                        .unwrap()
                        .values()
                        .as_slice(),
                    orig
                );
            });
    }

    #[test]
    fn iter_arrow() {
        let arr = REEArray::new(
            PrimitiveArray::from_vec(vec![2, 5, 10]).into(),
            PrimitiveArray::from_vec(vec![1, 2, 3]).into(),
        );
        arr.iter_arrow()
            .zip_eq([vec![1, 1], vec![2, 2, 2], vec![3, 3, 3, 3, 3]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(
                    from_iter
                        .as_any()
                        .downcast_ref::<ArrowPrimitiveArray<i32>>()
                        .unwrap()
                        .values()
                        .as_slice(),
                    orig
                );
            });
    }
}
