use std::any::Any;
use std::cmp::min;
use std::sync::{Arc, RwLock};
use std::vec::IntoIter;

use arrow::array::types::UInt64Type;
use arrow::array::{Array as ArrowArray, ArrayRef as ArrowArrayRef};
use arrow::array::{PrimitiveArray as ArrowPrimitiveArray, Scalar as ArrowScalar};
use arrow::datatypes::DataType;

use crate::array::formatter::{ArrayDisplay, ArrayFormatter};
use crate::array::stats::{Stats, StatsSet};
use crate::array::{Array, ArrayRef, ArrowIterator, Encoding, EncodingId, EncodingRef};
use crate::arrow::compute::repeat;
use crate::compute;
use crate::compute::search_sorted::SearchSortedSide;
use crate::error::EncResult;
use crate::scalar::Scalar;
use crate::types::DType;

#[derive(Debug, Clone)]
pub struct REEArray {
    ends: ArrayRef,
    values: ArrayRef,
    offset: usize,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl REEArray {
    pub fn new(ends: ArrayRef, values: ArrayRef) -> Self {
        let length = run_ends_logical_length(&ends);
        Self {
            ends,
            values,
            length,
            offset: 0,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
    }

    pub fn find_physical_index(&self, index: usize) -> EncResult<usize> {
        compute::search_sorted::search_sorted_usize(
            &self.ends,
            index + self.offset,
            SearchSortedSide::Right,
        )
    }

    #[inline]
    pub fn ends(&self) -> &dyn Array {
        self.ends.as_ref()
    }

    #[inline]
    pub fn values(&self) -> &dyn Array {
        self.values.as_ref()
    }
}

impl Array for REEArray {
    #[inline]
    fn as_any(&self) -> &dyn Any {
        self
    }

    #[inline]
    fn boxed(self) -> ArrayRef {
        Box::new(self)
    }

    #[inline]
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }

    #[inline]
    fn len(&self) -> usize {
        self.length
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.length == 0
    }

    #[inline]
    fn dtype(&self) -> &DType {
        self.values.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        self.values.scalar_at(self.find_physical_index(index)?)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let physical_offset = self.find_physical_index(0).unwrap();

        // TODO(robert): Do better? Compute? This is subtract with limiting
        let mut ends = Vec::<usize>::new();
        let mut left_to_skip = physical_offset;
        for c in self.ends.iter_arrow() {
            let cast_res =
                arrow::compute::kernels::cast::cast(c.as_ref(), &DataType::UInt64).unwrap();
            let casted = cast_res
                .as_any()
                .downcast_ref::<ArrowPrimitiveArray<UInt64Type>>()
                .unwrap();
            let limited: Vec<usize> = casted
                .values()
                .iter()
                .skip(min(casted.len(), left_to_skip))
                .map(|v| *v as usize)
                .map(|v| v - (self.offset))
                .map(|v| min(v, self.length))
                .take_while(|v| *v <= self.length)
                .collect();

            ends.extend(limited);
            left_to_skip -= min(casted.len(), left_to_skip);
        }

        Box::new(REEArrowIterator::new(
            ends.into_iter(),
            self.values.iter_arrow(),
        ))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        self.check_slice_bounds(start, stop)?;
        let slice_begin = self.find_physical_index(start).unwrap();
        let slice_end = self.find_physical_index(stop).unwrap();
        Ok(Self {
            ends: self.ends.slice(slice_begin, slice_end + 1).unwrap(),
            values: self.values.slice(slice_begin, slice_end + 1).unwrap(),
            offset: start,
            length: stop - start,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &REEEncoding
    }

    #[inline]
    // Values and ends have been sliced to the nearest run end value so the size in bytes is accurate
    fn nbytes(&self) -> usize {
        self.values.nbytes() + self.ends.nbytes()
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for REEArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

#[derive(Debug)]
pub struct REEEncoding;

pub const REE_ENCODING: EncodingId = EncodingId("enc.ree");

impl Encoding for REEEncoding {
    fn id(&self) -> &EncodingId {
        &REE_ENCODING
    }
}

impl ArrayDisplay for REEArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln("values:")?;
        f.indent(|indented| indented.array(self.values()))?;
        f.writeln("ends:")?;
        f.indent(|indented| indented.array(self.ends()))
    }
}

struct REEArrowIterator {
    ends: IntoIter<usize>,
    values: Box<ArrowIterator>,
    current_idx: usize,
    current_arrow_array: Option<ArrowArrayRef>,
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
    type Item = ArrowArrayRef;

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
                let new_scalar: ArrowScalar<ArrowArrayRef> =
                    ArrowScalar::new(carr.as_ref().slice(self.current_idx, 1));
                let repeat_count = n - self.last_end;
                self.current_idx += 1;
                self.last_end = n;
                repeat(&new_scalar, repeat_count)
            })
    }
}

/// Gets the logical end of ends array of run end encoding.
fn run_ends_logical_length<T: AsRef<dyn Array>>(ends: &T) -> usize {
    ends.as_ref()
        .scalar_at(ends.as_ref().len() - 1)
        .and_then(|end| end.try_into())
        .unwrap_or_else(|_| panic!("Couldn't convert ends to usize"))
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use arrow::array::cast::AsArray;
    use arrow::array::types::Int32Type;
    use itertools::Itertools;

    use crate::types::{IntWidth, Signedness};

    use super::*;

    #[test]
    fn new() {
        let arr = REEArray::new(vec![2, 5, 10].into(), vec![1, 2, 3].into());
        assert_eq!(arr.len(), 10);
        assert_eq!(arr.dtype(), &DType::Int(IntWidth::_32, Signedness::Signed));

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
        let arr = REEArray::new(vec![2, 5, 10].into(), vec![1, 2, 3].into())
            .slice(3, 8)
            .unwrap();
        assert_eq!(arr.dtype(), &DType::Int(IntWidth::_32, Signedness::Signed));
        assert_eq!(arr.len(), 5);

        arr.iter_arrow()
            .zip_eq([vec![2, 2], vec![3, 3, 3]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(from_iter.as_primitive::<Int32Type>().values().deref(), orig);
            });
    }

    #[test]
    fn iter_arrow() {
        let arr = REEArray::new(vec![2, 5, 10].into(), vec![1, 2, 3].into());
        arr.iter_arrow()
            .zip_eq([vec![1, 1], vec![2, 2, 2], vec![3, 3, 3, 3, 3]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(from_iter.as_primitive::<Int32Type>().values().deref(), orig);
            });
    }
}
