use std::any::Any;
use std::cmp::min;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use arrow::array::ArrowPrimitiveType;
use arrow::array::{Array as ArrowArray, ArrayRef as ArrowArrayRef, AsArray};
use num_traits::AsPrimitive;

use codecz::ree::SupportsREE;
use enc::array::primitive::PrimitiveArray;
use enc::array::{
    check_index_bounds, check_slice_bounds, Array, ArrayKind, ArrayRef, ArrowIterator, Encoding,
    EncodingId, EncodingRef,
};
use enc::arrow::match_arrow_numeric_type;
use enc::compress::EncodingCompression;
use enc::compute;
use enc::compute::search_sorted::SearchSortedSide;
use enc::dtype::DType;
use enc::error::{EncError, EncResult};
use enc::formatter::{ArrayDisplay, ArrayFormatter};
use enc::ptype::NativePType;
use enc::scalar::Scalar;
use enc::stats::{Stats, StatsSet};

use crate::compress::ree_encode;

#[derive(Debug, Clone)]
pub struct REEArray {
    ends: ArrayRef,
    values: ArrayRef,
    offset: usize,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl REEArray {
    pub fn new(ends: ArrayRef, values: ArrayRef, length: usize) -> Self {
        // TODO(robert): This requires all array implement scalar_at, take length in constructor for now
        // let length = run_ends_logical_length(&ends);
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
            self.ends(),
            index + self.offset,
            SearchSortedSide::Right,
        )
    }

    pub fn encode(array: &dyn Array) -> EncResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => {
                let (ends, values) = ree_encode(p);
                Ok(REEArray::new(ends.boxed(), values.boxed(), array.len()).boxed())
            }
            _ => Err(EncError::InvalidEncoding(array.encoding().id().clone())),
        }
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
        check_index_bounds(self, index)?;
        self.values.scalar_at(self.find_physical_index(index)?)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        // TODO(robert): Plumb offset rewriting to zig to fuse with REE decompression
        let ends: Vec<u32> = self
            .ends
            .iter_arrow()
            .flat_map(|c| {
                match_arrow_numeric_type!(self.ends.dtype(), |$E| {
                    let ends = c.as_primitive::<$E>()
                        .values()
                        .iter()
                        .map(|v| AsPrimitive::<u32>::as_(*v))
                        .map(|v| v - self.offset as u32)
                        .map(|v| min(v, self.length as u32))
                        .take_while(|v| *v <= (self.length as u32))
                        .collect::<Vec<u32>>();
                    ends.into_iter()
                })
            })
            .collect();

        match_arrow_numeric_type!(self.values.dtype(), |$N| {
            Box::new(REEArrowIterator::<$N>::new(ends, self.values.iter_arrow()))
        })
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;
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

pub const REE_ENCODING: EncodingId = EncodingId::new("enc.ree");

impl Encoding for REEEncoding {
    fn id(&self) -> &EncodingId {
        &REE_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
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

pub struct REEArrowIterator<T: ArrowPrimitiveType>
where
    T::Native: NativePType + SupportsREE,
{
    ends: Vec<u32>,
    values: Box<ArrowIterator>,
    current_idx: usize,
    _marker: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> REEArrowIterator<T>
where
    T::Native: NativePType + SupportsREE,
{
    pub fn new(ends: Vec<u32>, values: Box<ArrowIterator>) -> Self {
        Self {
            ends,
            values,
            current_idx: 0,
            _marker: PhantomData,
        }
    }
}

impl<T: ArrowPrimitiveType> Iterator for REEArrowIterator<T>
where
    T::Native: NativePType + SupportsREE,
{
    type Item = ArrowArrayRef;

    fn next(&mut self) -> Option<Self::Item> {
        self.values.next().and_then(|vs| {
            let batch_ends = &self.ends[self.current_idx..self.current_idx + vs.len()];
            self.current_idx += vs.len();
            let decoded =
                codecz::ree::decode::<T::Native>(vs.as_primitive::<T>().values(), batch_ends)
                    .unwrap();
            // TODO(robert): Is there a better way to construct a primitive arrow array
            PrimitiveArray::from_vec_in(decoded).iter_arrow().next()
        })
    }
}

/// Gets the logical end of ends array of run end encoding.
// TODO(robert): Once we fix scalar at for all arrays use this function
#[allow(dead_code)]
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

    use enc::dtype::{IntWidth, Nullability, Signedness};

    use super::*;

    #[test]
    fn new() {
        let arr = REEArray::new(vec![2, 5, 10].into(), vec![1, 2, 3].into(), 10);
        assert_eq!(arr.len(), 10);
        assert_eq!(
            arr.dtype(),
            &DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable)
        );

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
        let arr = REEArray::new(vec![2, 5, 10].into(), vec![1, 2, 3].into(), 10)
            .slice(3, 8)
            .unwrap();
        assert_eq!(
            arr.dtype(),
            &DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable)
        );
        assert_eq!(arr.len(), 5);

        arr.iter_arrow()
            .zip_eq([vec![2, 2, 3, 3, 3]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(from_iter.as_primitive::<Int32Type>().values().deref(), orig);
            });
    }

    #[test]
    fn iter_arrow() {
        let arr = REEArray::new(vec![2, 5, 10].into(), vec![1, 2, 3].into(), 10);
        arr.iter_arrow()
            .zip_eq([vec![1, 1, 2, 2, 2, 3, 3, 3, 3, 3]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(from_iter.as_primitive::<Int32Type>().values().deref(), orig);
            });
    }
}
