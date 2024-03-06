use std::any::Any;
use std::cmp::min;
use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use arrow::array::ArrowPrimitiveType;
use arrow::array::{Array as ArrowArray, ArrayRef as ArrowArrayRef, AsArray};
use num_traits::AsPrimitive;

use codecz::ree::SupportsREE;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{
    check_slice_bounds, check_validity_buffer, Array, ArrayKind, ArrayRef, ArrowIterator,
    CloneOptionalArray, Encoding, EncodingId, EncodingRef,
};
use vortex::arrow::match_arrow_numeric_type;
use vortex::compress::EncodingCompression;
use vortex::compute;
use vortex::compute::scalar_at::scalar_at;
use vortex::compute::search_sorted::SearchSortedSide;
use vortex::dtype::{DType, Nullability, Signedness};
use vortex::error::{VortexError, VortexResult};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::ptype::NativePType;
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{Stat, Stats, StatsCompute, StatsSet};

use crate::compress::ree_encode;

#[derive(Debug, Clone)]
pub struct REEArray {
    ends: ArrayRef,
    values: ArrayRef,
    validity: Option<ArrayRef>,
    offset: usize,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl REEArray {
    pub fn new(
        ends: ArrayRef,
        values: ArrayRef,
        validity: Option<ArrayRef>,
        length: usize,
    ) -> Self {
        Self::try_new(ends, values, validity, length).unwrap()
    }

    pub fn try_new(
        ends: ArrayRef,
        values: ArrayRef,
        validity: Option<ArrayRef>,
        length: usize,
    ) -> VortexResult<Self> {
        check_validity_buffer(validity.as_deref())?;

        if !matches!(
            ends.dtype(),
            DType::Int(_, Signedness::Unsigned, Nullability::NonNullable)
        ) {
            return Err(VortexError::InvalidDType(ends.dtype().clone()));
        }

        if !ends
            .stats()
            .get_as::<bool>(&Stat::IsStrictSorted)
            .unwrap_or(true)
        {
            return Err(VortexError::IndexArrayMustBeStrictSorted);
        }

        // see https://github.com/fulcrum-so/spiral/issues/873
        // let length = run_ends_logical_length(&ends);
        Ok(Self {
            ends,
            values,
            validity,
            length,
            offset: 0,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn find_physical_index(&self, index: usize) -> VortexResult<usize> {
        compute::search_sorted::search_sorted_usize(
            self.ends(),
            index + self.offset,
            SearchSortedSide::Right,
        )
    }

    pub fn encode(array: &dyn Array) -> VortexResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => {
                let (ends, values) = ree_encode(p);
                Ok(REEArray::new(
                    ends.boxed(),
                    values.boxed(),
                    p.validity().clone_optional(),
                    p.len(),
                )
                .boxed())
            }
            _ => Err(VortexError::InvalidEncoding(array.encoding().id().clone())),
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

    #[inline]
    pub fn validity(&self) -> Option<&dyn Array> {
        self.validity.as_deref()
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

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;
        let slice_begin = self.find_physical_index(start)?;
        let slice_end = self.find_physical_index(stop)?;
        Ok(Self {
            ends: self.ends.slice(slice_begin, slice_end + 1)?,
            values: self.values.slice(slice_begin, slice_end + 1)?,
            validity: self
                .validity
                .as_ref()
                .map(|v| v.slice(slice_begin, slice_end + 1))
                .transpose()?,
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

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl StatsCompute for REEArray {}

impl<'arr> AsRef<(dyn Array + 'arr)> for REEArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

#[derive(Debug)]
pub struct REEEncoding;

impl REEEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.ree");
}

impl Encoding for REEEncoding {
    fn id(&self) -> &EncodingId {
        &Self::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
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

/// Gets the logical end from the ends array.
#[allow(dead_code)]
fn run_ends_logical_length<T: AsRef<dyn Array>>(ends: &T) -> usize {
    if ends.as_ref().is_empty() {
        0
    } else {
        scalar_at(ends.as_ref(), ends.as_ref().len() - 1)
            .and_then(|end| end.try_into())
            .unwrap_or_else(|_| panic!("Couldn't convert ends to usize"))
    }
}

#[cfg(test)]
mod test {
    use arrow::array::cast::AsArray;
    use arrow::array::types::Int32Type;
    use itertools::Itertools;
    use vortex::array::Array;
    use vortex::compute::scalar_at::scalar_at;

    use crate::REEArray;
    use vortex::dtype::{DType, IntWidth, Nullability, Signedness};

    #[test]
    fn new() {
        let arr = REEArray::new(vec![2u32, 5, 10].into(), vec![1, 2, 3].into(), None, 10);
        assert_eq!(arr.len(), 10);
        assert_eq!(
            arr.dtype(),
            &DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable)
        );

        // 0, 1 => 1
        // 2, 3, 4 => 2
        // 5, 6, 7, 8, 9 => 3
        assert_eq!(scalar_at(arr.as_ref(), 0).unwrap().try_into(), Ok(1));
        assert_eq!(scalar_at(arr.as_ref(), 2).unwrap().try_into(), Ok(2));
        assert_eq!(scalar_at(arr.as_ref(), 5).unwrap().try_into(), Ok(3));
        assert_eq!(scalar_at(arr.as_ref(), 9).unwrap().try_into(), Ok(3));
    }

    #[test]
    fn slice() {
        let arr = REEArray::new(vec![2u32, 5, 10].into(), vec![1, 2, 3].into(), None, 10)
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
                assert_eq!(*from_iter.as_primitive::<Int32Type>().values(), orig);
            });
    }

    #[test]
    fn iter_arrow() {
        let arr = REEArray::new(vec![2u32, 5, 10].into(), vec![1, 2, 3].into(), None, 10);
        arr.iter_arrow()
            .zip_eq([vec![1, 1, 2, 2, 2, 3, 3, 3, 3, 3]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(*from_iter.as_primitive::<Int32Type>().values(), orig);
            });
    }
}
