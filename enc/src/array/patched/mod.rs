mod compress;
mod stats;

use std::any::Any;
use std::sync::{Arc, RwLock};
use std::usize;

use arrow::array::ArrayRef as ArrowArrayRef;
use arrow::compute::interleave;

use crate::array::{
    check_index_bounds, check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId,
    EncodingRef,
};
use crate::arrow::CombineChunks;
use crate::compress::ArrayCompression;
use crate::compute::search_sorted::{search_sorted_usize, SearchSortedSide};
use crate::dtype::DType;
use crate::error::{EncError, EncResult};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::Scalar;
use crate::stats::{Stats, StatsSet};

#[derive(Debug, Clone)]
pub struct PatchedArray {
    data: ArrayRef,
    // used internally to track the starting index of the array in case of slicing
    offset: usize,
    // used internally to track the length of the array in case of slicing
    length: usize,
    patch_indices: ArrayRef,
    patch_values: ArrayRef,
    stats: Arc<RwLock<StatsSet>>,
}

impl PatchedArray {
    pub fn new(data: ArrayRef, patch_indices: ArrayRef, patch_values: ArrayRef) -> Self {
        Self::try_new(data, patch_indices, patch_values).unwrap()
    }

    pub fn try_new(
        data: ArrayRef,
        patch_indices: ArrayRef,
        patch_values: ArrayRef,
    ) -> EncResult<Self> {
        if data.dtype() != patch_values.dtype() {
            return Err(EncError::MismatchedTypes(
                data.dtype().clone(),
                patch_values.dtype().clone(),
            ));
        }
        let length = data.len();
        // TODO(jjiang): check path_indices is an unsigned int array type
        Ok(Self {
            data,
            offset: 0,
            length,
            patch_indices,
            patch_values,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    #[inline]
    pub fn patch_values(&self) -> &dyn Array {
        self.patch_values.as_ref()
    }

    #[inline]
    pub fn patch_indices(&self) -> &dyn Array {
        self.patch_indices.as_ref()
    }

    #[inline]
    pub fn data(&self) -> &dyn Array {
        self.data.as_ref()
    }
}

impl Array for PatchedArray {
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
        self.data.dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        check_index_bounds(self, index)?;
        let true_index = index + self.offset;

        // Check whether `true_index` exists in the patch index array
        // First, get the index of the patch index array that is the first index
        // greater than or equal to the true index
        search_sorted_usize(&self.patch_indices, true_index, SearchSortedSide::Left)
            .and_then(|idx| {
                // If the value at this index is equal to the true index, then it exists in the patch index array
                // and we should return the value at the corresponding index in the patch values array
                let patch_index = self.patch_indices.scalar_at(idx)?;
                if usize::try_from(patch_index)? == true_index {
                    self.patch_values.scalar_at(idx)
                } else {
                    Err(EncError::MalformedPatches(idx))
                }
            })
            // Otherwise, we should return the value at the corresponding index in the data array
            .or_else(|_| self.data.scalar_at(true_index))
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        Box::new(std::iter::once(
            PatchedArrowIterator::new(self).get_array_ref(),
        ))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        Ok(PatchedArray {
            data: self.data.clone(),
            offset: self.offset + start,
            length: stop - start,
            patch_indices: self.patch_indices.clone(),
            patch_values: self.patch_values.clone(),
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &PatchedEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        // TODO(robert): Take into account offsets
        self.data.nbytes() + self.patch_indices.nbytes() + self.patch_values.nbytes()
    }

    fn compression(&self) -> Option<&dyn ArrayCompression> {
        Some(self)
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for PatchedArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for PatchedArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln(format!("offset: {}", self.offset()))?;
        f.writeln("patch indices:")?;
        f.indent(|indented| indented.array(self.patch_indices()))?;
        f.writeln("patches:")?;
        f.indent(|indented| indented.array(self.patch_values()))?;
        f.writeln("data:")?;
        f.indent(|indented| indented.array(self.data()))
    }
}

#[derive(Debug)]
pub struct PatchedEncoding;

pub const PATCHED_ENCODING: EncodingId = EncodingId("enc.patched");

impl Encoding for PatchedEncoding {
    fn id(&self) -> &EncodingId {
        &PATCHED_ENCODING
    }
}

struct PatchedArrowIterator {
    data: ArrayRef,
    length: usize,
    patch_indices: ArrayRef,
    patch_values: ArrayRef,
    // Used for sliced arrays to track the offset of the patch values array
    // When an array is sliced, the patch_values should be shifted by this offset
    patch_value_offset: usize,
}

impl PatchedArrowIterator {
    fn next_patch_index<T: AsRef<dyn Array>>(
        patch_indices: T,
        index: usize,
        array_starting_offset: usize,
    ) -> Option<usize> {
        if index < patch_indices.as_ref().len() {
            Some(
                usize::try_from(patch_indices.as_ref().scalar_at(index).ok()?).ok()?
                    - array_starting_offset,
            )
        } else {
            None
        }
    }

    fn new(array: &PatchedArray) -> Self {
        // Slice the data array to get the data that is relevant to this array
        // unwrap directly because the start and length are already checked in .slice()
        let data = array
            .data
            .slice(array.offset, array.offset + array.length)
            .unwrap();

        // Find the index of the first patch index that is greater than or equal to the offset of this array
        let patch_index_start_index =
            search_sorted_usize(&array.patch_indices, array.offset, SearchSortedSide::Left)
                .unwrap();

        // Slice the patch indices array to get the data that is relevant to this array
        let patch_indices: ArrayRef = array
            .patch_indices
            .slice(patch_index_start_index, array.patch_indices.len())
            .unwrap();

        // Slice the patch values array to get the data that is relevant to this array
        let patch_values = array
            .patch_values
            .slice(patch_index_start_index, array.patch_values.len())
            .unwrap();

        Self {
            data,
            length: array.length,
            patch_indices,
            patch_values,
            patch_value_offset: array.offset,
        }
    }

    fn get_array_ref(&mut self) -> ArrowArrayRef {
        let mut indices: Vec<(usize, usize)> = vec![Default::default(); self.length];

        let mut patch_indices_index: usize = 0;
        let mut next_patch_index: Option<usize> = None;
        if self.patch_indices.len() > 0 {
            next_patch_index = PatchedArrowIterator::next_patch_index(
                &self.patch_indices,
                0,
                self.patch_value_offset,
            );
        }

        for (i, index) in indices.iter_mut().enumerate().take(self.length) {
            if next_patch_index.is_some() && Some(i) == next_patch_index {
                *index = (1, patch_indices_index);
                patch_indices_index += 1;
                next_patch_index = PatchedArrowIterator::next_patch_index(
                    &self.patch_indices,
                    patch_indices_index,
                    self.patch_value_offset,
                );
            } else {
                *index = (0, i);
            }
        }

        // `self.data` and `self.patch_values` are guaranteed to have the same type through the constructor
        // `interleave` shouldn't return an error and it's safe to unwrap
        interleave(
            &[
                &(self.data.iter_arrow().combine_chunks()),
                &self.patch_values.iter_arrow().combine_chunks(),
            ],
            indices.as_ref(),
        )
        .unwrap()
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use arrow::array::AsArray;
    use arrow::datatypes::Int32Type;
    use itertools::Itertools;

    use crate::array::patched::PatchedArray;
    use crate::array::primitive::PrimitiveArray;
    use crate::array::{Array, ArrayRef};
    use crate::error::EncError;

    fn patched_array() -> PatchedArray {
        // merged array: [0, 1, 100, 3, 4, 200, 6, 7, 300, 9]
        PatchedArray::new(
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into(),
            PrimitiveArray::from_vec(vec![2, 5, 8]).boxed(),
            vec![100, 200, 300].into(),
        )
    }

    #[test]
    pub fn iter() {
        patched_array()
            .iter_arrow()
            .zip_eq([vec![0, 1, 100, 3, 4, 200, 6, 7, 300, 9]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(from_iter.as_primitive::<Int32Type>().values().deref(), orig);
            });
    }

    #[test]
    pub fn iter_no_patch() {
        let data_vec = vec![0, 1, 2, 3, 4, 4, 6, 7, 8, 9];
        let empty_patch_indices: PrimitiveArray = PrimitiveArray::from_vec(Vec::<i32>::new());
        let empty_path_values: ArrayRef = Vec::<i32>::new().into();
        PatchedArray::try_new(
            data_vec.clone().into(),
            empty_patch_indices.boxed(),
            empty_path_values.clone(),
        )
        .unwrap()
        .iter_arrow()
        .zip_eq([data_vec])
        .for_each(|(from_iter, orig)| {
            assert_eq!(from_iter.as_primitive::<Int32Type>().values().deref(), orig);
        });
    }

    #[test]
    pub fn iter_sliced() {
        patched_array()
            .slice(2, 7)
            .unwrap()
            .iter_arrow()
            .zip_eq([vec![100, 3, 4, 200, 6]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(from_iter.as_primitive::<Int32Type>().values().deref(), orig);
            });
    }

    #[test]
    pub fn iter_sliced_twice() {
        let sliced_once = patched_array().slice(1, 8).unwrap();

        sliced_once
            .iter_arrow()
            .zip_eq([vec![1, 100, 3, 4, 200, 6, 7]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(from_iter.as_primitive::<Int32Type>().values().deref(), orig);
            });

        sliced_once
            .slice(1, 6)
            .unwrap()
            .iter_arrow()
            .zip_eq([vec![100, 3, 4, 200, 6]])
            .for_each(|(from_iter, orig)| {
                assert_eq!(from_iter.as_primitive::<Int32Type>().values().deref(), orig);
            });
    }

    #[test]
    pub fn scalar_at() {
        assert_eq!(
            usize::try_from(patched_array().scalar_at(2).unwrap()).unwrap(),
            100
        );
        assert_eq!(
            usize::try_from(patched_array().scalar_at(3).unwrap()).unwrap(),
            3
        );
        assert_eq!(
            patched_array().scalar_at(10).err().unwrap(),
            EncError::OutOfBounds(10, 0, 10)
        );
    }

    #[test]
    pub fn scalar_at_sliced() {
        let sliced = patched_array().slice(2, 7).unwrap();
        assert_eq!(usize::try_from(sliced.scalar_at(0).unwrap()).unwrap(), 100);
        assert_eq!(usize::try_from(sliced.scalar_at(1).unwrap()).unwrap(), 3);
        assert_eq!(
            sliced.scalar_at(5).err().unwrap(),
            EncError::OutOfBounds(5, 0, 5)
        );
    }

    #[test]
    pub fn scalar_at_sliced_twice() {
        let sliced_once = patched_array().slice(1, 8).unwrap();
        assert_eq!(
            usize::try_from(sliced_once.scalar_at(1).unwrap()).unwrap(),
            100
        );
        assert_eq!(
            usize::try_from(sliced_once.scalar_at(6).unwrap()).unwrap(),
            7
        );
        assert_eq!(
            sliced_once.scalar_at(7).err().unwrap(),
            EncError::OutOfBounds(7, 0, 7)
        );

        let sliced_twice = sliced_once.slice(1, 6).unwrap();
        assert_eq!(
            usize::try_from(sliced_twice.scalar_at(4).unwrap()).unwrap(),
            6
        );
        assert_eq!(
            sliced_twice.scalar_at(5).err().unwrap(),
            EncError::OutOfBounds(5, 0, 5)
        );
    }
}
