use std::any::Any;
use std::iter;
use std::sync::{Arc, RwLock};

use arrow::compute::interleave;

use enc::array::{
    check_index_bounds, check_slice_bounds, Array, ArrayRef, ArrowIterator, Encoding, EncodingId,
    EncodingRef,
};
use enc::arrow::CombineChunks;
use enc::compress::EncodingCompression;
use enc::compute::search_sorted::{search_sorted_usize, SearchSortedSide};
use enc::dtype::{DType, Nullability, Signedness};
use enc::error::{EncError, EncResult};
use enc::formatter::{ArrayDisplay, ArrayFormatter};
use enc::scalar::Scalar;
use enc::stats::{Stats, StatsSet};

#[derive(Debug, Clone)]
pub struct PatchedArray {
    data: ArrayRef,
    // Offset value for patch indicies as a result of slicing
    patch_indices_offset: usize,
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
        if !data.dtype().eq_ignore_nullability(patch_values.dtype()) {
            return Err(EncError::MismatchedTypes(
                data.dtype().clone(),
                patch_values.dtype().clone(),
            ));
        }
        if !data.dtype().is_nullable() && patch_values.dtype().is_nullable() {
            return Err(EncError::NullPatchValuesNotAllowed(data.dtype().clone()));
        }
        if !matches!(
            patch_indices.dtype(),
            DType::Int(_, Signedness::Unsigned, Nullability::NonNullable)
        ) {
            return Err(EncError::InvalidDType(patch_indices.dtype().clone()));
        }

        Ok(Self {
            data,
            patch_indices_offset: 0,
            patch_indices,
            patch_values,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn patch_indices_offset(&self) -> usize {
        self.patch_indices_offset
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
        self.data.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.data.is_empty()
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

        // Check whether `true_patch_index` exists in the patch index array
        // First, get the index of the patch index array that is the first index
        // greater than or equal to the true index
        let true_patch_index = index + self.patch_indices_offset;
        search_sorted_usize(
            self.patch_indices(),
            true_patch_index,
            SearchSortedSide::Left,
        )
        .and_then(|idx| {
            // If the value at this index is equal to the true index, then it exists in the patch index array
            // and we should return the value at the corresponding index in the patch values array
            self.patch_indices()
                .scalar_at(idx)
                .and_then(usize::try_from)
                .and_then(|patch_index| {
                    if patch_index == true_patch_index {
                        self.patch_values().scalar_at(idx)
                    } else {
                        // Otherwise, we should return the value at the corresponding index in the data array
                        self.data().scalar_at(index)
                    }
                })
                // In this case idx is out of bounds of patch_indices
                .or_else(|_| self.data.scalar_at(index))
        })
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        if self.patch_indices().is_empty() {
            return self.data.iter_arrow();
        }

        let mut indices: Vec<(usize, usize)> = vec![Default::default(); self.len()];
        let patch_indices = ScalarIterator::new(self.patch_indices())
            .map(|v| usize::try_from(v).unwrap() - self.patch_indices_offset)
            .filter(|i| i < &self.len())
            .enumerate();

        let mut current_offset: usize = 0;
        for (patch_index_index, patch_index) in patch_indices {
            indices.splice(
                current_offset..patch_index,
                iter::repeat(0).zip(current_offset..patch_index),
            );
            indices[patch_index] = (1, patch_index_index);
            current_offset = patch_index + 1;
        }

        if current_offset < self.len() {
            indices.splice(
                current_offset..self.len(),
                iter::repeat(0).zip(current_offset..self.len()),
            );
        }

        Box::new(iter::once(
            interleave(
                &[
                    &(self.data.iter_arrow().combine_chunks()),
                    &self.patch_values.iter_arrow().combine_chunks(),
                ],
                indices.as_ref(),
            )
            .unwrap(),
        ))
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        // Find the index of the first patch index that is greater than or equal to the offset of this array
        let patch_index_start_index =
            search_sorted_usize(self.patch_indices(), start, SearchSortedSide::Left)?;
        let patch_index_end_index =
            search_sorted_usize(self.patch_indices(), stop, SearchSortedSide::Right)?;

        Ok(PatchedArray {
            data: self.data.slice(start, stop)?,
            patch_indices_offset: self.patch_indices_offset + start,
            patch_indices: self
                .patch_indices
                .slice(patch_index_start_index, patch_index_end_index)?,
            patch_values: self
                .patch_values
                .slice(patch_index_start_index, patch_index_end_index)?,
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
        self.data.nbytes() + self.patch_indices.nbytes() + self.patch_values.nbytes()
    }
}

struct ScalarIterator<'a> {
    array: &'a dyn Array,
    index: usize,
}

impl<'a> ScalarIterator<'a> {
    pub fn new(array: &'a dyn Array) -> Self {
        Self { array, index: 0 }
    }
}

impl<'a> Iterator for ScalarIterator<'a> {
    type Item = Box<dyn Scalar>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.array.len() {
            None
        } else {
            let res = self.array.scalar_at(self.index).unwrap();
            self.index += 1;
            Some(res)
        }
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for PatchedArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for PatchedArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln(format!("offset: {}", self.patch_indices_offset()))?;
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

pub const PATCHED_ENCODING: EncodingId = EncodingId::new("enc.patched");

impl Encoding for PatchedEncoding {
    fn id(&self) -> &EncodingId {
        &PATCHED_ENCODING
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }
}

#[cfg(test)]
mod test {
    use std::ops::Deref;

    use arrow::array::AsArray;
    use arrow::datatypes::Int32Type;
    use itertools::Itertools;

    use enc::array::primitive::PrimitiveArray;
    use enc::array::{Array, ArrayRef};
    use enc::error::EncError;

    use super::*;

    fn patched_array() -> PatchedArray {
        // merged array: [0, 1, 100, 3, 4, 200, 6, 7, 300, 9]
        PatchedArray::new(
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9].into(),
            PrimitiveArray::from_vec(vec![2u32, 5, 8]).boxed(),
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
        let empty_patch_indices: PrimitiveArray = PrimitiveArray::from_vec(Vec::<u32>::new());
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
