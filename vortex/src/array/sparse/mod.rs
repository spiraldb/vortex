// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;
use std::iter;
use std::sync::{Arc, RwLock};

use arrow::array::{
    Array as ArrowArray, PrimitiveArray as ArrowPrimitiveArray, StructArray as ArrowStructArray,
};
use arrow::array::AsArray;
use arrow::datatypes::{Field, Fields, UInt64Type};
use itertools::Itertools;
use linkme::distributed_slice;
use num_traits::AsPrimitive;

use crate::array::{
    Array, ArrayRef, ArrowIterator, check_index_bounds, check_slice_bounds, Encoding, EncodingId,
    EncodingRef,
};
use crate::array::{ArrowArrayRef, ENCODINGS};
use crate::arrow::CombineChunks;
use crate::compress::EncodingCompression;
use crate::compute::search_sorted::{search_sorted_usize, SearchSortedSide};
use crate::dtype::{DType, Nullability, Signedness};
use crate::error::{VortexError, VortexResult};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::match_arrow_numeric_type;
use crate::scalar::{NullableScalar, Scalar};
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsSet};

mod compress;
mod serde;
mod stats;

#[derive(Debug, Clone)]
pub struct SparseArray {
    indices: ArrayRef,
    values: ArrayRef,
    // Offset value for patch indices as a result of slicing
    indices_offset: usize,
    len: usize,
    dtype: DType,
    stats: Arc<RwLock<StatsSet>>,
}

impl SparseArray {
    pub const ID: EncodingId = EncodingId::new("vortex.sparse");

    pub fn new(indices: ArrayRef, values: ArrayRef, len: usize) -> Self {
        Self::try_new(indices, values, len).unwrap()
    }

    pub fn try_new(indices: ArrayRef, values: ArrayRef, len: usize) -> VortexResult<Self> {
        Self::new_with_offset(indices, values, len, 0)
    }

    pub(crate) fn new_with_offset(
        indices: ArrayRef,
        values: ArrayRef,
        len: usize,
        indices_offset: usize,
    ) -> VortexResult<Self> {
        if !matches!(
            indices.dtype(),
            DType::Int(_, Signedness::Unsigned, Nullability::NonNullable)
        ) {
            return Err(VortexError::InvalidDType(indices.dtype().clone()));
        }

        // TODO(ngates): check that indices.max falls within the length

        let dtype = DType::Struct(
            vec![
                Arc::new("indices".to_string()),
                Arc::new("values".to_string()),
            ],
            vec![indices.dtype().clone(), values.dtype().clone()],
        );

        Ok(Self {
            indices,
            values,
            indices_offset,
            dtype,
            len,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    #[inline]
    pub fn indices_offset(&self) -> usize {
        self.indices_offset
    }

    #[inline]
    pub fn values(&self) -> &dyn Array {
        self.values.as_ref()
    }

    #[inline]
    pub fn indices(&self) -> &dyn Array {
        self.indices.as_ref()
    }

    /// Return indices as a vector of usize with the indices_offset applied.
    pub fn resolved_indices(&self) -> Vec<usize> {
        let mut indices = Vec::with_capacity(self.len());
        self.indices().iter_arrow().for_each(|c| {
            indices.extend(
                arrow::compute::cast(c.as_ref(), &arrow::datatypes::DataType::UInt64)
                    .unwrap()
                    .as_primitive::<UInt64Type>()
                    .values()
                    .into_iter()
                    .map(|v| (*v as usize) - self.indices_offset),
            )
        });
        indices
    }
}

impl Array for SparseArray {
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
        self.len
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>> {
        check_index_bounds(self, index)?;

        // Check whether `true_patch_index` exists in the patch index array
        // First, get the index of the patch index array that is the first index
        // greater than or equal to the true index
        let true_patch_index = index + self.indices_offset;
        search_sorted_usize(self.indices(), true_patch_index, SearchSortedSide::Left).and_then(
            |idx| {
                // If the value at this index is equal to the true index, then it exists in the patch index array
                // and we should return the value at the corresponding index in the patch values array
                self.indices()
                    .scalar_at(idx)
                    .or_else(|_| Ok(NullableScalar::none(self.values().dtype().clone()).boxed()))
                    .and_then(usize::try_from)
                    .and_then(|patch_index| {
                        if patch_index == true_patch_index {
                            self.values().scalar_at(idx)
                        } else {
                            Ok(NullableScalar::none(self.values().dtype().clone()).boxed())
                        }
                    })
            },
        )
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        // TODO(robert): Use compute dispatch to perform subtract
        let indices_array = match_arrow_numeric_type!(self.indices().dtype(), |$E| {
            let indices: Vec<<$E as ArrowPrimitiveType>::Native> = self
                .indices()
                .iter_arrow()
                .flat_map(|c| {
                        let ends = c.as_primitive::<$E>()
                            .values()
                            .iter()
                            .map(|v| *v - AsPrimitive::<<$E as ArrowPrimitiveType>::Native>::as_(self.indices_offset))
                            .collect::<Vec<_>>();
                        ends.into_iter()
                })
                .collect();
            Arc::new(ArrowPrimitiveArray::<$E>::from(indices)) as ArrowArrayRef
        });

        let DType::Struct(names, children) = self.dtype() else {
            unreachable!("DType should have been a struct")
        };
        let fields: Fields = names
            .iter()
            .zip_eq(children)
            .map(|(name, dtype)| Field::new(name.as_str(), dtype.into(), dtype.is_nullable()))
            .map(Arc::new)
            .collect();
        Box::new(iter::once(Arc::new(ArrowStructArray::new(
            fields,
            vec![indices_array, self.values.iter_arrow().combine_chunks()],
            None,
        )) as Arc<dyn ArrowArray>))
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        // Find the index of the first patch index that is greater than or equal to the offset of this array
        let index_start_index = search_sorted_usize(self.indices(), start, SearchSortedSide::Left)?;
        let index_end_index = search_sorted_usize(self.indices(), stop, SearchSortedSide::Left)?;

        Ok(SparseArray {
            indices_offset: self.indices_offset + start,
            indices: self.indices.slice(index_start_index, index_end_index)?,
            values: self.values.slice(index_start_index, index_end_index)?,
            dtype: self.dtype.clone(),
            len: stop - start,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        }
        .boxed())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &SparseEncoding
    }

    #[inline]
    fn nbytes(&self) -> usize {
        self.indices.nbytes() + self.values.nbytes()
    }

    fn serde(&self) -> &dyn ArraySerde {
        self
    }
}

impl<'arr> AsRef<(dyn Array + 'arr)> for SparseArray {
    fn as_ref(&self) -> &(dyn Array + 'arr) {
        self
    }
}

impl ArrayDisplay for SparseArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.writeln(format!("offset: {}", self.indices_offset()))?;
        f.writeln("indices:")?;
        f.indent(|indented| indented.array(self.indices()))?;
        f.writeln("values:")?;
        f.indent(|indented| indented.array(self.values()))
    }
}

#[derive(Debug)]
pub struct SparseEncoding;

#[distributed_slice(ENCODINGS)]
static ENCODINGS_SPARSE: EncodingRef = &SparseEncoding;

impl Encoding for SparseEncoding {
    fn id(&self) -> &EncodingId {
        &SparseArray::ID
    }

    fn compression(&self) -> Option<&dyn EncodingCompression> {
        Some(self)
    }

    fn serde(&self) -> Option<&dyn EncodingSerde> {
        Some(self)
    }
}

#[cfg(test)]
mod test {
    use arrow::array::AsArray;
    use arrow::datatypes::{Int32Type, UInt32Type};

    use crate::array::Array;
    use crate::array::sparse::SparseArray;
    use crate::error::VortexError;

    fn sparse_array() -> SparseArray {
        // merged array: [null, null, 100, null, null, 200, null, null, 300, null]
        SparseArray::new(vec![2u32, 5, 8].into(), vec![100, 200, 300].into(), 10)
    }

    fn assert_sparse_array(sparse: &dyn Array, values: (&[u32], &[i32])) {
        let sparse_arrow = sparse.as_ref().iter_arrow().next().unwrap();
        assert_eq!(
            *sparse_arrow
                .as_struct()
                .column_by_name("indices")
                .unwrap()
                .as_primitive::<UInt32Type>()
                .values(),
            values.0
        );
        assert_eq!(
            *sparse_arrow
                .as_struct()
                .column_by_name("values")
                .unwrap()
                .as_primitive::<Int32Type>()
                .values(),
            values.1
        );
    }

    #[test]
    pub fn iter() {
        assert_sparse_array(
            sparse_array().as_ref(),
            (&[2u32, 5, 8], &[100i32, 200, 300]),
        );
    }

    #[test]
    pub fn iter_sliced() {
        assert_sparse_array(
            sparse_array().slice(2, 7).unwrap().as_ref(),
            (&[0u32, 3], &[100i32, 200]),
        );
    }

    #[test]
    pub fn iter_sliced_twice() {
        let sliced_once = sparse_array().slice(1, 8).unwrap();
        assert_sparse_array(sliced_once.as_ref(), (&[1u32, 4], &[100i32, 200]));
        assert_sparse_array(
            sliced_once.slice(1, 6).unwrap().as_ref(),
            (&[0u32, 3], &[100i32, 200]),
        );
    }

    #[test]
    pub fn scalar_at() {
        assert_eq!(
            usize::try_from(sparse_array().scalar_at(2).unwrap()).unwrap(),
            100
        );
        assert_eq!(
            sparse_array().scalar_at(10).err().unwrap(),
            VortexError::OutOfBounds(10, 0, 10)
        );
    }

    #[test]
    pub fn scalar_at_sliced() {
        let sliced = sparse_array().slice(2, 7).unwrap();
        assert_eq!(usize::try_from(sliced.scalar_at(0).unwrap()).unwrap(), 100);
        assert_eq!(
            sliced.scalar_at(5).err().unwrap(),
            VortexError::OutOfBounds(5, 0, 5)
        );
    }

    #[test]
    pub fn scalar_at_sliced_twice() {
        let sliced_once = sparse_array().slice(1, 8).unwrap();
        assert_eq!(
            usize::try_from(sliced_once.scalar_at(1).unwrap()).unwrap(),
            100
        );
        assert_eq!(
            sliced_once.scalar_at(7).err().unwrap(),
            VortexError::OutOfBounds(7, 0, 7)
        );

        let sliced_twice = sliced_once.slice(1, 6).unwrap();
        assert_eq!(
            usize::try_from(sliced_twice.scalar_at(3).unwrap()).unwrap(),
            200
        );
        assert_eq!(
            sliced_twice.scalar_at(5).err().unwrap(),
            VortexError::OutOfBounds(5, 0, 5)
        );
    }
}
