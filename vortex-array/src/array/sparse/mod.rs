use std::sync::{Arc, RwLock};

use itertools::Itertools;
use linkme::distributed_slice;
use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::DType;

use crate::array::constant::ConstantArray;
use crate::array::{check_slice_bounds, Array, ArrayRef};
use crate::compress::EncodingCompression;
use crate::compute::flatten::flatten_primitive;
use crate::compute::scalar_at::scalar_at;
use crate::compute::search_sorted::{search_sorted, SearchSortedSide};
use crate::compute::ArrayCompute;
use crate::encoding::{Encoding, EncodingId, EncodingRef, ENCODINGS};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::Scalar;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::{Stats, StatsCompute, StatsSet};
use crate::validity::ArrayValidity;
use crate::validity::Validity;
use crate::{impl_array, match_each_integer_ptype, ArrayWalker};

mod compress;
mod compute;
mod serde;

#[derive(Debug, Clone)]
pub struct SparseArray {
    indices: ArrayRef,
    values: ArrayRef,
    // Offset value for patch indices as a result of slicing
    indices_offset: usize,
    len: usize,
    stats: Arc<RwLock<StatsSet>>,
    fill_value: Scalar,
}

impl SparseArray {
    pub fn new(indices: ArrayRef, values: ArrayRef, len: usize, fill_value: Scalar) -> Self {
        Self::try_new(indices, values, len, fill_value).unwrap()
    }

    pub fn try_new(
        indices: ArrayRef,
        values: ArrayRef,
        len: usize,
        fill_value: Scalar,
    ) -> VortexResult<Self> {
        Self::try_new_with_offset(indices, values, len, 0, fill_value)
    }

    pub(crate) fn try_new_with_offset(
        indices: ArrayRef,
        values: ArrayRef,
        len: usize,
        indices_offset: usize,
        fill_value: Scalar,
    ) -> VortexResult<Self> {
        if !matches!(indices.dtype(), &DType::IDX) {
            vortex_bail!("Cannot use {} as indices", indices.dtype());
        }

        Ok(Self {
            indices,
            values,
            indices_offset,
            len,
            stats: Arc::new(RwLock::new(StatsSet::new())),
            fill_value,
        })
    }

    #[inline]
    pub fn indices_offset(&self) -> usize {
        self.indices_offset
    }

    #[inline]
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    #[inline]
    pub fn indices(&self) -> &ArrayRef {
        &self.indices
    }

    #[inline]
    fn fill_value(&self) -> &Scalar {
        &self.fill_value
    }

    /// Returns the position of a given index in the indices array if it exists.
    pub fn find_index(&self, index: usize) -> VortexResult<Option<usize>> {
        let true_index = self.indices_offset + index;

        // TODO(ngates): replace this with a binary search that tells us if we get an exact match.
        let idx = search_sorted(self.indices(), true_index, SearchSortedSide::Left)?;

        // If the value at this index is equal to the true index, then it exists in the
        // indices array.
        let patch_index: usize = scalar_at(self.indices(), idx)?.try_into()?;
        if true_index == patch_index {
            Ok(Some(idx))
        } else {
            Ok(None)
        }
    }

    /// Return indices as a vector of usize with the indices_offset applied.
    pub fn resolved_indices(&self) -> Vec<usize> {
        let flat_indices = flatten_primitive(self.indices()).unwrap();
        match_each_integer_ptype!(flat_indices.ptype(), |$P| {
            flat_indices
                .typed_data::<$P>()
                .iter()
                .map(|v| (*v as usize) - self.indices_offset)
                .collect_vec()
        })
    }
}

impl Array for SparseArray {
    impl_array!();

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
        self.values().dtype()
    }

    #[inline]
    fn stats(&self) -> Stats {
        Stats::new(&self.stats, self)
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        check_slice_bounds(self, start, stop)?;

        // Find the index of the first patch index that is greater than or equal to the offset of this array
        let index_start_index = search_sorted(self.indices(), start, SearchSortedSide::Left)?;
        let index_end_index = search_sorted(self.indices(), stop, SearchSortedSide::Left)?;

        Ok(SparseArray {
            indices_offset: self.indices_offset + start,
            indices: self.indices.slice(index_start_index, index_end_index)?,
            values: self.values.slice(index_start_index, index_end_index)?,
            len: stop - start,
            stats: Arc::new(RwLock::new(StatsSet::new())),
            fill_value: self.fill_value.clone(),
        }
        .into_array())
    }

    #[inline]
    fn encoding(&self) -> EncodingRef {
        &SparseEncoding
    }

    fn nbytes(&self) -> usize {
        self.indices.nbytes() + self.values.nbytes()
    }

    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.indices())?;
        walker.visit_child(self.values())
    }
}

impl StatsCompute for SparseArray {}

impl ArrayValidity for SparseArray {
    fn logical_validity(&self) -> Validity {
        let validity = if self.fill_value().is_null() {
            // If we have a null fill value, then the result is a Sparse array with a fill_value
            // of true, and patch values of false.
            SparseArray::try_new_with_offset(
                self.indices.clone(),
                ConstantArray::new(false, self.len()).into_array(),
                self.len(),
                self.indices_offset,
                true.into(),
            )
        } else {
            // If the fill_value is non-null, then the validity is based on the validity of the
            // existing values.
            SparseArray::try_new_with_offset(
                self.indices.clone(),
                self.values()
                    .logical_validity()
                    .to_bool_array()
                    .into_array(),
                self.len(),
                self.indices_offset,
                true.into(),
            )
        }
        .unwrap();

        Validity::Array(validity.into_array())
    }

    fn is_valid(&self, index: usize) -> bool {
        match self.find_index(index).unwrap() {
            None => !self.fill_value().is_null(),
            Some(idx) => self.values().is_valid(idx),
        }
    }
}

impl ArrayDisplay for SparseArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.property("offset", self.indices_offset())?;
        f.child("indices", self.indices())?;
        f.child("values", self.values())
    }
}

#[derive(Debug)]
pub struct SparseEncoding;

impl SparseEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.sparse");
}

#[distributed_slice(ENCODINGS)]
static ENCODINGS_SPARSE: EncodingRef = &SparseEncoding;

impl Encoding for SparseEncoding {
    fn id(&self) -> EncodingId {
        Self::ID
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
    use itertools::Itertools;
    use vortex_error::VortexError;
    use vortex_schema::Nullability::Nullable;
    use vortex_schema::Signedness::Signed;
    use vortex_schema::{DType, IntWidth};

    use crate::array::sparse::SparseArray;
    use crate::array::Array;
    use crate::array::IntoArray;
    use crate::compute::flatten::flatten_primitive;
    use crate::compute::scalar_at::scalar_at;
    use crate::scalar::Scalar;

    fn nullable_fill() -> Scalar {
        Scalar::null(&DType::Int(IntWidth::_32, Signed, Nullable))
    }
    fn non_nullable_fill() -> Scalar {
        Scalar::from(42i32)
    }

    fn sparse_array(fill_value: Scalar) -> SparseArray {
        // merged array: [null, null, 100, null, null, 200, null, null, 300, null]
        SparseArray::new(
            vec![2u64, 5, 8].into_array(),
            vec![100i32, 200, 300].into_array(),
            10,
            fill_value,
        )
    }

    fn assert_sparse_array(sparse: &dyn Array, values: &[Option<i32>]) {
        let sparse_arrow = flatten_primitive(sparse)
            .unwrap()
            .iter::<i32>()
            .collect_vec();
        assert_eq!(sparse_arrow, values);
    }

    #[test]
    pub fn iter() {
        assert_sparse_array(
            &sparse_array(nullable_fill()),
            &[
                None,
                None,
                Some(100),
                None,
                None,
                Some(200),
                None,
                None,
                Some(300),
                None,
            ],
        );
    }

    #[test]
    pub fn iter_sliced() {
        let p_fill_val = Some(non_nullable_fill().try_into().unwrap());
        assert_sparse_array(
            sparse_array(non_nullable_fill())
                .slice(2, 7)
                .unwrap()
                .as_ref(),
            &[Some(100), p_fill_val, p_fill_val, Some(200), p_fill_val],
        );
    }

    #[test]
    pub fn iter_sliced_nullable() {
        assert_sparse_array(
            sparse_array(nullable_fill()).slice(2, 7).unwrap().as_ref(),
            &[Some(100), None, None, Some(200), None],
        );
    }

    #[test]
    pub fn iter_sliced_twice() {
        let sliced_once = sparse_array(nullable_fill()).slice(1, 8).unwrap();
        assert_sparse_array(
            sliced_once.as_ref(),
            &[None, Some(100), None, None, Some(200), None, None],
        );
        assert_sparse_array(
            sliced_once.slice(1, 6).unwrap().as_ref(),
            &[Some(100), None, None, Some(200), None],
        );
    }

    #[test]
    pub fn test_find_index() {
        let sparse = sparse_array(nullable_fill());
        assert_eq!(sparse.find_index(0).unwrap(), None);
        assert_eq!(sparse.find_index(2).unwrap(), Some(0));
        assert_eq!(sparse.find_index(5).unwrap(), Some(1));
    }

    #[test]
    pub fn test_scalar_at() {
        assert_eq!(
            usize::try_from(scalar_at(&sparse_array(nullable_fill()), 2).unwrap()).unwrap(),
            100
        );
        let error = scalar_at(&sparse_array(nullable_fill()), 10).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error else {
            unreachable!()
        };
        assert_eq!(i, 10);
        assert_eq!(start, 0);
        assert_eq!(stop, 10);
    }

    #[test]
    pub fn scalar_at_sliced() {
        let sliced = sparse_array(nullable_fill()).slice(2, 7).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(sliced.as_ref(), 0).unwrap()).unwrap(),
            100
        );
        let error = scalar_at(sliced.as_ref(), 5).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error else {
            unreachable!()
        };
        assert_eq!(i, 5);
        assert_eq!(start, 0);
        assert_eq!(stop, 5);
    }

    #[test]
    pub fn scalar_at_sliced_twice() {
        let sliced_once = sparse_array(nullable_fill()).slice(1, 8).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(sliced_once.as_ref(), 1).unwrap()).unwrap(),
            100
        );
        let error = scalar_at(sliced_once.as_ref(), 7).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error else {
            unreachable!()
        };
        assert_eq!(i, 7);
        assert_eq!(start, 0);
        assert_eq!(stop, 7);

        let sliced_twice = sliced_once.slice(1, 6).unwrap();
        assert_eq!(
            usize::try_from(scalar_at(sliced_twice.as_ref(), 3).unwrap()).unwrap(),
            200
        );
        let error2 = scalar_at(sliced_twice.as_ref(), 5).err().unwrap();
        let VortexError::OutOfBounds(i, start, stop, _) = error2 else {
            unreachable!()
        };
        assert_eq!(i, 5);
        assert_eq!(start, 0);
        assert_eq!(stop, 5);
    }
}
