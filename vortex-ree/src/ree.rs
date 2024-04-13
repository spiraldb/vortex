use std::sync::{Arc, RwLock};

use vortex::array::{Array, ArrayKind, ArrayRef};
use vortex::compress::EncodingCompression;
use vortex::compute::scalar_at::scalar_at;
use vortex::compute::search_sorted::{search_sorted, SearchSortedSide};
use vortex::compute::ArrayCompute;
use vortex::encoding::{Encoding, EncodingId, EncodingRef};
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::serde::{ArraySerde, EncodingSerde};
use vortex::stats::{ArrayStatistics, OwnedStats, Stat, Statistics, StatsCompute, StatsSet};
use vortex::validity::Validity;
use vortex::validity::{OwnedValidity, ValidityView};
use vortex::view::{AsView, ToOwnedView};
use vortex::{impl_array, ArrayWalker};
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::DType;

use crate::compress::ree_encode;

#[derive(Debug, Clone)]
pub struct REEArray {
    ends: ArrayRef,
    values: ArrayRef,
    validity: Option<Validity>,
    offset: usize,
    length: usize,
    stats: Arc<RwLock<StatsSet>>,
}

impl REEArray {
    pub fn new(ends: ArrayRef, values: ArrayRef, validity: Option<Validity>) -> Self {
        Self::try_new(ends, values, validity).unwrap()
    }

    pub fn try_new(
        ends: ArrayRef,
        values: ArrayRef,
        validity: Option<Validity>,
    ) -> VortexResult<Self> {
        let length: usize = scalar_at(&ends, ends.len() - 1)?.try_into()?;
        Self::with_offset_and_size(ends, values, validity, length, 0)
    }

    pub(crate) fn with_offset_and_size(
        ends: ArrayRef,
        values: ArrayRef,
        validity: Option<Validity>,
        length: usize,
        offset: usize,
    ) -> VortexResult<Self> {
        if let Some(v) = &validity {
            assert_eq!(v.len(), length);
        }

        if !ends
            .statistics()
            .get_as(Stat::IsStrictSorted)
            .unwrap_or(true)
        {
            vortex_bail!("Ends array must be strictly sorted",);
        }

        Ok(Self {
            ends,
            values,
            validity,
            length,
            offset,
            stats: Arc::new(RwLock::new(StatsSet::new())),
        })
    }

    pub fn find_physical_index(&self, index: usize) -> VortexResult<usize> {
        search_sorted(self.ends(), index + self.offset, SearchSortedSide::Right)
            .map(|s| s.to_index())
    }

    pub fn encode(array: &dyn Array) -> VortexResult<ArrayRef> {
        match ArrayKind::from(array) {
            ArrayKind::Primitive(p) => {
                let (ends, values) = ree_encode(p);
                Ok(REEArray::new(
                    ends.into_array(),
                    values.into_array(),
                    p.validity().to_owned_view(),
                )
                .into_array())
            }
            _ => Err(vortex_err!("REE can only encode primitive arrays")),
        }
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.offset
    }

    #[inline]
    pub fn ends(&self) -> &ArrayRef {
        &self.ends
    }

    #[inline]
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }
}

impl Array for REEArray {
    impl_array!();
    #[inline]
    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        f(self)
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
    fn encoding(&self) -> EncodingRef {
        &REEEncoding
    }

    #[inline]
    // Values and ends have been sliced to the nearest run end value so the size in bytes is accurate
    fn nbytes(&self) -> usize {
        self.values.nbytes() + self.ends.nbytes()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        walker.visit_child(self.values())?;
        walker.visit_child(self.ends())
    }
}

impl OwnedValidity for REEArray {
    fn validity(&self) -> Option<ValidityView> {
        self.validity.as_view()
    }
}

impl StatsCompute for REEArray {
    fn compute(&self, _stat: Stat) -> VortexResult<StatsSet> {
        Ok(StatsSet::new())
    }
}

#[derive(Debug)]
pub struct REEEncoding;

impl REEEncoding {
    pub const ID: EncodingId = EncodingId::new("vortex.ree");
}

impl Encoding for REEEncoding {
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

impl ArrayDisplay for REEArray {
    fn fmt(&self, f: &mut ArrayFormatter) -> std::fmt::Result {
        f.child("values", self.values())?;
        f.child("ends", self.ends())
    }
}

impl OwnedStats for REEArray {
    fn stats_set(&self) -> &RwLock<StatsSet> {
        &self.stats
    }
}

impl ArrayStatistics for REEArray {
    fn statistics(&self) -> &dyn Statistics {
        self
    }
}

#[cfg(test)]
mod test {
    use vortex::array::Array;
    use vortex::array::IntoArray;
    use vortex::compute::flatten::flatten_primitive;
    use vortex::compute::scalar_at::scalar_at;
    use vortex::compute::slice::slice;
    use vortex_schema::{DType, IntWidth, Nullability, Signedness};

    use crate::REEArray;

    #[test]
    fn new() {
        let arr = REEArray::new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            None,
        );
        assert_eq!(arr.len(), 10);
        assert_eq!(
            arr.dtype(),
            &DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable)
        );

        // 0, 1 => 1
        // 2, 3, 4 => 2
        // 5, 6, 7, 8, 9 => 3
        assert_eq!(scalar_at(&arr, 0).unwrap(), 1.into());
        assert_eq!(scalar_at(&arr, 2).unwrap(), 2.into());
        assert_eq!(scalar_at(&arr, 5).unwrap(), 3.into());
        assert_eq!(scalar_at(&arr, 9).unwrap(), 3.into());
    }

    #[test]
    fn slice_array() {
        let arr = slice(
            &REEArray::new(
                vec![2u32, 5, 10].into_array(),
                vec![1i32, 2, 3].into_array(),
                None,
            ),
            3,
            8,
        )
        .unwrap();
        assert_eq!(
            arr.dtype(),
            &DType::Int(IntWidth::_32, Signedness::Signed, Nullability::NonNullable)
        );
        assert_eq!(arr.len(), 5);

        assert_eq!(
            flatten_primitive(&arr).unwrap().typed_data::<i32>(),
            vec![2, 2, 3, 3, 3]
        );
    }

    #[test]
    fn flatten() {
        let arr = REEArray::new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            None,
        );
        assert_eq!(
            flatten_primitive(&arr).unwrap().typed_data::<i32>(),
            vec![1, 1, 2, 2, 2, 3, 3, 3, 3, 3]
        );
    }
}
