use serde::{Deserialize, Serialize};
use vortex::compute::unary::scalar_at;
use vortex::compute::{search_sorted, SearchSortedSide};
use vortex::stats::{ArrayStatistics, ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::variants::{ArrayVariants, BoolArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoArrayVariant,
    IntoCanonical,
};
use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_bail, VortexExpect as _, VortexResult};

use crate::compress::runend_bool_decode;

impl_encoding!("vortex.runendbool", 23u16, RunEndBool);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEndBoolMetadata {
    start: bool,
    validity: ValidityMetadata,
    ends_dtype: DType,
    num_runs: usize,
    offset: usize,
    length: usize,
}

impl RunEndBoolArray {
    pub fn try_new(ends: Array, start: bool, validity: Validity) -> VortexResult<Self> {
        let length: usize = scalar_at(&ends, ends.len() - 1)?.as_ref().try_into()?;
        Self::with_offset_and_size(ends, start, validity, length, 0)
    }

    pub(crate) fn with_offset_and_size(
        ends: Array,
        start: bool,
        validity: Validity,
        length: usize,
        offset: usize,
    ) -> VortexResult<Self> {
        if !ends.statistics().compute_is_strict_sorted().unwrap_or(true) {
            vortex_bail!("Ends array must be strictly sorted",);
        }
        let metadata = RunEndBoolMetadata {
            start,
            validity: validity.to_metadata(length)?,
            ends_dtype: ends.dtype().clone(),
            num_runs: ends.len(),
            offset,
            length,
        };

        let mut children = Vec::with_capacity(2);
        children.push(ends);
        if let Some(a) = validity.into_array() {
            children.push(a)
        }

        Self::try_from_parts(
            DType::Bool(Nullability::NonNullable),
            length,
            metadata,
            children.into(),
            StatsSet::new(),
        )
    }

    pub fn find_physical_index(&self, index: usize) -> VortexResult<usize> {
        search_sorted(&self.ends(), index + self.offset(), SearchSortedSide::Right)
            .map(|s| s.to_ends_index(self.ends().len()))
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(2, &Validity::DTYPE, self.len()))
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.metadata().offset
    }

    #[inline]
    pub fn start(&self) -> bool {
        self.metadata().start
    }

    #[inline]
    pub fn ends(&self) -> Array {
        self.array()
            .child(0, &self.metadata().ends_dtype, self.metadata().num_runs)
            .vortex_expect("RunEndBoolArray is missing its run ends")
    }
}

impl BoolArrayTrait for RunEndBoolArray {
    fn maybe_null_indices_iter<'a>(&'a self) -> Box<dyn Iterator<Item = usize> + 'a> {
        todo!()
    }

    fn maybe_null_slices_iter<'a>(&'a self) -> Box<dyn Iterator<Item = (usize, usize)> + 'a> {
        todo!()
    }
}

impl ArrayVariants for RunEndBoolArray {
    fn as_bool_array(&self) -> Option<&dyn BoolArrayTrait> {
        Some(self)
    }
}

impl ArrayTrait for RunEndBoolArray {}

impl ArrayValidity for RunEndBoolArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl IntoCanonical for RunEndBoolArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let pends = self.ends().into_primitive()?;
        runend_bool_decode(
            &pends,
            self.start(),
            self.validity(),
            self.offset(),
            self.len(),
        )
        .map(Canonical::Bool)
    }
}

impl AcceptArrayVisitor for RunEndBoolArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("ends", &self.ends())?;
        visitor.visit_validity(&self.validity())
    }
}

impl ArrayStatisticsCompute for RunEndBoolArray {}

#[cfg(test)]
mod test {
    use vortex::array::BoolArray;
    use vortex::compute::unary::scalar_at;
    use vortex::compute::{slice, take};
    use vortex::validity::Validity;
    use vortex::{Array, ArrayDType, IntoArray, IntoCanonical, ToArray};
    use vortex_dtype::{DType, Nullability};

    use crate::RunEndBoolArray;

    #[test]
    fn new() {
        // [false, false, true, true, false]
        let arr =
            RunEndBoolArray::try_new(vec![2u32, 4, 5].into_array(), false, Validity::NonNullable)
                .unwrap();
        assert_eq!(arr.len(), 5);
        assert_eq!(arr.dtype(), &DType::Bool(Nullability::NonNullable));

        assert_eq!(scalar_at(arr.array(), 0).unwrap(), false.into());
        assert_eq!(scalar_at(arr.array(), 2).unwrap(), true.into());
        assert_eq!(scalar_at(arr.array(), 4).unwrap(), false.into());
    }

    #[test]
    fn slice_array() {
        let arr = slice(
            // [t, t, f, f, f, t, f, t, t, t]
            RunEndBoolArray::try_new(
                vec![2i32, 5, 6, 7, 10].into_array(),
                true,
                Validity::NonNullable,
            )
            .unwrap()
            .array(),
            2,
            8,
        )
        .unwrap();
        assert_eq!(arr.dtype(), &DType::Bool(Nullability::NonNullable));

        assert_eq!(
            to_bool_vec(&arr),
            vec![false, false, false, true, false, true],
        );
    }

    #[test]
    fn slice_slice_array() {
        let raw = BoolArray::from(vec![
            true, true, false, false, false, true, false, true, true, true,
        ])
        .to_array();
        let arr = slice(&raw, 2, 8).unwrap();
        assert_eq!(arr.dtype(), &DType::Bool(Nullability::NonNullable));

        assert_eq!(
            to_bool_vec(&arr),
            vec![false, false, false, true, false, true],
        );

        let arr2 = slice(&arr, 3, 6).unwrap();
        assert_eq!(to_bool_vec(&arr2), vec![true, false, true],);

        let arr3 = slice(&arr2, 1, 3).unwrap();
        assert_eq!(to_bool_vec(&arr3), vec![false, true],);
    }

    #[test]
    fn flatten() {
        let arr =
            RunEndBoolArray::try_new(vec![2u32, 4, 5].into_array(), true, Validity::NonNullable)
                .unwrap();

        assert_eq!(
            to_bool_vec(&arr.to_array()),
            vec![true, true, false, false, true]
        );
    }

    #[test]
    fn take_bool() {
        let arr = take(
            &RunEndBoolArray::try_new(
                vec![2u32, 4, 5, 10].into_array(),
                true,
                Validity::NonNullable,
            )
            .unwrap()
            .to_array(),
            &vec![0, 0, 6, 4].into_array(),
        )
        .unwrap();

        assert_eq!(to_bool_vec(&arr), vec![true, true, false, true]);
    }

    fn to_bool_vec(arr: &Array) -> Vec<bool> {
        arr.clone()
            .into_canonical()
            .unwrap()
            .into_bool()
            .unwrap()
            .boolean_buffer()
            .iter()
            .collect::<Vec<_>>()
    }
}
