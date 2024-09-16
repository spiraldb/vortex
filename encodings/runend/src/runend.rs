use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use vortex::array::PrimitiveArray;
use vortex::compute::unary::scalar_at;
use vortex::compute::{search_sorted, SearchSortedSide};
use vortex::stats::{ArrayStatistics, ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoArray, IntoArrayVariant,
    IntoCanonical,
};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexExpect as _, VortexResult};

use crate::compress::{runend_decode, runend_encode};

impl_encoding!("vortex.runend", 19u16, RunEnd);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunEndMetadata {
    validity: ValidityMetadata,
    ends_dtype: DType,
    num_runs: usize,
    offset: usize,
    length: usize,
}

impl RunEndArray {
    pub fn try_new(ends: Array, values: Array, validity: Validity) -> VortexResult<Self> {
        let length: usize = scalar_at(&ends, ends.len() - 1)?.as_ref().try_into()?;
        Self::with_offset_and_size(ends, values, validity, length, 0)
    }

    pub(crate) fn with_offset_and_size(
        ends: Array,
        values: Array,
        validity: Validity,
        length: usize,
        offset: usize,
    ) -> VortexResult<Self> {
        if values.dtype().is_nullable() == (validity == Validity::NonNullable) {
            vortex_bail!(
                "incorrect validity {:?} for dtype {}",
                validity,
                values.dtype()
            );
        }

        if offset != 0 && !ends.is_empty() {
            let first_run_end: usize = scalar_at(&ends, 0)?.as_ref().try_into()?;
            if first_run_end <= offset {
                vortex_bail!("First run end {first_run_end} must be bigger than offset {offset}");
            }
        }

        if !ends.statistics().compute_is_strict_sorted().unwrap_or(true) {
            vortex_bail!("Ends array must be strictly sorted",);
        }
        let dtype = values.dtype().clone();
        let metadata = RunEndMetadata {
            validity: validity.to_metadata(length)?,
            ends_dtype: ends.dtype().clone(),
            num_runs: ends.len(),
            offset,
            length,
        };

        let mut children = Vec::with_capacity(3);
        children.push(ends);
        children.push(values);
        if let Some(a) = validity.into_array() {
            children.push(a)
        }

        Self::try_from_parts(dtype, length, metadata, children.into(), StatsSet::new())
    }

    /// Run the array through run-end encoding.
    pub fn encode(array: Array) -> VortexResult<Self> {
        if let Ok(parray) = PrimitiveArray::try_from(array) {
            let (ends, values) = runend_encode(&parray);
            Self::try_new(ends.into_array(), values.into_array(), parray.validity())
        } else {
            vortex_bail!("REE can only encode primitive arrays")
        }
    }

    pub fn validity(&self) -> Validity {
        self.metadata()
            .validity
            .to_validity(self.array().child(2, &Validity::DTYPE, self.len()))
    }

    /// The offset that the `ends` is relative to.
    ///
    /// This is generally zero for a "new" array, and non-zero after a slicing operation.
    #[inline]
    pub fn offset(&self) -> usize {
        self.metadata().offset
    }

    /// The encoded "ends" of value runs.
    ///
    /// The `i`-th element indicates that there is a run of the same value, beginning
    /// at `ends[i]` (inclusive) and terminating at `ends[i+1]` (exclusive).
    #[inline]
    pub fn ends(&self) -> Array {
        self.array()
            .child(0, &self.metadata().ends_dtype, self.metadata().num_runs)
            .vortex_expect("RunEndArray is missing its run ends")
    }

    /// The scalar values.
    ///
    /// The `i`-th element is the scalar value for the `i`-th repeated run. The run begins
    /// at `ends[i]` (inclusive) and terminates at `ends[i+1]` (exclusive).
    #[inline]
    pub fn values(&self) -> Array {
        self.array()
            .child(1, self.dtype(), self.metadata().num_runs)
            .vortex_expect("RunEndArray is missing its values")
    }
}

/// Convert the given logical index to an index into the `values` array
pub(crate) fn find_physical_index(
    ends: &Array,
    index: usize,
    offset: usize,
) -> VortexResult<usize> {
    search_sorted(ends, index + offset, SearchSortedSide::Right)
        .map(|s| s.to_ends_index(ends.len()))
}

impl ArrayTrait for RunEndArray {}

impl ArrayVariants for RunEndArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

impl PrimitiveArrayTrait for RunEndArray {}

impl ArrayValidity for RunEndArray {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl IntoCanonical for RunEndArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        let pends = self.ends().into_primitive()?;
        let pvalues = self.values().into_primitive()?;
        runend_decode(&pends, &pvalues, self.validity(), self.offset(), self.len())
            .map(Canonical::Primitive)
    }
}

impl AcceptArrayVisitor for RunEndArray {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("ends", &self.ends())?;
        visitor.visit_child("values", &self.values())?;
        visitor.visit_validity(&self.validity())
    }
}

impl ArrayStatisticsCompute for RunEndArray {}

#[cfg(test)]
mod tests {
    use vortex::compute::unary::scalar_at;
    use vortex::validity::Validity;
    use vortex::{ArrayDType, IntoArray};
    use vortex_dtype::{DType, Nullability, PType};

    use crate::RunEndArray;

    #[test]
    fn new() {
        let arr = RunEndArray::try_new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            Validity::NonNullable,
        )
        .unwrap();
        assert_eq!(arr.len(), 10);
        assert_eq!(
            arr.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );

        // 0, 1 => 1
        // 2, 3, 4 => 2
        // 5, 6, 7, 8, 9 => 3
        assert_eq!(scalar_at(arr.array(), 0).unwrap(), 1.into());
        assert_eq!(scalar_at(arr.array(), 2).unwrap(), 2.into());
        assert_eq!(scalar_at(arr.array(), 5).unwrap(), 3.into());
        assert_eq!(scalar_at(arr.array(), 9).unwrap(), 3.into());
    }
}
