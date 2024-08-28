use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use vortex::array::{Primitive, PrimitiveArray};
use vortex::compute::unary::scalar_at;
use vortex::compute::{search_sorted, SearchSortedSide};
use vortex::iter::AccessorRef;
use vortex::stats::{ArrayStatistics, ArrayStatisticsCompute, StatsSet};
use vortex::validity::{ArrayValidity, LogicalValidity, Validity, ValidityMetadata};
use vortex::variants::{ArrayVariants, PrimitiveArrayTrait};
use vortex::visitor::{AcceptArrayVisitor, ArrayVisitor};
use vortex::{
    impl_encoding, Array, ArrayDType, ArrayDef, ArrayTrait, Canonical, IntoArray, IntoArrayVariant,
    IntoCanonical,
};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

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

    pub fn find_physical_index(&self, index: usize) -> VortexResult<usize> {
        let searched_index =
            search_sorted(&self.ends(), index + self.offset(), SearchSortedSide::Right)?.to_index();
        Ok(if searched_index == self.ends().len() {
            searched_index - 1
        } else {
            searched_index
        })
    }

    pub fn encode(array: Array) -> VortexResult<Self> {
        if array.encoding().id() == Primitive::ID {
            let primitive = PrimitiveArray::try_from(array)?;
            let (ends, values) = runend_encode(&primitive);
            Self::try_new(ends.into_array(), values.into_array(), primitive.validity())
        } else {
            vortex_bail!("REE can only encode primitive arrays")
        }
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
    pub fn ends(&self) -> Array {
        self.array()
            .child(0, &self.metadata().ends_dtype, self.metadata().num_runs)
            .expect("missing ends")
    }

    #[inline]
    pub fn values(&self) -> Array {
        self.array()
            .child(1, self.dtype(), self.metadata().num_runs)
            .expect("missing values")
    }
}

impl ArrayTrait for RunEndArray {}

impl ArrayVariants for RunEndArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }
}

impl PrimitiveArrayTrait for RunEndArray {
    fn u8_accessor(&self) -> Option<AccessorRef<u8>> {
        todo!()
    }

    fn u16_accessor(&self) -> Option<AccessorRef<u16>> {
        todo!()
    }
    fn u32_accessor(&self) -> Option<AccessorRef<u32>> {
        todo!()
    }

    fn u64_accessor(&self) -> Option<AccessorRef<u64>> {
        todo!()
    }

    fn f32_accessor(&self) -> Option<AccessorRef<f32>> {
        todo!()
    }

    fn f64_accessor(&self) -> Option<AccessorRef<f64>> {
        todo!()
    }

    fn i8_accessor(&self) -> Option<AccessorRef<i8>> {
        todo!()
    }

    fn i16_accessor(&self) -> Option<AccessorRef<i16>> {
        todo!()
    }

    fn i32_accessor(&self) -> Option<AccessorRef<i32>> {
        todo!()
    }

    fn i64_accessor(&self) -> Option<AccessorRef<i64>> {
        todo!()
    }
}

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
mod test {
    use vortex::compute::slice;
    use vortex::compute::unary::scalar_at;
    use vortex::validity::Validity;
    use vortex::{ArrayDType, IntoArray, IntoArrayVariant};
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

    #[test]
    fn slice_array() {
        let arr = slice(
            RunEndArray::try_new(
                vec![2u32, 5, 10].into_array(),
                vec![1i32, 2, 3].into_array(),
                Validity::NonNullable,
            )
            .unwrap()
            .array(),
            3,
            8,
        )
        .unwrap();
        assert_eq!(
            arr.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );
        assert_eq!(arr.len(), 5);

        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            vec![2, 2, 3, 3, 3]
        );
    }

    #[test]
    fn slice_end_inclusive() {
        let arr = slice(
            RunEndArray::try_new(
                vec![2u32, 5, 10].into_array(),
                vec![1i32, 2, 3].into_array(),
                Validity::NonNullable,
            )
            .unwrap()
            .array(),
            4,
            10,
        )
        .unwrap();
        assert_eq!(
            arr.dtype(),
            &DType::Primitive(PType::I32, Nullability::NonNullable)
        );
        assert_eq!(arr.len(), 6);

        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            vec![2, 3, 3, 3, 3, 3]
        );
    }

    #[test]
    fn flatten() {
        let arr = RunEndArray::try_new(
            vec![2u32, 5, 10].into_array(),
            vec![1i32, 2, 3].into_array(),
            Validity::NonNullable,
        )
        .unwrap();

        assert_eq!(
            arr.into_primitive().unwrap().maybe_null_slice::<i32>(),
            vec![1, 1, 2, 2, 2, 3, 3, 3, 3, 3]
        );
    }
}
