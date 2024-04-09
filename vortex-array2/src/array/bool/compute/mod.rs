use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, BooleanArray as ArrowBoolArray};
use arrow_buffer::buffer::BooleanBuffer;
use itertools::Itertools;
use vortex::scalar::{BoolScalar, Scalar};
use vortex_error::VortexResult;
use vortex_schema::Nullability;

use crate::array::bool::{BoolArray, BoolData};
use crate::arrow::wrappers::as_nulls;
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::flatten::{flatten_bool, FlattenFn, FlattenedData};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::search_sorted::Len;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::validity::ArrayValidity;
use crate::validity::Validity;
use crate::{Array, ArrayTrait, IntoArray, ToArrayData, WithArray};

mod take;

impl ArrayCompute for BoolArray<'_> {
    fn as_arrow(&self) -> Option<&dyn AsArrowArray> {
        Some(self)
    }

    fn as_contiguous(&self) -> Option<&dyn AsContiguousFn> {
        Some(self)
    }

    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl AsArrowArray for BoolArray<'_> {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        Ok(Arc::new(ArrowBoolArray::new(
            self.buffer().clone(),
            as_nulls(self.logical_validity())?,
        )))
    }
}

impl AsContiguousFn for BoolArray<'_> {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<Array> {
        let validity = Validity::from_iter(
            arrays
                .iter()
                .map(|a| a.with_array(|a| a.logical_validity())),
        );

        let mut bools = Vec::with_capacity(arrays.iter().map(|a| a.len()).sum());
        for buffer in arrays
            .iter()
            .map(|a| flatten_bool(a).map(|bool_data| bool_data.as_ref().buffer()))
        {
            bools.extend(buffer?.iter().collect_vec())
        }

        Ok(BoolData::try_new(BooleanBuffer::from(bools), validity)?.into_array())
    }
}

impl FlattenFn for BoolArray<'_> {
    fn flatten(&self) -> VortexResult<FlattenedData> {
        Ok(FlattenedData::Bool(BoolData::try_from(
            self.to_array_data(),
        )?))
    }
}

impl ScalarAtFn for BoolArray<'_> {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        Ok(BoolScalar::try_new(
            self.is_valid(index).then(|| self.buffer().value(index)),
            self.dtype().nullability(),
        )
        .unwrap()
        .into())
    }
}

impl FillForwardFn for BoolArray<'_> {
    fn fill_forward(&self) -> VortexResult<Array<'static>> {
        if self.dtype().nullability() == Nullability::NonNullable {
            return Ok(self.to_array_data().into_array());
        }

        let validity = self.validity().unwrap().to_bool_array();
        let bools = self.buffer();
        let mut last_value = false;
        let filled = bools
            .iter()
            .zip(validity.buffer().iter())
            .map(|(v, valid)| {
                if valid {
                    last_value = v;
                }
                last_value
            })
            .collect::<Vec<_>>();
        Ok(BoolArray::from(filled).to_array_data())
    }
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::compute;

    #[test]
    fn fill_forward() {
        let barr = BoolArray::from_iter(vec![None, Some(false), None, Some(true), None]);
        let filled = compute::fill::fill_forward(&barr).unwrap();
        let filled_bool = filled.as_bool();
        assert_eq!(
            filled_bool.buffer().iter().collect::<Vec<bool>>(),
            vec![false, false, false, true, true]
        );
        assert!(filled_bool.validity().is_none());
    }
}
