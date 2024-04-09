use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, BooleanArray as ArrowBoolArray};
use arrow_buffer::buffer::BooleanBuffer;
use itertools::Itertools;
use vortex::scalar::{BoolScalar, Scalar};
use vortex_error::VortexResult;
use vortex_schema::Nullability;

use crate::array::bool::{BoolArray, BoolData};
use crate::compute::as_arrow::AsArrowArray;
use crate::compute::as_contiguous::AsContiguousFn;
use crate::compute::fill::FillForwardFn;
use crate::compute::flatten::{flatten_bool, FlattenFn, FlattenedData};
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::validity::ArrayValidity;
use crate::validity::Validity;
use crate::{Array, ArrayTrait, IntoArray, ToArrayData};

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
            self.logical_validity().to_null_buffer()?,
        )))
    }
}

impl AsContiguousFn for BoolArray<'_> {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<Array> {
        let validity = if self.dtype().is_nullable() {
            Validity::from_iter(arrays.iter().map(|a| {
                let bool_data = BoolData::try_from(a.to_array_data()).unwrap();
                bool_data.as_ref().validity().to_static()
            }))
        } else {
            Validity::NonNullable
        };

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

        let validity = self.logical_validity().to_null_buffer()?.unwrap();
        let bools = self.buffer();
        let mut last_value = false;
        let filled = bools
            .iter()
            .zip(validity.inner().iter())
            .map(|(v, valid)| {
                if valid {
                    last_value = v;
                }
                last_value
            })
            .collect::<Vec<_>>();
        Ok(BoolData::from_vec(filled, Validity::NonNullable).into_array())
    }
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolData;
    use crate::validity::Validity;
    use crate::{compute, IntoArray, ToArrayData};

    #[test]
    fn fill_forward() {
        let barr =
            BoolData::from_iter(vec![None, Some(false), None, Some(true), None]).into_array();
        let filled =
            BoolData::try_from(compute::fill::fill_forward(&barr).unwrap().to_array_data())
                .unwrap();
        let filled_bool = filled.as_ref();
        assert_eq!(
            filled_bool.buffer().iter().collect::<Vec<bool>>(),
            vec![false, false, false, true, true]
        );
        assert_eq!(*filled_bool.validity(), Validity::NonNullable);
    }
}
