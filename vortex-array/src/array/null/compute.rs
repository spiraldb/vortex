use vortex_dtype::{match_each_integer_ptype, DType};
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::array::null::NullArray;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::slice::SliceFn;
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::{Array, ArrayTrait, IntoArray};

impl ArrayCompute for NullArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }

    fn slice(&self) -> Option<&dyn SliceFn> {
        Some(self)
    }

    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl SliceFn for NullArray {
    fn slice(&self, start: usize, stop: usize) -> VortexResult<Array> {
        assert!(stop < self.len(), "cannot slice past end of the array");
        Ok(NullArray::new(stop - start).into_array())
    }
}

impl ScalarAtFn for NullArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        assert!(index < self.len(), "cannot index past end of the array");

        Ok(Scalar::null(DType::Null))
    }
}

impl TakeFn for NullArray {
    fn take(&self, indices: &Array) -> VortexResult<Array> {
        let indices = indices.clone().flatten_primitive()?;

        // Enforce all indices are valid
        match_each_integer_ptype!(indices.ptype(), |$T| {
            for index in indices.scalar_buffer::<$T>().iter() {
                assert!((*index as usize) < self.len(), "cannot take past end of the array");
            }
        });

        Ok(NullArray::new(indices.len()).into_array())
    }
}

#[cfg(test)]
mod test {
    use vortex_dtype::DType;

    use crate::array::null::NullArray;
    use crate::compute::scalar_at::scalar_at;
    use crate::compute::slice::slice;
    use crate::compute::take::take;
    use crate::validity::{ArrayValidity, LogicalValidity};
    use crate::{ArrayTrait, IntoArray};

    #[test]
    fn test_slice_nulls() {
        let nulls = NullArray::new(10).into_array();
        let sliced = NullArray::try_from(slice(&nulls, 0, 4).unwrap()).unwrap();

        assert_eq!(sliced.len(), 4);
        assert!(matches!(
            sliced.logical_validity(),
            LogicalValidity::AllInvalid(4)
        ));
    }

    #[test]
    fn test_take_nulls() {
        let nulls = NullArray::new(10).into_array();
        let taken =
            NullArray::try_from(take(&nulls, &vec![0u64, 2, 4, 6, 8].into_array()).unwrap())
                .unwrap();

        assert_eq!(taken.len(), 5);
        assert!(matches!(
            taken.logical_validity(),
            LogicalValidity::AllInvalid(5)
        ));
    }

    #[test]
    fn test_scalar_at_nulls() {
        let nulls = NullArray::new(10);

        let scalar = scalar_at(&nulls.into_array(), 0).unwrap();
        assert!(scalar.is_null());
        assert_eq!(scalar.dtype().clone(), DType::Null);
    }
}
