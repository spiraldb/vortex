use crate::array::bool::BoolArray;
use crate::array::{Array, ArrayRef};
use crate::compute::cast::{cast_bool, CastBoolFn};
use crate::compute::fill::FillForwardFn;
use crate::compute::scalar_at::ScalarAtFn;
use crate::compute::ArrayCompute;
use crate::error::VortexResult;
use crate::scalar::{NullableScalar, Scalar, ScalarRef};
use crate::stats::Stat;

impl ArrayCompute for BoolArray {
    fn cast_bool(&self) -> Option<&dyn CastBoolFn> {
        Some(self)
    }

    fn fill_forward(&self) -> Option<&dyn FillForwardFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl CastBoolFn for BoolArray {
    fn cast_bool(&self) -> VortexResult<BoolArray> {
        Ok(self.clone())
    }
}

impl ScalarAtFn for BoolArray {
    fn scalar_at(&self, index: usize) -> VortexResult<ScalarRef> {
        if self.is_valid(index) {
            Ok(self.buffer.value(index).into())
        } else {
            Ok(NullableScalar::none(self.dtype().clone()).boxed())
        }
    }
}

impl FillForwardFn for BoolArray {
    fn fill_forward(&self) -> VortexResult<ArrayRef> {
        if self.validity().is_none() {
            Ok(dyn_clone::clone_box(self))
        } else if self
            .stats()
            .get_or_compute_as::<usize>(&Stat::NullCount)
            .unwrap()
            == 0usize
        {
            return Ok(BoolArray::new(self.buffer().clone(), None).boxed());
        } else {
            let validity = cast_bool(self.validity().unwrap())?;
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
            Ok(BoolArray::from(filled).boxed())
        }
    }
}

#[cfg(test)]
mod test {
    use crate::array::bool::BoolArray;
    use crate::array::downcast::DowncastArrayBuiltin;
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
