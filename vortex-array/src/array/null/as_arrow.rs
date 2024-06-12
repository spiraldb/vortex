//! Implementation of the [AsArrowArray] trait for [ConstantArray] that is representing
//! [DType::Null] values.

use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, NullArray as ArrowNullArray};
use vortex_error::VortexResult;

use crate::array::null::NullArray;
use crate::compute::as_arrow::AsArrowArray;
use crate::ArrayTrait;

impl AsArrowArray for NullArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        let arrow_null = ArrowNullArray::new(self.len());
        Ok(Arc::new(arrow_null))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{Array, NullArray as ArrowNullArray};

    use crate::array::null::NullArray;
    use crate::arrow::FromArrowArray;
    use crate::compute::as_arrow::AsArrowArray;
    use crate::validity::{ArrayValidity, LogicalValidity};
    use crate::{ArrayData, ArrayTrait, IntoArray};

    #[test]
    fn test_round_trip() {
        let arrow_nulls = ArrowNullArray::new(10);
        let vortex_nulls = ArrayData::from_arrow(&arrow_nulls, true).into_array();

        let vortex_nulls = NullArray::try_from(vortex_nulls).unwrap();
        assert_eq!(vortex_nulls.len(), 10);
        assert!(matches!(
            vortex_nulls.logical_validity(),
            LogicalValidity::AllInvalid(10)
        ));

        let to_arrow = vortex_nulls.as_arrow().unwrap();
        assert_eq!(
            *to_arrow.as_any().downcast_ref::<ArrowNullArray>().unwrap(),
            arrow_nulls
        );
    }
}
