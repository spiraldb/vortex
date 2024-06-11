//! Implementation of the [AsArrowArray] trait for [ConstantArray] that is representing
//! [DType::Null] values.

use std::sync::Arc;

use arrow_array::{ArrayRef as ArrowArrayRef, NullArray};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};

use crate::array::constant::ConstantArray;
use crate::compute::as_arrow::AsArrowArray;
use crate::{ArrayDType, ArrayTrait};

impl AsArrowArray for ConstantArray {
    fn as_arrow(&self) -> VortexResult<ArrowArrayRef> {
        if self.dtype() != &DType::Null {
            vortex_bail!(InvalidArgument: "only null ConstantArrays convert to arrow");
        }

        let arrow_null = NullArray::new(self.len());
        Ok(Arc::new(arrow_null))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{Array, NullArray};

    use crate::array::constant::ConstantArray;
    use crate::arrow::FromArrowArray;
    use crate::compute::as_arrow::AsArrowArray;
    use crate::{ArrayData, IntoArray};

    #[test]
    fn test_round_trip() {
        let arrow_nulls = NullArray::new(10);
        let vortex_nulls = ArrayData::from_arrow(&arrow_nulls, true).into_array();

        assert_eq!(
            *ConstantArray::try_from(vortex_nulls)
                .unwrap()
                .as_arrow()
                .unwrap()
                .as_any()
                .downcast_ref::<NullArray>()
                .unwrap(),
            arrow_nulls
        );
    }
}
