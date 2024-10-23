use vortex_dtype::{DType, Nullability};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::BoolArray;
use crate::compute::unary::CastFn;
use crate::validity::Validity;
use crate::{Array, ArrayDType, IntoArray};

impl CastFn for BoolArray {
    fn cast(&self, dtype: &DType) -> VortexResult<Array> {
        if !dtype.is_boolean() {
            vortex_bail!("Cannot cast BoolArray to non-Bool type");
        }

        match (self.dtype().nullability(), dtype.nullability()) {
            // convert to same nullability => no-op
            (Nullability::NonNullable, Nullability::NonNullable)
            | (Nullability::Nullable, Nullability::Nullable) => Ok(self.clone().into_array()),

            // convert non-nullable to nullable
            (Nullability::NonNullable, Nullability::Nullable) => {
                Ok(BoolArray::try_new(self.boolean_buffer(), Validity::AllValid)?.into_array())
            }

            // convert nullable to non-nullable, only safe if there are no nulls present.
            (Nullability::Nullable, Nullability::NonNullable) => {
                if self.validity() != Validity::AllValid {
                    vortex_bail!("cannot cast bool array with nulls as non-nullable");
                }

                Ok(BoolArray::try_new(self.boolean_buffer(), Validity::NonNullable)?.into_array())
            }
        }
    }
}
