use vortex::array::{Array, ArrayRef};
use vortex::compute::ArrayCompute;
use vortex::compute::flatten::{FlattenedArray, FlattenFn};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::take::{take, TakeFn};
use vortex::scalar::Scalar;
use vortex_error::VortexResult;

use crate::FoRArray;
use crate::r#for::compress::decompress;

impl ArrayCompute for FoRArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
    Some(self)
}
    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl FlattenFn for FoRArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        decompress(self).map(FlattenedArray::Primitive)
    }
}

impl TakeFn for FoRArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        Ok(FoRArray::try_new(
            take(self.encoded(), indices)?,
            self.reference.clone(),
            self.shift,
        )?
            .into_array())
    }
}

impl ScalarAtFn for FoRArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        let encoded_scalar: usize = scalar_at(self.encoded(), index)?.try_into()?;
        let reference: usize = self.reference().try_into()?;
        Ok(encoded_scalar >> self.shift() + reference).into())
    }
}
