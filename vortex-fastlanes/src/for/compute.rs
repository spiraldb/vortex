use crate::r#for::compress::decompress;
use crate::FoRArray;
use vortex::array::{Array, ArrayRef};
use vortex::compute::flatten::{FlattenFn, FlattenedArray};
use vortex::compute::take::{take, TakeFn};
use vortex::compute::ArrayCompute;
use vortex_error::VortexResult;

impl ArrayCompute for FoRArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
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
