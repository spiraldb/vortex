use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::compute::flatten::{FlattenFn, FlattenedArray};
use vortex::error::VortexResult;

use crate::compress::decompress;
use crate::ALPArray;

impl FlattenFn for ALPArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        Ok(FlattenedArray::Primitive(
            decompress(self)?.as_primitive().clone(),
        ))
    }
}
