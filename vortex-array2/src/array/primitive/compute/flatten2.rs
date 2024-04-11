use vortex_error::VortexResult;

use crate::array::primitive::PrimitiveArray2;
use crate::compute::flatten2::{Flatten2Fn, Flattened};

impl PrimitiveArray2<'_> {}

impl Flatten2Fn for PrimitiveArray2<'_> {
    fn flatten2(&self) -> VortexResult<Flattened> {
        Ok(Flattened::Primitive(self.clone()))
    }
}
