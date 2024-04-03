use arrow_buffer::Buffer;
use vortex_error::VortexResult;

use crate::array::Array;

pub trait ArrayWalker {
    fn visit_child(&mut self, array: &dyn Array) -> VortexResult<()>;

    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()>;
}
