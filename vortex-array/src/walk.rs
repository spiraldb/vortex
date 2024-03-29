use crate::array::Array;
use arrow_buffer::Buffer;
use vortex_error::VortexResult;

pub trait ArrayWalker {
    fn visit_child(&mut self, array: &dyn Array) -> VortexResult<()>;

    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()>;
}
