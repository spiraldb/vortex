use arrow_buffer::Buffer;
use vortex_error::VortexResult;

use crate::validity::Validity;
use crate::Array;

pub trait AcceptArrayVisitor {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()>;
}

// TODO(ngates): maybe we make this more like the inverse of TryFromParts?
pub trait ArrayVisitor {
    fn visit_array(&mut self, name: &str, array: &Array) -> VortexResult<()>;
    fn visit_validity(&mut self, validity: &Validity) -> VortexResult<()> {
        if let Some(v) = validity.array() {
            self.visit_array("validity", v)
        } else {
            Ok(())
        }
    }
    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()>;
}
