use arrow_buffer::Buffer;
use vortex_error::VortexResult;

use crate::validity::Validity;
use crate::Array;

pub trait AcceptArrayVisitor {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()>;
}

// TODO(ngates): maybe we make this more like the inverse of TryFromParts?
pub trait ArrayVisitor {
    /// Visit a child column of this array.
    fn visit_column(&mut self, name: &str, array: &Array) -> VortexResult<()>;

    /// Visit a child of this array.
    fn visit_child(&mut self, name: &str, array: &Array) -> VortexResult<()>;

    /// Utility for visiting Array validity.
    fn visit_validity(&mut self, validity: &Validity) -> VortexResult<()> {
        if let Some(v) = validity.array() {
            self.visit_child("validity", v)
        } else {
            Ok(())
        }
    }

    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()>;
}
