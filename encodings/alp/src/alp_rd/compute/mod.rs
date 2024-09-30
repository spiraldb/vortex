use vortex::compute::ArrayCompute;
use vortex::compute::unary::ScalarAtFn;
use crate::ALPRDArray;

mod scalar_at;

impl ArrayCompute for ALPRDArray {
    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}
