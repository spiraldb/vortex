use vortex::compute::scalar_at::ScalarAtFn;
use vortex::compute::ArrayCompute;
use vortex::compute::flatten::FlattenFn;

use crate::ALPArray;

mod scalar_at;
mod flatten;

impl ArrayCompute for ALPArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}
