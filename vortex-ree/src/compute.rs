use vortex::array::Array;
use vortex::compute::flatten::{flatten, FlattenFn, FlattenedArray};
use vortex::compute::scalar_at::{scalar_at, ScalarAtFn};
use vortex::compute::ArrayCompute;
use vortex::scalar::Scalar;
use vortex::validity::ArrayValidity;
use vortex_error::{VortexError, VortexResult};

use crate::compress::ree_decode;
use crate::REEArray;

impl ArrayCompute for REEArray {
    fn flatten(&self) -> Option<&dyn FlattenFn> {
        Some(self)
    }

    fn scalar_at(&self) -> Option<&dyn ScalarAtFn> {
        Some(self)
    }
}

impl FlattenFn for REEArray {
    fn flatten(&self) -> VortexResult<FlattenedArray> {
        let ends = flatten(self.ends())?;
        let FlattenedArray::Primitive(pends) = ends else {
            return Err(VortexError::InvalidArgument(
                "REE Ends array didn't flatten to primitive".into(),
            ));
        };

        let values = flatten(self.values())?;
        if let FlattenedArray::Primitive(pvalues) = values {
            ree_decode(&pends, &pvalues, self.validity(), self.offset(), self.len())
                .map(FlattenedArray::Primitive)
        } else {
            Err(VortexError::InvalidArgument(
                "Cannot yet flatten non-primitive REE array".into(),
            ))
        }
    }
}

impl ScalarAtFn for REEArray {
    fn scalar_at(&self, index: usize) -> VortexResult<Scalar> {
        scalar_at(self.values(), self.find_physical_index(index)?)
    }
}
