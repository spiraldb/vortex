use vortex_error::VortexResult;

use crate::array::varbin::VarBinArray;
use crate::{ArrayFlatten, Flattened};

impl ArrayFlatten for VarBinArray {
    fn flatten(self) -> VortexResult<Flattened> {
        Ok(Flattened::VarBin(self))
    }
}
