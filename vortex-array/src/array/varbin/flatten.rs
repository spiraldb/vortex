use vortex_error::VortexResult;

use crate::array::varbin::VarBinArray;
use crate::{ArrayFlatten, Flattened};

impl ArrayFlatten for VarBinArray {
    fn flatten<'a>(self) -> VortexResult<Flattened<'a>>
    where
        Self: 'a,
    {
        Ok(Flattened::VarBin(self))
    }
}
