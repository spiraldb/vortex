use vortex_error::VortexResult;

use crate::array::varbin::VarBinArray;
use crate::{Canonical, IntoCanonical};

impl IntoCanonical for VarBinArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        Ok(Canonical::VarBin(self))
    }
}
