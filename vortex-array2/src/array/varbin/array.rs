use vortex_error::VortexResult;

use crate::array::varbin::VarBinArray;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayTrait;

impl ArrayValidity for VarBinArray<'_> {
    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }

    fn logical_validity(&self) -> LogicalValidity {
        todo!()
    }
}

impl AcceptArrayVisitor for VarBinArray<'_> {
    fn accept(&self, _visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        todo!()
    }
}

impl ArrayTrait for VarBinArray<'_> {
    fn len(&self) -> usize {
        todo!()
    }
}
