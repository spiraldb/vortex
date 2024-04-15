use vortex_error::VortexResult;

use crate::array::varbin::VarBinArray;
use crate::validity::{ArrayValidity, LogicalValidity};
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};
use crate::ArrayTrait;

impl ArrayValidity for VarBinArray<'_> {
    fn is_valid(&self, index: usize) -> bool {
        self.validity().is_valid(index)
    }

    fn logical_validity(&self) -> LogicalValidity {
        self.validity().to_logical(self.len())
    }
}

impl AcceptArrayVisitor for VarBinArray<'_> {
    fn accept(&self, visitor: &mut dyn ArrayVisitor) -> VortexResult<()> {
        visitor.visit_child("offsets", &self.offsets())?;
        visitor.visit_child("offsets", &self.bytes())?;
        visitor.visit_validity(&self.validity())
    }
}

impl ArrayTrait for VarBinArray<'_> {
    fn len(&self) -> usize {
        self.offsets().len() - 1
    }
}
