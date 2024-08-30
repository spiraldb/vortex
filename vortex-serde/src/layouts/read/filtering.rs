use std::fmt::Debug;
use std::sync::Arc;

use vortex_dtype::field::FieldPath;
use vortex_expr::VortexExpr;

#[derive(Debug, Clone)]
pub struct RowFilter {
    pub(crate) filter: Arc<dyn VortexExpr>,
}

impl RowFilter {
    pub fn new(filter: Arc<dyn VortexExpr>) -> Self {
        Self { filter }
    }

    pub fn project(&self, _fields: &[FieldPath]) -> Self {
        todo!()
    }
}
