use std::fmt::Debug;

use vortex_dtype::field::FieldPath;
use vortex_expr::Disjunction;

#[derive(Default, Debug, Clone)]
pub struct RowFilter {
    pub(crate) disjunction: Disjunction,
}

impl RowFilter {
    pub fn new(disjunction: Disjunction) -> Self {
        Self { disjunction }
    }

    pub fn project(&self, _fields: &[FieldPath]) -> Self {
        todo!()
    }
}
