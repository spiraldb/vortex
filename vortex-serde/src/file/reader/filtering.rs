use vortex::Array;
use vortex_error::VortexResult;

use super::projections::Projection;

pub trait FilteringPredicate {
    fn projection(&self) -> &Projection;
    fn evaluate(&mut self, array: &Array) -> VortexResult<Array>;
}

#[derive(Default)]
pub struct RowFilter {
    pub(crate) _filters: Vec<Box<dyn FilteringPredicate>>,
}

impl RowFilter {
    pub fn new(filters: Vec<Box<dyn FilteringPredicate>>) -> Self {
        Self { _filters: filters }
    }
}
