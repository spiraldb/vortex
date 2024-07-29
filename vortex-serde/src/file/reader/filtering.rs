use vortex::Array;

use super::projections::Projection;

pub trait FilteringPredicate {
    fn projection(&self) -> &Projection;
    fn evaluate(&mut self, array: &Array) -> Array;
}

pub struct RowFilter {
    pub(crate) _filters: Vec<Box<dyn FilteringPredicate>>,
}

impl RowFilter {
    pub fn new(filters: Vec<Box<dyn FilteringPredicate>>) -> Self {
        Self { _filters: filters }
    }
}
