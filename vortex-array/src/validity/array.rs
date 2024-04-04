use crate::array::Array;
use crate::validity::owned::Validity;
use crate::validity::ValidityView;

pub trait ArrayValidity {
    fn logical_validity(&self) -> Validity;

    fn is_valid(&self, index: usize) -> bool;
}

pub trait OwnedValidity {
    fn validity(&self) -> Option<ValidityView>;
}

impl<T: Array + OwnedValidity> ArrayValidity for T {
    fn logical_validity(&self) -> Validity {
        self.validity()
            .and_then(|v| v.logical_validity())
            .unwrap_or_else(|| Validity::Valid(self.len()))
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .map_or(true, |v| ValidityView::is_valid(&v, index))
    }
}
