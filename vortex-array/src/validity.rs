use crate::array::validity::Validity;
use crate::array::Array;

pub trait ArrayValidity {
    fn logical_validity(&self) -> Validity;

    fn is_valid(&self, index: usize) -> bool;
}

pub trait OwnedValidity {
    fn validity(&self) -> Option<&Validity>;
}

impl<T: Array + OwnedValidity> ArrayValidity for T {
    fn logical_validity(&self) -> Validity {
        self.validity()
            .map(|v| v.logical_validity())
            .unwrap_or_else(|| Validity::Valid(self.len()))
    }

    fn is_valid(&self, index: usize) -> bool {
        self.validity()
            .map_or(true, |v| Validity::is_valid(v, index))
    }
}
