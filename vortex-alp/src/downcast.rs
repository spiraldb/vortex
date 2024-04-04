use vortex::array::{ArrayRef, OwnedArray};

use crate::ALPArray;

mod private {
    pub trait Sealed {}
}

pub trait DowncastALP: private::Sealed {
    fn maybe_alp(&self) -> Option<&ALPArray>;

    fn as_alp(&self) -> &ALPArray {
        self.maybe_alp().unwrap()
    }
}

impl private::Sealed for dyn OwnedArray + '_ {}

impl DowncastALP for dyn OwnedArray + '_ {
    fn maybe_alp(&self) -> Option<&ALPArray> {
        self.as_any().downcast_ref()
    }
}

impl private::Sealed for ArrayRef {}

impl DowncastALP for ArrayRef {
    fn maybe_alp(&self) -> Option<&ALPArray> {
        self.as_any().downcast_ref()
    }
}
