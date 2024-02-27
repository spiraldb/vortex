use vortex::array::{Array, ArrayRef};

use crate::FFORArray;

mod private {
    pub trait Sealed {}
}

pub trait DowncastFFOR: private::Sealed {
    fn maybe_ffor(&self) -> Option<&FFORArray>;

    fn as_ffor(&self) -> &FFORArray {
        self.maybe_ffor().unwrap()
    }
}

impl private::Sealed for dyn Array {}

impl DowncastFFOR for dyn Array {
    fn maybe_ffor(&self) -> Option<&FFORArray> {
        self.as_any().downcast_ref()
    }
}

impl private::Sealed for ArrayRef {}

impl DowncastFFOR for ArrayRef {
    fn maybe_ffor(&self) -> Option<&FFORArray> {
        self.as_any().downcast_ref()
    }
}
