use vortex::array::{ArrayRef, OwnedArray};

use crate::REEArray;

mod private {
    pub trait Sealed {}
}

pub trait DowncastREE: private::Sealed {
    fn maybe_ree(&self) -> Option<&REEArray>;

    fn as_ree(&self) -> &REEArray {
        self.maybe_ree().unwrap()
    }
}

impl private::Sealed for dyn OwnedArray + '_ {}

impl DowncastREE for dyn OwnedArray + '_ {
    fn maybe_ree(&self) -> Option<&REEArray> {
        self.as_any().downcast_ref()
    }
}

impl private::Sealed for ArrayRef {}

impl DowncastREE for ArrayRef {
    fn maybe_ree(&self) -> Option<&REEArray> {
        self.as_any().downcast_ref()
    }
}
