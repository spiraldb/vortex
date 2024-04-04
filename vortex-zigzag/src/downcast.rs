use vortex::array::{ArrayRef, OwnedArray};

use crate::ZigZagArray;

mod private {
    pub trait Sealed {}
}

pub trait DowncastZigzag: private::Sealed {
    fn maybe_zigzag(&self) -> Option<&ZigZagArray>;

    fn as_zigzag(&self) -> &ZigZagArray {
        self.maybe_zigzag().unwrap()
    }
}

impl private::Sealed for dyn OwnedArray + '_ {}

impl DowncastZigzag for dyn OwnedArray + '_ {
    fn maybe_zigzag(&self) -> Option<&ZigZagArray> {
        self.as_any().downcast_ref()
    }
}

impl private::Sealed for ArrayRef {}

impl DowncastZigzag for ArrayRef {
    fn maybe_zigzag(&self) -> Option<&ZigZagArray> {
        self.as_any().downcast_ref()
    }
}
