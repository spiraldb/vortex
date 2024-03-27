use vortex::array::{Array, ArrayRef};

use crate::{BitPackedArray, DeltaArray, FoRArray};

mod private {
    pub trait Sealed {}
}

pub trait DowncastFastlanes: private::Sealed {
    fn maybe_for(&self) -> Option<&FoRArray>;

    fn as_for(&self) -> &FoRArray {
        self.maybe_for().unwrap()
    }

    fn maybe_delta(&self) -> Option<&DeltaArray>;

    fn as_delta(&self) -> &DeltaArray {
        self.maybe_delta().unwrap()
    }

    fn maybe_bitpacked(&self) -> Option<&BitPackedArray>;

    fn as_bitpacked(&self) -> &BitPackedArray {
        self.maybe_bitpacked().unwrap()
    }
}

impl private::Sealed for dyn Array + '_ {}

impl DowncastFastlanes for dyn Array + '_ {
    fn maybe_for(&self) -> Option<&FoRArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_delta(&self) -> Option<&DeltaArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_bitpacked(&self) -> Option<&BitPackedArray> {
        self.as_any().downcast_ref()
    }
}

impl private::Sealed for ArrayRef {}

impl DowncastFastlanes for ArrayRef {
    fn maybe_for(&self) -> Option<&FoRArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_delta(&self) -> Option<&DeltaArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_bitpacked(&self) -> Option<&BitPackedArray> {
        self.as_any().downcast_ref()
    }
}
