use enc::array::{Array, ArrayRef};

use crate::{RoaringBoolArray, RoaringIntArray};

mod private {
    pub trait Sealed {}
}

pub trait DowncastRoaring: private::Sealed {
    fn maybe_roaring_int(&self) -> Option<&RoaringIntArray>;

    fn as_roaring_int(&self) -> &RoaringIntArray {
        self.maybe_roaring_int().unwrap()
    }

    fn maybe_roaring_bool(&self) -> Option<&RoaringBoolArray>;

    fn as_roaring_bool(&self) -> &RoaringBoolArray {
        self.maybe_roaring_bool().unwrap()
    }
}

impl private::Sealed for dyn Array {}

impl DowncastRoaring for dyn Array {
    fn maybe_roaring_int(&self) -> Option<&RoaringIntArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_roaring_bool(&self) -> Option<&RoaringBoolArray> {
        self.as_any().downcast_ref()
    }
}

impl private::Sealed for ArrayRef {}

impl DowncastRoaring for ArrayRef {
    fn maybe_roaring_int(&self) -> Option<&RoaringIntArray> {
        self.as_any().downcast_ref()
    }

    fn maybe_roaring_bool(&self) -> Option<&RoaringBoolArray> {
        self.as_any().downcast_ref()
    }
}
