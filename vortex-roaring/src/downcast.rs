// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use vortex::array::{Array, ArrayRef};

use crate::{RoaringBoolArray, RoaringIntArray};

mod private {
    pub trait Sealed {}
}

#[allow(dead_code)]
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
