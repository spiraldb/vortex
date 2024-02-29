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

impl private::Sealed for dyn Array {}

impl DowncastALP for dyn Array {
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
