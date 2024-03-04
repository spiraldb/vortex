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

use crate::array::constant::ConstantArray;
use crate::array::{Array, ArrayRef};
use crate::compute::take::TakeFn;
use crate::error::VortexResult;

impl TakeFn for ConstantArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        Ok(ConstantArray::new(dyn_clone::clone_box(self.scalar()), indices.len()).boxed())
    }
}
