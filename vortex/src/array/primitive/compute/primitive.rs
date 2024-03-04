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

use crate::array::primitive::PrimitiveArray;
use crate::compute::primitive::AsPrimitiveFn;
use crate::error::VortexResult;
use crate::ptype::PType;

impl AsPrimitiveFn for PrimitiveArray {
    fn as_primitive(&self, ptype: &PType) -> VortexResult<PrimitiveArray> {
        if self.ptype() == ptype {
            Ok(self.clone())
        } else {
            todo!()
        }
    }
}
