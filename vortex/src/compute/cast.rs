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
use crate::array::Array;
use crate::error::{VortexError, VortexResult};
use crate::ptype::PType;
use crate::scalar::Scalar;

pub trait CastPrimitiveFn {
    fn cast_primitive(&self, ptype: &PType) -> VortexResult<PrimitiveArray>;
}

pub fn cast_primitive(array: &dyn Array, ptype: &PType) -> VortexResult<PrimitiveArray> {
    PType::try_from(array.dtype()).map_err(|_| VortexError::InvalidDType(array.dtype().clone()))?;
    array
        .cast_primitive()
        .map(|t| t.cast_primitive(ptype))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "cast_primitive",
                array.encoding().id(),
            ))
        })
}
