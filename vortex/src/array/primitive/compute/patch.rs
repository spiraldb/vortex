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

use itertools::Itertools;

use crate::{compute, match_each_native_ptype};
use crate::array::{Array, ArrayRef, BoxOptionalArray};
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::SparseArray;
use crate::compute::patch::PatchFn;
use crate::error::{VortexError, VortexResult};

impl PatchFn for PrimitiveArray {
    fn patch(&self, patch: &dyn Array) -> VortexResult<ArrayRef> {
        match patch.encoding().id() {
            &SparseArray::ID => patch_with_sparse(self, patch.as_sparse()),
            // TODO(ngates): support a default implementation based on iter_arrow?
            _ => Err(VortexError::MissingKernel(
                "patch",
                self.encoding().id(),
                vec![patch.encoding().id().into()],
            )),
        }
    }
}

fn patch_with_sparse(array: &PrimitiveArray, patch: &SparseArray) -> VortexResult<ArrayRef> {
    let patch_indices = patch.resolved_indices();
    match_each_native_ptype!(array.ptype(), |$T| {
        let mut values = Vec::from(array.typed_data::<$T>());
        let patch_values = compute::primitive::as_primitive::<$T>(patch.values())?;
        for (idx, value) in patch_indices.iter().zip_eq(patch_values.iter()) {
            values[*idx] = *value;
        }
        Ok(PrimitiveArray::from_nullable(
            values,
            array.validity().boxed(),
        ).boxed())
    })
}
