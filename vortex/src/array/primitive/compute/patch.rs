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

use arrow::array::ArrayRef as ArrowArrayRef;
use itertools::Itertools;

use crate::{match_arrow_numeric_type, match_each_native_ptype};
use crate::array::{Array, ArrayRef};
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::PrimitiveArray;
use crate::array::sparse::{SparseArray, SparseEncoding};
use crate::compute::patch::PatchFn;
use crate::compute::take::TakeFn;
use crate::error::{VortexError, VortexResult};
use crate::ptype::NativePType;

impl PatchFn for PrimitiveArray {
    fn patch(&self, patch: &dyn Array) -> VortexResult<ArrayRef> {
        match patch.encoding().id() {
            &SparseEncoding::id() => patch_with_sparse(self, patch.as_sparse().unwrap()),
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
    match_each_native_ptype!(array.ptype(), |$T| {

    });

    let array: ArrowArrayRef = match_arrow_numeric_type!(self.values().dtype(), |$E| {
        let mut validity = BooleanBufferBuilder::new(self.len());
        validity.append_n(self.len(), false);
        let mut values = vec![<$E as ArrowPrimitiveType>::Native::default(); self.len()];
        let mut offset = 0;
        for values_array in self.values().iter_arrow() {
            for v in values_array.as_primitive::<$E>().values() {
                let idx = indices[offset];
                values[idx] = *v;
                validity.set_bit(idx, true);
                offset += 1;
            }
        }
        Arc::new(ArrowPrimitiveArray::<$E>::new(
            ScalarBuffer::from(values),
            Some(NullBuffer::from(validity.finish())),
        ))
    });
}

fn apply_sparse_patch<T: NativePType>(
    array: &mut Vec<T>,
    patch_indices: Vec<usize>,
    patch_values: &[T],
) {
    for (idx, value) in patch_indices.iter().zip_eq(patch_values) {
        array[*idx] = *value;
    }
}
