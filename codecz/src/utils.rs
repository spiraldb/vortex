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

extern crate alloc;

use crate::{AlignedVec, ALIGNED_ALLOCATOR};
use alloc::sync::Arc;
use arrow_buffer::{BooleanBuffer, Buffer};
use core::ptr::NonNull;

pub fn into_u32_vec(bb: &BooleanBuffer, cardinality: usize) -> AlignedVec<u32> {
    let mut vec: AlignedVec<u32> = AlignedVec::with_capacity_in(cardinality, ALIGNED_ALLOCATOR);
    if cardinality > 0 {
        for idx in bb.set_indices() {
            vec.push(idx as u32);
        }
    }
    vec
}

pub fn gather_patches<T: Copy + Sized>(data: &[T], indices: &[u32]) -> AlignedVec<T> {
    let mut vec: AlignedVec<T> = AlignedVec::with_capacity_in(indices.len(), ALIGNED_ALLOCATOR);
    for idx in indices {
        vec.push(data[*idx as usize]);
    }
    vec
}

pub(crate) fn into_boolean_buffer(values: AlignedVec<u8>, bit_len: usize) -> BooleanBuffer {
    let ptr = values.as_ptr();
    let buffer = unsafe {
        Buffer::from_custom_allocation(
            NonNull::new(ptr as _).unwrap(),
            values.len() * core::mem::size_of::<u8>(),
            Arc::new(values),
        )
    };
    BooleanBuffer::new(buffer, 0, bit_len)
}
