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
