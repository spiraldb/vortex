extern crate alloc;

use alloc::sync::Arc;
use arrow_buffer::{BooleanBuffer, Buffer};
use core::ptr::NonNull;

use crate::AlignedVec;

pub fn into_boolean_buffer(values: AlignedVec<u8>, bit_len: usize) -> BooleanBuffer {
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
