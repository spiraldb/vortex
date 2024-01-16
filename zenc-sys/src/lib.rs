#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use alloc::AlignedVec;

pub mod alloc;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

impl ByteBuffer_t {
    pub fn new(buf: &mut AlignedVec<u8>) -> Self {
        Self {
            ptr: buf.as_mut_ptr(),
            len: buf.capacity(),
        }
    }
}

impl<T> From<&mut AlignedVec<T>> for ByteBuffer_t
where
    T: Sized + safe_transmute::TriviallyTransmutable,
{
    fn from(vec: &mut AlignedVec<T>) -> Self {
        Self {
            ptr: vec.as_mut_ptr() as *mut u8,
            len: vec.capacity() * std::mem::size_of::<T>(),
        }
    }
}

impl<T> From<&[T]> for ByteBuffer_t
where
    T: Sized + safe_transmute::TriviallyTransmutable,
{
    fn from(slice: &[T]) -> Self {
        Self {
            ptr: slice.as_ptr() as *mut u8,
            len: std::mem::size_of_val(slice),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_zimd_max() {
        let vec = [1.0, 2.0, 3.0];
        let max = unsafe { zimd_max_f64(vec.as_ptr(), vec.len()) };
        assert_eq!(max, 3.0);
    }
}
