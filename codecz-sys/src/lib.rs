#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use alloc::AlignedVec;
use core::mem::size_of;

pub mod alloc;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

impl ByteBuffer_t {
    pub fn new(buf: &mut AlignedVec<u8>) -> Self {
        Self {
            ptr: buf.as_mut_ptr(),
            len: buf.capacity() as u64,
        }
    }
}

impl<T> From<&mut AlignedVec<T>> for ByteBuffer_t
where
    T: Sized,
{
    fn from(vec: &mut AlignedVec<T>) -> Self {
        Self {
            ptr: vec.as_mut_ptr() as *mut u8,
            len: (vec.capacity() * std::mem::size_of::<T>()) as u64,
        }
    }
}

impl<T> From<&[T]> for ByteBuffer_t
where
    T: Sized,
{
    fn from(slice: &[T]) -> Self {
        Self {
            ptr: slice.as_ptr() as *mut u8,
            len: std::mem::size_of_val(slice) as u64,
        }
    }
}

impl WrittenBuffer_t {
    pub fn new<T>(buf: &mut AlignedVec<T>) -> Self {
        Self {
            buffer: buf.into(),
            bitSizePerElement: (size_of::<T>() * 8) as u8,
            numElements: 0,
            inputBytesUsed: 0,
        }
    }
}

impl OneBufferResult_t {
    pub fn new<T>(buf: &mut AlignedVec<T>) -> Self {
        Self {
            status: ResultStatus_t_UnknownCodecError,
            buf: WrittenBuffer_t::new(buf),
        }
    }
}

impl TwoBufferResult_t {
    pub fn new<T1, T2>(first: &mut AlignedVec<T1>, second: &mut AlignedVec<T2>) -> Self {
        Self {
            status: ResultStatus_t_UnknownCodecError,
            first: WrittenBuffer_t::new(first),
            second: WrittenBuffer_t::new(second),
        }
    }
}

impl Default for AlpExponentsResult_t {
    fn default() -> Self {
        Self {
            status: ResultStatus_t_UnknownCodecError,
            exponents: Default::default(),
        }
    }
}

impl Default for AlpExponents_t {
    fn default() -> Self {
        Self {
            e: u8::MAX,
            f: u8::MAX,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_zimd_max() {
        let vec = [1.0, 2.0, 3.0];
        let max = unsafe { codecz_math_max_f64(vec.as_ptr(), vec.len()) };
        assert_eq!(max, 3.0);
    }
}
