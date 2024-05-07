mod flexbuffers;
mod string;

use std::cmp::Ordering;
use std::ops::{Deref, Range};

use arrow_buffer::Buffer as ArrowBuffer;
pub use string::*;
use vortex_dtype::{match_each_native_ptype, NativePType};

#[derive(Debug, Clone)]
pub enum Buffer {
    // TODO(ngates): we could add Aligned(Arc<AVec>) from aligned-vec package
    Arrow(ArrowBuffer),
    Bytes(bytes::Bytes),
}

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Buffer {
    pub fn len(&self) -> usize {
        match self {
            Buffer::Arrow(b) => b.len(),
            Buffer::Bytes(b) => b.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Buffer::Arrow(b) => b.is_empty(),
            Buffer::Bytes(b) => b.is_empty(),
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        match self {
            Buffer::Arrow(b) => {
                Buffer::Arrow(b.slice_with_length(range.start, range.end - range.start))
            }
            Buffer::Bytes(b) => Buffer::Bytes(b.slice(range)),
        }
    }

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        match self {
            Buffer::Arrow(buffer) => unsafe {
                match_each_native_ptype!(T::PTYPE, |$T| {
                    std::mem::transmute(buffer.typed_data::<$T>())
                })
            },
            Buffer::Bytes(bytes) => {
                // From ArrowBuffer::typed_data
                let (prefix, offsets, suffix) = unsafe { bytes.align_to::<T>() };
                assert!(prefix.is_empty() && suffix.is_empty());
                offsets
            }
        }
    }

    pub fn into_vec<T: NativePType>(self) -> Result<Vec<T>, Buffer> {
        match self {
            Buffer::Arrow(buffer) => match_each_native_ptype!(T::PTYPE, |$T| {
                buffer
                    .into_vec()
                    .map(|vec| unsafe { std::mem::transmute::<Vec<$T>, Vec<T>>(vec) })
                    .map_err(Buffer::Arrow)
            }),
            // Cannot always convert bytes into a mutable vec
            Buffer::Bytes(_) => Err(self),
        }
    }
}

impl Deref for Buffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        match self {
            Buffer::Arrow(b) => b.deref(),
            Buffer::Bytes(b) => b.deref(),
        }
    }
}

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        match self {
            Buffer::Arrow(b) => b.as_ref(),
            Buffer::Bytes(b) => b.as_ref(),
        }
    }
}

impl From<&[u8]> for Buffer {
    fn from(value: &[u8]) -> Self {
        // We prefer Arrow since it retains mutability
        Buffer::Arrow(ArrowBuffer::from(value))
    }
}

impl From<Vec<u8>> for Buffer {
    fn from(value: Vec<u8>) -> Self {
        // We prefer Arrow since it retains mutability
        Buffer::Arrow(ArrowBuffer::from_vec(value))
    }
}

impl From<ArrowBuffer> for Buffer {
    fn from(value: ArrowBuffer) -> Self {
        Buffer::Arrow(value)
    }
}

impl PartialEq for Buffer {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(other.as_ref())
    }
}

impl Eq for Buffer {}

impl PartialOrd for Buffer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.as_ref().partial_cmp(other.as_ref())
    }
}
