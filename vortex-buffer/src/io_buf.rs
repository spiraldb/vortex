//! Provides types that can be used by I/O frameworks to work with byte buffer-shaped data.

use crate::Buffer;

/// Trait for types that can provide a readonly byte buffer interface to I/O frameworks.
/// 
/// # Safety
/// The type must support contiguous raw memory access via pointer, such as `Vec` or `[u8]`.
pub unsafe trait IoBuf: Unpin + 'static {
    /// Returns a raw pointer to the vector’s buffer.
    fn read_ptr(&self) -> *const u8;

    /// Number of initialized bytes.
    fn bytes_init(&self) -> usize;

    /// Access the buffer as a byte slice
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.read_ptr(), self.bytes_init()) }
    }

    /// Access the buffer as a byte slice with begin and end indices
    #[inline]
    fn slice(self, begin: usize, end: usize) -> Slice<Self>
    where
        Self: Sized,
    {
        Slice {
            buf: self,
            begin,
            end,
        }
    }
}

/// An owned view into a contiguous sequence of bytes.
pub struct Slice<T> {
    buf: T,
    begin: usize,
    end: usize,
}

impl<T> Slice<T> {
    /// Unwrap the slice into its underlying type.
    pub fn into_inner(self) -> T {
        self.buf
    }
}

unsafe impl IoBuf for &'static [u8] {
    #[inline]
    fn read_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        <[u8]>::len(self)
    }
}

unsafe impl<const N: usize> IoBuf for [u8; N] {
    #[inline]
    fn read_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        N
    }
}

unsafe impl IoBuf for Vec<u8> {
    #[inline]
    fn read_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        self.len()
    }
}

unsafe impl<T: IoBuf> IoBuf for Slice<T> {
    #[inline]
    fn read_ptr(&self) -> *const u8 {
        unsafe { self.buf.read_ptr().add(self.begin) }
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        self.end - self.begin
    }
}

unsafe impl IoBuf for Buffer {
    #[inline]
    fn read_ptr(&self) -> *const u8 {
        match self {
            Buffer::Arrow(b) => b.as_ptr(),
            Buffer::Bytes(b) => b.as_ptr(),
        }
    }

    #[inline]
    fn bytes_init(&self) -> usize {
        self.len()
    }
}
