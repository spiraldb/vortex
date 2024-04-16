use arrow_buffer::Buffer as ArrowBuffer;

use crate::ptype::NativePType;
use crate::ToStatic;

#[derive(Debug, Clone)]
pub enum Buffer<'a> {
    Owned(ArrowBuffer),
    View(&'a [u8]),
}

pub type OwnedBuffer = Buffer<'static>;

impl Buffer<'_> {
    pub fn len(&self) -> usize {
        match self {
            Buffer::Owned(buffer) => buffer.len(),
            Buffer::View(slice) => slice.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Buffer::Owned(buffer) => buffer.as_slice(),
            Buffer::View(slice) => slice,
        }
    }

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        match self {
            Buffer::Owned(buffer) => buffer.typed_data::<T>(),
            Buffer::View(slice) => {
                // From ArrowBuffer::typed_data
                let (prefix, offsets, suffix) = unsafe { slice.align_to::<T>() };
                assert!(prefix.is_empty() && suffix.is_empty());
                offsets
            }
        }
    }
}

impl<'a> Buffer<'a> {
    pub fn into_vec<T: NativePType>(self) -> Result<Vec<T>, Buffer<'a>> {
        match self {
            Buffer::Owned(buffer) => buffer.into_vec().map_err(Buffer::Owned),
            Buffer::View(_) => Err(self),
        }
    }
}

impl ToStatic for Buffer<'_> {
    type Static = Buffer<'static>;

    fn to_static(&self) -> Self::Static {
        match self {
            Buffer::Owned(d) => Buffer::Owned(d.clone()),
            Buffer::View(_) => Buffer::Owned(self.into()),
        }
    }
}

impl From<ArrowBuffer> for OwnedBuffer {
    fn from(value: ArrowBuffer) -> Self {
        Buffer::Owned(value)
    }
}

impl From<Buffer<'_>> for ArrowBuffer {
    fn from(value: Buffer<'_>) -> Self {
        match value {
            Buffer::Owned(b) => b,
            Buffer::View(_) => ArrowBuffer::from(&value),
        }
    }
}

impl From<&Buffer<'_>> for ArrowBuffer {
    fn from(value: &Buffer<'_>) -> Self {
        match value {
            Buffer::Owned(b) => b.clone(),
            // FIXME(ngates): this conversion loses alignment information since go via u8.
            Buffer::View(v) => ArrowBuffer::from_vec(v.to_vec()),
        }
    }
}

impl PartialEq for Buffer<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice().eq(other.as_slice())
    }
}

impl Eq for Buffer<'_> {}
