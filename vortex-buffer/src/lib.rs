use bytes::Bytes;
use vortex_dtype::{match_each_native_ptype, NativePType};

#[derive(Debug, Clone)]
pub struct Buffer(Bytes);

unsafe impl Send for Buffer {}
unsafe impl Sync for Buffer {}

impl Buffer {
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn typed_data<T: NativePType>(&self) -> &[T] {
        // Based on ArrowBuffer::typed_data
        let (prefix, offsets, suffix) = unsafe { self.0.align_to::<T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        offsets
    }

    pub fn into_vec<T: NativePType>(self) -> Result<Vec<T>, Buffer> {
        let mut bytes: Bytes = self.0;
        match self {
            Buffer::Owned(buffer) => match_each_native_ptype!(T::PTYPE, |$T| {
                buffer
                    .into_vec()
                    .map(|vec| unsafe { std::mem::transmute::<Vec<$T>, Vec<T>>(vec) })
                    .map_err(Buffer::Owned)
            }),
            Buffer::View(_) => Err(self),
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
