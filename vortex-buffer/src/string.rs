use std::ops::Deref;
use std::str::Utf8Error;

use crate::Buffer;

/// A wrapper around a `Buffer` that guarantees that the buffer contains valid UTF-8.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct BufferString(Buffer);

impl BufferString {
    pub unsafe fn new_unchecked(buffer: Buffer) -> Self {
        Self(buffer)
    }
}

impl From<BufferString> for Buffer {
    fn from(value: BufferString) -> Self {
        value.0
    }
}

impl From<&str> for BufferString {
    fn from(value: &str) -> Self {
        BufferString(Buffer::from(value.as_bytes()))
    }
}

impl TryFrom<Buffer> for BufferString {
    type Error = Utf8Error;

    fn try_from(value: Buffer) -> Result<Self, Self::Error> {
        let _ = std::str::from_utf8(value.as_ref())?;
        Ok(Self(value))
    }
}

impl Deref for BufferString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        // SAFETY: We have already validated that the buffer is valid UTF-8
        unsafe { std::str::from_utf8_unchecked(self.0.as_ref()) }
    }
}
