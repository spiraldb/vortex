#![cfg(feature = "flexbuffers")]
use std::ops::Range;
use std::str::Utf8Error;

use crate::string::BufferString;
use crate::Buffer;

impl flexbuffers::Buffer for Buffer {
    type BufferString = BufferString;

    fn slice(&self, range: Range<usize>) -> Option<Self> {
        // TODO(ngates): bounds-check and return None?
        Some(Self::slice(self, range))
    }

    fn empty() -> Self {
        Self::from(vec![])
    }

    fn buffer_str(&self) -> Result<Self::BufferString, Utf8Error> {
        BufferString::try_from(self.clone())
    }
}
