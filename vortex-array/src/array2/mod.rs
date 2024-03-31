use crate::flatbuffers::array as fb;
use arrow_buffer::Buffer;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

mod data;
#[allow(unused_imports)]
pub use data::*;
mod view;
pub use view::*;
use vortex_error::VortexResult;
use vortex_schema::DType;

mod vtable;
pub use vtable::*;

use crate::encoding::EncodingRef;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ArrayData {
    encoding: EncodingRef,
    dtype: DType,
    metadata: Option<Buffer>,
    children: Arc<[ArrayData]>,
    buffers: Arc<[Buffer]>,
}

#[derive(Clone)]
pub struct ArrayView<'a> {
    encoding: EncodingRef,
    dtype: DType,
    array: fb::Array<'a>,
    buffers: &'a [Buffer],
}

impl<'a> Debug for ArrayView<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayView")
            .field("encoding", &self.encoding)
            .field("dtype", &self.dtype)
            // .field("array", &self.array)
            .field("buffers", &self.buffers)
            .finish()
    }
}

pub trait ArrayMetadata: Send + Sync + Sized {
    fn try_from_bytes<'a>(bytes: Option<&'a [u8]>, dtype: &DType) -> VortexResult<Self>;
}
