use crate::context::IPCContext;
use crate::flatbuffers::ipc::VortexArray;
use arrow_buffer::Buffer;
use std::any::Any;
use std::sync::Arc;
use vortex::array::{Array, ArrayRef};
use vortex::compute::ArrayCompute;
use vortex::encoding::EncodingRef;
use vortex::formatter::{ArrayDisplay, ArrayFormatter};
use vortex::stats::Stats;
use vortex::validity::{ArrayValidity, Validity};
use vortex_error::VortexResult;
use vortex_schema::DType;

#[derive(Debug)]
pub struct ArrayView<'a> {
    pub(crate) ctx: &'a IPCContext,
    pub(crate) array: VortexArray<'a>,
    pub(crate) fb_buffer: &'a [u8],
    pub(crate) buffers: &'a [Buffer],
}

impl<'a> Array for ArrayView<'a> {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        todo!()
    }

    fn to_array(&self) -> ArrayRef {
        todo!()
    }

    fn into_array(self) -> ArrayRef {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn dtype(&self) -> &DType {
        todo!()
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        todo!()
    }

    fn encoding(&self) -> EncodingRef {
        todo!()
    }

    fn nbytes(&self) -> usize {
        todo!()
    }
}

impl<'a> ArrayCompute for ArrayView<'a> {}

impl<'a> ArrayValidity for ArrayView<'a> {
    fn validity(&self) -> Option<Validity> {
        todo!()
    }
}

impl<'a> ArrayDisplay for ArrayView<'a> {
    fn fmt(&self, _fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        todo!()
    }
}
