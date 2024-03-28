use crate::flatbuffers::ipc as fb;
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

impl<'a> Array for fb::VortexArray<'a> {
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

impl<'a> ArrayCompute for fb::VortexArray<'a> {}

impl<'a> ArrayValidity for fb::VortexArray<'a> {
    fn validity(&self) -> Option<Validity> {
        todo!()
    }
}

impl<'a> ArrayDisplay for fb::VortexArray<'a> {
    fn fmt(&self, _fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        todo!()
    }
}
