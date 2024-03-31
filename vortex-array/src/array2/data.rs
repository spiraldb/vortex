use crate::array::{Array, ArrayRef};
use crate::array2::ArrayMetadata;
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::{ArraySerde, WriteCtx};
use crate::stats::Stats;
use crate::validity::{ArrayValidity, Validity};
use crate::ArrayWalker;
use arrow_buffer::Buffer;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use vortex_error::VortexResult;
use vortex_schema::DType;

#[allow(dead_code)]
pub struct TypedArrayData<M: ArrayMetadata> {
    encoding: EncodingRef,
    dtype: DType,
    metadata: M,
    children: Arc<[ArrayData]>,
    buffers: Arc<[Buffer]>,
}

#[allow(dead_code)]
pub type ArrayData = TypedArrayData<Option<Buffer>>;

impl ArrayData {
    pub fn metadata(&self) -> Option<&[u8]> {
        self.metadata.as_deref()
    }
}

impl<M: ArrayMetadata> TypedArrayData<M> {
    pub fn new(
        encoding: EncodingRef,
        dtype: DType,
        metadata: M,
        children: Vec<ArrayData>,
        buffers: Vec<Buffer>,
    ) -> Self {
        Self {
            encoding,
            dtype,
            metadata,
            children: children.into(),
            buffers: buffers.into(),
        }
    }
}

impl ArrayMetadata for Option<Buffer> {
    fn to_bytes(&self) -> Option<Vec<u8>> {
        match self {
            None => None,
            Some(b) => Some(b.to_vec()),
        }
    }

    fn try_from_bytes<'a>(bytes: Option<&'a [u8]>, _dtype: &DType) -> VortexResult<Self> {
        Ok(bytes.map(|b| Buffer::from_vec(b.to_vec())))
    }
}

impl<M: ArrayMetadata> ArrayCompute for TypedArrayData<M> {}

impl<M: ArrayMetadata> ArrayValidity for TypedArrayData<M> {
    fn validity(&self) -> Option<Validity> {
        todo!()
    }
}

impl<M: ArrayMetadata> ArrayDisplay for TypedArrayData<M> {
    fn fmt(&self, _fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        todo!()
    }
}

impl<M: ArrayMetadata> Debug for TypedArrayData<M> {
    fn fmt(&self, _f: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

impl<M: ArrayMetadata> Array for TypedArrayData<M> {
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
        &self.dtype
    }

    fn stats(&self) -> Stats {
        todo!()
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        todo!()
    }

    fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    fn nbytes(&self) -> usize {
        todo!()
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        Some(self)
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        for child in self.children.iter() {
            walker.visit_child(child)?;
        }
        for buffer in self.buffers.iter() {
            walker.visit_buffer(buffer)?;
        }
        Ok(())
    }
}

impl<M: ArrayMetadata> ArraySerde for TypedArrayData<M> {
    fn write(&self, _ctx: &mut WriteCtx) -> VortexResult<()> {
        todo!()
    }

    fn metadata(&self) -> VortexResult<Option<Vec<u8>>> {
        Ok(self.metadata.to_bytes())
    }
}
