use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_buffer::Buffer;
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::DType;

use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::flatbuffers::array as fb;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::context::SerdeContext;
use crate::serde::EncodingSerde;
use crate::stats::Stats;
use crate::validity::ArrayValidity;
use crate::validity::Validity;
use crate::ArrayWalker;

#[derive(Clone)]
pub struct ArrayView<'a> {
    encoding: EncodingRef,
    dtype: &'a DType,
    array: fb::Array<'a>,
    buffers: &'a [Buffer],
    ctx: &'a SerdeContext,
}

impl<'a> Debug for ArrayView<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayView")
            .field("encoding", &self.encoding)
            .field("dtype", &self.dtype)
            // .field("array", &self.array)
            .field("buffers", &self.buffers)
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<'a> ArrayView<'a> {
    pub fn try_new(
        ctx: &'a SerdeContext,
        dtype: &'a DType,
        array: fb::Array<'a>,
        buffers: &'a [Buffer],
    ) -> VortexResult<Self> {
        let encoding = ctx
            .find_encoding(array.encoding())
            .ok_or_else(|| vortex_err!(InvalidSerde: "Encoding ID out of bounds"))?;
        let _vtable = encoding.serde().ok_or_else(|| {
            // TODO(ngates): we could fall-back to heap-allocating?
            vortex_err!(InvalidSerde: "Encoding {} does not support serde", encoding)
        })?;

        if buffers.len() != Self::cumulative_nbuffers(array) {
            vortex_bail!(InvalidSerde:
                "Incorrect number of buffers {}, expected {}",
                buffers.len(),
                Self::cumulative_nbuffers(array)
            )
        }

        Ok(Self {
            encoding,
            dtype,
            array,
            buffers,
            ctx,
        })
    }

    pub fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    pub fn vtable(&self) -> &dyn EncodingSerde {
        self.encoding.serde().unwrap()
    }

    pub fn dtype(&self) -> &DType {
        self.dtype
    }

    pub fn metadata(&self) -> Option<&'a [u8]> {
        self.array.metadata().map(|m| m.bytes())
    }

    pub fn nchildren(&self) -> usize {
        self.array.children().map(|c| c.len()).unwrap_or_default()
    }

    pub fn child(&self, idx: usize, dtype: &'a vortex_schema::DType) -> Option<ArrayView<'a>> {
        let child = self.array_child(idx)?;

        // Figure out how many buffers to skip...
        // We store them depth-first.
        let buffer_offset = self
            .array
            .children()?
            .iter()
            .take(idx)
            .map(|child| {
                child
                    .child()
                    .map(|c| Self::cumulative_nbuffers(c))
                    .unwrap_or_default()
            })
            .sum();
        let buffer_count = Self::cumulative_nbuffers(child);

        Some(
            Self::try_new(
                self.ctx,
                dtype,
                child,
                &self.buffers[buffer_offset..][0..buffer_count],
            )
            .unwrap(),
        )
    }

    fn array_child(&self, idx: usize) -> Option<fb::Array<'a>> {
        let children = self.array.children()?;
        if idx < children.len() {
            children.get(idx).child()
        } else {
            None
        }
    }

    /// The number of buffers used by the current Array.
    pub fn nbuffers(&self) -> usize {
        self.array.nbuffers() as usize
    }

    /// The number of buffers used by the current Array and all its children.
    fn cumulative_nbuffers(array: fb::Array) -> usize {
        let mut nbuffers = array.nbuffers() as usize;
        for child in array.children().unwrap_or_default() {
            nbuffers += child
                .child()
                .map(|c| Self::cumulative_nbuffers(c))
                .unwrap_or_default();
        }
        nbuffers
    }

    pub fn buffers(&self) -> &'a [Buffer] {
        // This is only true for the immediate current node?
        &self.buffers[0..self.nbuffers()]
    }
}

impl<'a> Array for ArrayView<'a> {
    fn as_any(&self) -> &dyn Any {
        panic!("Not implemented for ArrayView")
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        panic!("Not implemented for ArrayView")
    }

    fn to_array(&self) -> ArrayRef {
        self.vtable().to_array(self)
    }

    fn to_array_data(self) -> ArrayRef {
        // Not much point adding VTable.to_array_data for ArrayView since everything is by-reference.
        self.vtable().to_array(&self)
    }

    fn len(&self) -> usize {
        self.vtable().len(self)
    }

    fn is_empty(&self) -> bool {
        todo!()
        // self.vtable.is_empty(self).unwrap()
    }

    fn dtype(&self) -> &DType {
        self.dtype
    }

    fn stats(&self) -> Stats {
        // TODO(ngates): implement a dynamic trait for stats?
        todo!()
    }

    fn slice(&self, _start: usize, _stop: usize) -> VortexResult<ArrayRef> {
        todo!()
    }

    fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    fn nbytes(&self) -> usize {
        self.buffers.iter().map(|b| b.len()).sum()
    }

    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        self.encoding()
            .serde()
            .expect("TODO(ngates): heap allocate ArrayView and invoke compute")
            .with_view_compute(self, f)
    }

    fn walk(&self, _walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        todo!()
    }
}

impl ArrayValidity for ArrayView<'_> {
    fn logical_validity(&self) -> Validity {
        todo!()
    }

    fn is_valid(&self, _index: usize) -> bool {
        todo!()
    }
}

impl<'a> ArrayDisplay for ArrayView<'a> {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        fmt.property("encoding", self.encoding)?;
        fmt.property("dtype", self.dtype)?;
        fmt.property("metadata", format!("{:?}", self.array.metadata()))?;
        fmt.property("nchildren", self.nchildren())
    }
}
