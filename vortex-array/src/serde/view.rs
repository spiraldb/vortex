use crate::array::{Array, ArrayRef};
use crate::compute::take::{take, TakeFn};
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::flatbuffers::array as fb;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::context::SerdeContext;
use crate::serde::EncodingSerde;
use crate::stats::Stats;
use crate::validity::{ArrayValidity, Validity};
use arrow_buffer::Buffer;
use log::info;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

#[derive(Clone)]
pub struct ArrayView<'a> {
    encoding: EncodingRef,
    dtype: DType,
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
        dtype: DType,
        array: fb::Array<'a>,
        buffers: &'a [Buffer],
    ) -> VortexResult<Self> {
        let encoding = ctx
            .find_encoding(array.encoding())
            .ok_or_else(|| VortexError::InvalidSerde("Encoding ID out of bounds".into()))?;
        let _vtable = encoding.serde().ok_or_else(|| {
            // TODO(ngates): we could fall-back to heap-allocating?
            VortexError::InvalidSerde(
                format!("Encoding {} does not support serde", encoding).into(),
            )
        })?;
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
        &self.dtype
    }

    pub fn metadata(&self) -> Option<&'a [u8]> {
        self.array.metadata().map(|m| m.bytes())
    }

    pub fn child(&self, idx: usize, dtype: DType) -> Option<ArrayView<'a>> {
        let child = self.array_child(idx)?;

        // Figure out how many buffers to skip...
        // We store them depth-first.
        let buffer_offset = self
            .array
            .children()?
            .iter()
            .take(idx)
            .map(|child| Self::cumulative_nbuffers(child))
            .sum();
        let buffer_count = child.buffers().unwrap_or_default().len();

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
            Some(children.get(idx))
        } else {
            None
        }
    }

    /// The number of buffers used by the current Array.
    pub fn nbuffers(&self) -> usize {
        self.array.buffers().unwrap_or_default().len()
    }

    /// The number of buffers used by the current Array and all its children.
    fn cumulative_nbuffers(array: fb::Array) -> usize {
        let mut nbuffers = array.buffers().unwrap_or_default().len();
        for child in array.children().unwrap_or_default() {
            nbuffers += Self::cumulative_nbuffers(child);
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

    fn into_array(self) -> ArrayRef {
        // Not much point adding VTable.into_array for ArrayView since everything is by-reference.
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
        &self.dtype
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
}

impl<'a> ArrayValidity for ArrayView<'a> {
    fn validity(&self) -> Option<Validity> {
        todo!()
    }
}

impl<'a> ArrayDisplay for ArrayView<'a> {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        fmt.property("encoding", self.encoding)?;
        fmt.property("dtype", &self.dtype)?;
        fmt.property("metadata", format!("{:?}", self.array.metadata()))?;
        // for (_i, _child) in self.array.children().unwrap_or_default().iter().enumerate() {
        //     // TODO(ngates): children?
        //     // fmt.child(&format!("{}", i), &child)?;
        // }
        Ok(())
    }
}

impl<'a> ArrayCompute for ArrayView<'a> {
    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self)
    }
}

impl<'a> TakeFn for ArrayView<'a> {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let serde = self
            .encoding()
            .serde()
            .ok_or_else(|| VortexError::InvalidSerde("Serde not implemented".into()))?;

        serde
            .compute(self)
            .and_then(|compute| compute.take())
            .map(|t| t.take(self, indices))
            .unwrap_or_else(|| {
                info!(
                    "Serde compute not implemented for {}. Allocating...",
                    self.encoding().id()
                );
                take(&serde.to_array(self), indices)
            })
    }
}
