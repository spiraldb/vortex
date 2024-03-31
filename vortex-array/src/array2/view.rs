use crate::array::{Array, ArrayRef};
use crate::array2::{ArrayMetadata, ArrayView, ArrayViewVTable};
use crate::compute::take::TakeFn;
use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::flatbuffers::array as fb;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::stats::Stats;
use crate::validity::{ArrayValidity, Validity};
use arrow_buffer::Buffer;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

impl<'a> ArrayView<'a> {
    pub fn try_new(
        encoding: EncodingRef,
        dtype: DType,
        array: fb::Array<'a>,
        buffers: &'a [Buffer],
    ) -> VortexResult<Self> {
        let _vtable = encoding.view_vtable().ok_or_else(|| {
            // TODO(ngates): we could fall-back to heap-allocating?
            VortexError::InvalidSerde(
                format!("Encoding {} does not support reading from view", encoding).into(),
            )
        })?;
        Ok(Self {
            encoding,
            dtype,
            array,
            buffers,
        })
    }

    pub fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    pub fn vtable(&self) -> &ArrayViewVTable {
        self.encoding.view_vtable().unwrap()
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

        Some(ArrayView {
            encoding: self.encoding,
            dtype,
            array: child,
            buffers: &self.buffers[buffer_offset..][0..buffer_count],
        })
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

    pub fn try_as_typed<M>(&'a self) -> VortexResult<TypedArrayView<'a, M>>
    where
        M: ArrayMetadata,
    {
        // TODO(ngates): ideally we would verify the encoding here...
        Ok(TypedArrayView {
            view: self.clone(),
            metadata: M::try_from_bytes(self.metadata(), &self.dtype)?,
        })
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct TypedArrayView<'view, M> {
    view: ArrayView<'view>,
    metadata: M,
}

impl<'view, M> TypedArrayView<'view, M>
where
    M: ArrayMetadata,
{
    pub fn try_new(view: &ArrayView<'view>) -> VortexResult<Self> {
        Ok(Self {
            view: view.clone(),
            metadata: M::try_from_bytes(view.metadata(), &view.dtype)?,
        })
    }

    pub fn metadata(&self) -> &M {
        &self.metadata
    }

    pub fn view(&self) -> &ArrayView<'view> {
        &self.view
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
        todo!()
        // self.encoding.view_vtable().unwrap()
        // self.vtable.to_array(self).unwrap()
    }

    fn into_array(self) -> ArrayRef {
        unreachable!()
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

impl<'a> ArrayCompute for ArrayView<'a> {
    fn take(&self) -> Option<&dyn TakeFn> {
        Some(self.vtable())
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

impl<'view, M: ArrayMetadata> ArrayCompute for TypedArrayView<'view, M> {}

impl<'view, M: ArrayMetadata> ArrayValidity for TypedArrayView<'view, M> {
    fn validity(&self) -> Option<Validity> {
        todo!()
    }
}

impl<'view, M: ArrayMetadata> ArrayDisplay for TypedArrayView<'view, M> {
    fn fmt(&self, _fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        todo!()
    }
}

impl<'view, M: ArrayMetadata> Array for TypedArrayView<'view, M> {
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
