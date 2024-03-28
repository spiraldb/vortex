use crate::array::{Array, ArrayRef};
use crate::compute::ArrayCompute;
use crate::encoding::{Encoding, EncodingRef};
use crate::flatbuffers::array as fb;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::stats::Stats;
use crate::validity::{ArrayValidity, Validity};
use arrow_buffer::Buffer;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

#[derive(Clone)]
pub struct ArrayView<'a> {
    encoding: EncodingRef,
    vtable: &'a dyn ArrayViewVTable<'a>,
    dtype: DType,
    array: fb::Array<'a>,
    buffers: &'a [Buffer],
}

impl<'a> Debug for ArrayView<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayView")
            .field("encoding", &self.encoding)
            .field("dtype", &self.dtype)
            .field("array", &self.array)
            .field("buffers", &self.buffers)
            .finish()
    }
}

impl<'a> ArrayView<'a> {
    pub fn try_new(
        encoding: EncodingRef,
        dtype: DType,
        array: fb::Array<'a>,
        buffers: &'a [Buffer],
    ) -> VortexResult<Self> {
        let vtable = encoding.view_vtable().ok_or_else(|| {
            // TODO(ngates): we could fall-back to heap-allocating?
            VortexError::InvalidSerde(
                format!("Encoding {} does not support reading from view", encoding).into(),
            )
        })?;
        Ok(Self {
            encoding,
            dtype,
            vtable,
            array,
            buffers,
        })
    }

    pub fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    pub fn metadata(&self) -> Option<&'a [u8]> {
        self.array.metadata().map(|m| m.bytes())
    }

    pub fn child(&self, idx: usize) -> Option<ArrayView<'a>> {
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

        // Child DType?
        let dtype = self.vtable.child_dtype(self, idx)?;
        Some(ArrayView {
            encoding: self.encoding,
            vtable: self.vtable,
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
    fn cumulative_nbuffers<'b>(array: fb::Array<'b>) -> usize {
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

    pub fn as_typed<E: Encoding>(&self) -> TypedArrayView<'a, E> {
        // TODO(ngates): ideally we would verify the encoding here...
        TypedArrayView {
            view: self.clone(),
            _phantom: PhantomData,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct TypedArrayView<'view, E: Encoding> {
    view: ArrayView<'view>,
    _phantom: PhantomData<E>,
}

impl<'view, E: Encoding> TypedArrayView<'view, E> {
    pub fn view(&self) -> &ArrayView<'view> {
        &self.view
    }
}

pub trait ArrayViewVTable<'view>: Send + Sync {
    fn len(&self, view: &ArrayView<'view>) -> usize;
    fn child_dtype(&self, view: &ArrayView<'view>, idx: usize) -> Option<DType>;
}

impl<'view, E: Encoding> ArrayViewVTable<'view> for E
where
    TypedArrayView<'view, E>: Array,
{
    fn len(&self, data: &ArrayView<'view>) -> usize {
        data.as_typed::<E>().len()
    }

    fn child_dtype(&self, data: &ArrayView<'view>, idx: usize) -> Option<DType> {
        data.as_typed::<E>().child_dtype(idx)
    }
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
        self.vtable.len(self)
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
        self.buffers.iter().map(|b| b.len()).sum()
    }
}

impl<'a> ArrayCompute for ArrayView<'a> {}

impl<'a> ArrayValidity for ArrayView<'a> {
    fn validity(&self) -> Option<Validity> {
        todo!()
    }
}

impl<'a> ArrayDisplay for ArrayView<'a> {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        fmt.property("encoding", &self.encoding)?;
        fmt.property("dtype", &self.dtype)?;
        fmt.property("metadata", format!("{:?}", self.array.metadata()))?;
        for (_i, _child) in self.array.children().unwrap_or_default().iter().enumerate() {
            // TODO(ngates): children?
            // fmt.child(&format!("{}", i), &child)?;
        }
        Ok(())
    }
}
