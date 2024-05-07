use std::fmt::{Debug, Formatter};

use itertools::Itertools;
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};

use crate::encoding::{EncodingId, EncodingRef};
use crate::flatbuffers::array as fb;
use crate::stats::{EmptyStatistics, Statistics};
use crate::Context;
use crate::{Array, IntoArray, ToArray};

#[derive(Clone)]
pub struct ArrayView<'v> {
    encoding: EncodingRef,
    dtype: DType,
    array: fb::Array<'v>,
    buffers: [Buffer],
    ctx: &'v ViewContext,
    // TODO(ngates): a store a Projection. A projected ArrayView contains the full fb::Array
    //  metadata, but only the buffers from the selected columns. Therefore we need to know
    //  which fb:Array children to skip when calculating how to slice into buffers.
}

impl<'a> Debug for ArrayView<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArrayView")
            .field("encoding", &self.encoding)
            .field("dtype", self.dtype)
            // .field("array", &self.array)
            .field("buffers", &self.buffers)
            .field("ctx", &self.ctx)
            .finish()
    }
}

impl<'v> ArrayView<'v> {
    pub fn try_new(
        ctx: &'v ViewContext,
        dtype: &'v DType,
        array: fb::Array<'v>,
        buffers: &'v [Buffer],
    ) -> VortexResult<Self> {
        let encoding = ctx
            .find_encoding(array.encoding())
            .ok_or_else(|| vortex_err!(InvalidSerde: "Encoding ID out of bounds"))?;

        if buffers.len() != Self::cumulative_nbuffers(array) {
            vortex_bail!(InvalidSerde:
                "Incorrect number of buffers {}, expected {}",
                buffers.len(),
                Self::cumulative_nbuffers(array)
            )
        }

        let view = Self {
            encoding,
            dtype,
            array,
            buffers,
            ctx,
        };

        // Validate here that the metadata correctly parses, so that an encoding can infallibly
        // implement Encoding::with_view().
        // FIXME(ngates): validate the metadata
        view.to_array().with_dyn(|_| Ok::<(), VortexError>(()))?;

        Ok(view)
    }

    pub fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    pub fn dtype(&self) -> &DType {
        self.dtype
    }

    pub fn metadata(&self) -> Option<&'v [u8]> {
        self.array.metadata().map(|m| m.bytes())
    }

    // TODO(ngates): should we separate self and DType lifetimes? Should DType be cloned?
    pub fn child(&'v self, idx: usize, dtype: &'v DType) -> Option<ArrayView<'v>> {
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

    fn array_child(&self, idx: usize) -> Option<fb::Array<'v>> {
        let children = self.array.children()?;
        if idx < children.len() {
            Some(children.get(idx))
        } else {
            None
        }
    }

    /// Whether the current Array makes use of a buffer
    pub fn has_buffer(&self) -> bool {
        self.array.has_buffer()
    }

    /// The number of buffers used by the current Array and all its children.
    fn cumulative_nbuffers(array: fb::Array) -> usize {
        let mut nbuffers = if array.has_buffer() { 1 } else { 0 };
        for child in array.children().unwrap_or_default() {
            nbuffers += Self::cumulative_nbuffers(child)
        }
        nbuffers
    }

    pub fn buffer(&self) -> Option<&'v Buffer> {
        self.has_buffer().then(|| &self.buffers[0])
    }

    pub fn statistics(&self) -> &dyn Statistics {
        // TODO(ngates): store statistics in FlatBuffers
        &EmptyStatistics
    }
}

impl ToArray for ArrayView<'_> {
    fn to_array(&self) -> Array {
        Array::View(self.clone())
    }
}

impl<'v> IntoArray<'v> for ArrayView<'v> {
    fn into_array(self) -> Array<'v> {
        Array::View(self)
    }
}

#[derive(Debug)]
pub struct ViewContext {
    encodings: Vec<EncodingRef>,
}

impl ViewContext {
    pub fn new(encodings: Vec<EncodingRef>) -> Self {
        Self { encodings }
    }

    pub fn encodings(&self) -> &[EncodingRef] {
        self.encodings.as_ref()
    }

    pub fn find_encoding(&self, encoding_id: u16) -> Option<EncodingRef> {
        self.encodings.get(encoding_id as usize).cloned()
    }

    pub fn encoding_idx(&self, encoding_id: EncodingId) -> Option<u16> {
        self.encodings
            .iter()
            .position(|e| e.id() == encoding_id)
            .map(|i| i as u16)
    }
}

impl Default for ViewContext {
    fn default() -> Self {
        todo!("FIXME(ngates): which encodings to enable?")
    }
}

impl From<&Context> for ViewContext {
    fn from(value: &Context) -> Self {
        ViewContext::new(value.encodings().collect_vec())
    }
}
