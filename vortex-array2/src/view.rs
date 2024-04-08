use std::fmt::{Debug, Formatter};

use arrow_buffer::Buffer;
use vortex::flatbuffers::array as fb;
use vortex_error::{vortex_bail, vortex_err, VortexResult};
use vortex_schema::DType;

use crate::encoding::EncodingRef;
use crate::{Array, IntoArray, ToArray};
use crate::{ArrayParts, SerdeContext};

#[derive(Clone)]
pub struct ArrayView<'v> {
    encoding: EncodingRef,
    dtype: &'v DType,
    array: fb::Array<'v>,
    buffers: &'v [Buffer],
    ctx: &'v SerdeContext,
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
        ctx: &'v SerdeContext,
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
        encoding.with_view_mut(&view, &mut |_| Ok(()))?;

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
    //
    // pub fn nchildren(&self) -> usize {
    //     self.array.children().map(|c| c.len()).unwrap_or_default()
    // }

    pub fn child(&self, idx: usize, dtype: &'v DType) -> Option<ArrayView<'v>> {
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

    fn array_child(&self, idx: usize) -> Option<fb::Array<'v>> {
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

    pub fn buffers(&self) -> &'v [Buffer] {
        // This is only true for the immediate current node?
        self.buffers[0..self.nbuffers()].as_ref()
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

impl ArrayParts for ArrayView<'_> {
    fn dtype(&self) -> &DType {
        self.dtype
    }

    fn buffer(&self, idx: usize) -> Option<&Buffer> {
        self.buffers().get(idx)
    }

    fn child<'a>(&'a self, idx: usize, dtype: &'a DType) -> Option<Array> {
        self.child(idx, dtype).map(|a| a.into_array())
    }

    fn nchildren(&self) -> usize {
        self.array.children().map(|c| c.len()).unwrap_or_default()
    }
}
