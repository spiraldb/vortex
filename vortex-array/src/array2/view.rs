use std::fmt::{Debug, Formatter};

use arrow_buffer::Buffer;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};
use vortex_schema::DType;

use crate::array2::ArrayData;
use crate::array2::SerdeContext;
use crate::array2::{ArrayDef, ArrayMetadata, EncodingRef, ParseArrayMetadata, ToArrayData};
use crate::flatbuffers::array as fb;

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
            .field("dtype", &self.dtype)
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

    pub fn dtype(&self) -> &DType {
        self.dtype
    }

    pub fn metadata(&self) -> Option<&'v [u8]> {
        self.array.metadata().map(|m| m.bytes())
    }

    pub fn nchildren(&self) -> usize {
        self.array.children().map(|c| c.len()).unwrap_or_default()
    }

    pub fn child(&self, idx: usize, dtype: &'v DType) -> Option<ArrayView<'v>> {
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

    /// The number of buffers used by the current Array.
    pub fn nbuffers(&self) -> usize {
        self.array.nbuffers() as usize
    }

    /// The number of buffers used by the current Array and all its children.
    fn cumulative_nbuffers(array: fb::Array) -> usize {
        let mut nbuffers = array.nbuffers() as usize;
        for child in array.children().unwrap_or_default() {
            nbuffers += Self::cumulative_nbuffers(child);
        }
        nbuffers
    }

    pub fn buffers(&self) -> &'v [Buffer] {
        // This is only true for the immediate current node?
        &self.buffers[0..self.nbuffers()]
    }
}

pub struct TypedArrayView<'v, D: ArrayDef> {
    view: ArrayView<'v>,
    metadata: D::Metadata,
}

impl<'v, D: ArrayDef> TypedArrayView<'v, D> {
    pub fn metadata(&self) -> &D::Metadata {
        &self.metadata
    }

    pub fn view(&'v self) -> &'v ArrayView<'v> {
        &self.view
    }

    pub fn as_array(&self) -> &D::Array<'v>
    where
        Self: AsRef<D::Array<'v>>,
    {
        self.as_ref()
    }
}

impl<'v, D: ArrayDef> TryFrom<&'v ArrayView<'v>> for TypedArrayView<'v, D>
where
    D::Metadata: ParseArrayMetadata,
{
    type Error = VortexError;

    fn try_from(view: &'v ArrayView<'v>) -> Result<Self, Self::Error> {
        if view.encoding().id() != D::ID {
            vortex_bail!("Invalid encoding for array")
        }
        let metadata =
            <<D as ArrayDef>::Metadata as ParseArrayMetadata>::try_from(view.metadata())?;
        Ok(Self {
            view: view.clone(),
            metadata,
        })
    }
}

pub trait ArrayChildren {
    fn child_array_data(&self) -> Vec<ArrayData>;
}

impl<'v, D: ArrayDef> ToArrayData for TypedArrayView<'v, D>
where
    Self: ArrayChildren,
{
    fn to_data(&self) -> ArrayData {
        // TODO(ngates): how do we get the child types? I guess we could walk?

        ArrayData::new(
            self.view().encoding(),
            self.view().dtype().clone(),
            self.metadata().to_arc(),
            self.view().buffers().to_vec().into(),
            self.child_array_data().into(),
        )
    }
}
