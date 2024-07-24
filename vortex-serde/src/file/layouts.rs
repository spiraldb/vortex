use flatbuffers::{FlatBufferBuilder, WIPOffset};
use vortex_error::{VortexError, VortexResult};
use vortex_flatbuffers::WriteFlatBuffer;

use crate::flatbuffers::footer as fb;
use crate::writer::ByteRange;

#[derive(Debug, Clone)]
pub enum Layout {
    Chunked(ChunkedLayout),
    Struct(StructLayout),
    Flat(FlatLayout),
}

impl Layout {
    pub fn as_struct(&self) -> Option<&StructLayout> {
        match self {
            Self::Struct(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_struct_mut(&mut self) -> Option<&mut StructLayout> {
        match self {
            Self::Struct(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_flat(&self) -> Option<&FlatLayout> {
        match self {
            Self::Flat(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_flat_mut(&mut self) -> Option<&mut FlatLayout> {
        match self {
            Self::Flat(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_chunked(&self) -> Option<&ChunkedLayout> {
        match self {
            Self::Chunked(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_chunked_mut(&mut self) -> Option<&mut ChunkedLayout> {
        match self {
            Self::Chunked(l) => Some(l),
            _ => None,
        }
    }
}

impl WriteFlatBuffer for Layout {
    type Target<'a> = fb::Layout<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let layout_variant = match self {
            Self::Chunked(l) => l.write_flatbuffer(fbb).as_union_value(),
            Self::Struct(l) => l.write_flatbuffer(fbb).as_union_value(),
            Self::Flat(l) => l.write_flatbuffer(fbb).as_union_value(),
        };

        let mut layout = fb::LayoutBuilder::new(fbb);
        layout.add_layout_type(match self {
            Self::Chunked(_) => fb::LayoutVariant::NestedLayout,
            Self::Struct(_) => fb::LayoutVariant::NestedLayout,
            Self::Flat(_) => fb::LayoutVariant::FlatLayout,
        });
        layout.add_layout(layout_variant);
        layout.finish()
    }
}

#[derive(Debug, Copy, Clone)]
pub struct FlatLayout {
    pub(crate) range: ByteRange,
}

impl WriteFlatBuffer for FlatLayout {
    type Target<'a> = fb::FlatLayout<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        fb::FlatLayout::create(
            fbb,
            &fb::FlatLayoutArgs {
                begin: self.range.begin,
                end: self.range.end,
            },
        )
    }
}

impl FlatLayout {
    pub fn new(begin: u64, end: u64) -> Self {
        Self {
            range: ByteRange { begin, end },
        }
    }
}

#[derive(Debug, Clone)]
pub struct ChunkedLayout {
    pub(crate) children: Vec<Layout>,
}

impl WriteFlatBuffer for ChunkedLayout {
    type Target<'a> = fb::NestedLayout<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let child_offsets = self
            .children
            .iter()
            .map(|c| c.write_flatbuffer(fbb))
            .collect::<Vec<_>>();
        let child_vector = fbb.create_vector(&child_offsets);
        fb::NestedLayout::create(
            fbb,
            &fb::NestedLayoutArgs {
                children: Some(child_vector),
                // TODO(robert): Make this pluggable
                encoding: 1u16,
            },
        )
    }
}

impl ChunkedLayout {
    pub fn new(child_ranges: Vec<Layout>) -> Self {
        Self {
            children: child_ranges,
        }
    }

    pub fn metadata_range(&self) -> &Layout {
        &self.children[self.children.len() - 1]
    }
}

// TODO(robert): Should struct layout store a schema? How do you pick a child by name
#[derive(Debug, Clone)]
pub struct StructLayout {
    pub(crate) children: Vec<Layout>,
}

impl WriteFlatBuffer for StructLayout {
    type Target<'a> = fb::NestedLayout<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let child_offsets = self
            .children
            .iter()
            .map(|c| c.write_flatbuffer(fbb))
            .collect::<Vec<_>>();
        let child_vector = fbb.create_vector(&child_offsets);
        fb::NestedLayout::create(
            fbb,
            &fb::NestedLayoutArgs {
                children: Some(child_vector),
                // TODO(robert): Make this pluggable
                encoding: 2u16,
            },
        )
    }
}

impl StructLayout {
    pub fn new(child_ranges: Vec<Layout>) -> Self {
        Self {
            children: child_ranges,
        }
    }
}

impl TryFrom<fb::NestedLayout<'_>> for Layout {
    type Error = VortexError;

    fn try_from(value: fb::NestedLayout<'_>) -> Result<Self, Self::Error> {
        let children = value
            .children()
            .unwrap()
            .iter()
            .map(Layout::try_from)
            .collect::<VortexResult<Vec<_>>>()?;
        match value.encoding() {
            1 => Ok(Layout::Chunked(ChunkedLayout::new(children))),
            2 => Ok(Layout::Struct(StructLayout::new(children))),
            _ => unreachable!(),
        }
    }
}

impl From<fb::FlatLayout<'_>> for FlatLayout {
    fn from(value: fb::FlatLayout<'_>) -> Self {
        FlatLayout::new(value.begin(), value.end())
    }
}

impl TryFrom<fb::FlatLayout<'_>> for Layout {
    type Error = VortexError;

    fn try_from(value: fb::FlatLayout<'_>) -> Result<Self, Self::Error> {
        Ok(Layout::Flat(value.into()))
    }
}

impl TryFrom<fb::Layout<'_>> for Layout {
    type Error = VortexError;

    fn try_from(value: fb::Layout<'_>) -> Result<Self, Self::Error> {
        match value.layout_type() {
            fb::LayoutVariant::FlatLayout => value.layout_as_flat_layout().unwrap().try_into(),
            fb::LayoutVariant::NestedLayout => value.layout_as_nested_layout().unwrap().try_into(),
            _ => unreachable!(),
        }
    }
}
