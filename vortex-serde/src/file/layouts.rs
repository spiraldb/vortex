use flatbuffers::{FlatBufferBuilder, WIPOffset};
use vortex_flatbuffers::WriteFlatBuffer;

use crate::flatbuffers::footer as fb;
use crate::writer::ByteRange;

#[derive(Debug, Clone)]
pub enum Layout {
    Chunked(ChunkedLayout),
    Struct(StructLayout),
    Flat(FlatLayout),
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
    range: ByteRange,
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
    children: Vec<Layout>,
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
    children: Vec<Layout>,
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
