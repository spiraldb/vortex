use std::collections::VecDeque;

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use vortex_flatbuffers::{footer as fb, WriteFlatBuffer};

use crate::layouts::LayoutId;
use crate::stream_writer::ByteRange;

#[derive(Debug, Clone)]
pub enum Layout {
    Nested(NestedLayout),
    Flat(FlatLayout),
}

impl WriteFlatBuffer for Layout {
    type Target<'a> = fb::Layout<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let layout_variant = match self {
            Self::Nested(l) => l.write_flatbuffer(fbb).as_union_value(),
            Self::Flat(l) => l.write_flatbuffer(fbb).as_union_value(),
        };

        let mut layout = fb::LayoutBuilder::new(fbb);
        layout.add_layout_type(match self {
            Self::Nested(_) => fb::LayoutVariant::NestedLayout,
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
pub struct NestedLayout {
    pub(crate) children: VecDeque<Layout>,
    pub(crate) id: LayoutId,
}

impl WriteFlatBuffer for NestedLayout {
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
        let children = fbb.create_vector(&child_offsets);
        fb::NestedLayout::create(
            fbb,
            &fb::NestedLayoutArgs {
                children: Some(children),
                encoding: self.id.0,
            },
        )
    }
}

impl NestedLayout {
    pub fn new(children: VecDeque<Layout>, id: LayoutId) -> Self {
        Self { children, id }
    }
}
