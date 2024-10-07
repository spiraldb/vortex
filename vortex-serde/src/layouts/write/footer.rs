use flatbuffers::{FlatBufferBuilder, WIPOffset};
use vortex_flatbuffers::{footer as fb, WriteFlatBuffer};

use crate::layouts::write::layouts::Layout;

#[derive(Debug)]
pub struct Footer {
    layout: Layout,
    row_count: u64,
}

impl Footer {
    pub fn new(layout: Layout, row_count: u64) -> Self {
        Self { layout, row_count }
    }
}

impl WriteFlatBuffer for Footer {
    type Target<'a> = fb::Footer<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let layout_offset = self.layout.write_flatbuffer(fbb);
        fb::Footer::create(
            fbb,
            &fb::FooterArgs {
                layout: Some(layout_offset),
                row_count: self.row_count,
            },
        )
    }
}
