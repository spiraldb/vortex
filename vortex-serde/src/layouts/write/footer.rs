use flatbuffers::{FlatBufferBuilder, WIPOffset};
use vortex_flatbuffers::{footer as fb, WriteFlatBuffer};

use crate::layouts::write::layouts::Layout;

#[derive(Debug)]
pub struct Footer {
    layout: Layout,
}

impl Footer {
    pub fn new(layout: Layout) -> Self {
        Self { layout }
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
            },
        )
    }
}
