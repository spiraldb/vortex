use bytes::Bytes;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use vortex_flatbuffers::{footer as fb, WriteFlatBuffer};

use crate::layouts::{ChunkedLayoutSpec, ColumnLayoutSpec, FlatLayoutSpec, LayoutId};

#[derive(Debug, Clone)]
pub struct Layout {
    id: LayoutId,
    children: Option<Vec<Layout>>,
    metadata: Option<Bytes>,
}

impl Layout {
    pub fn flat(begin: u64, end: u64) -> Self {
        Self {
            id: FlatLayoutSpec::ID,
            children: None,
            metadata: Some(Bytes::copy_from_slice(
                [begin.to_le_bytes(), end.to_le_bytes()].as_flattened(),
            )),
        }
    }

    pub fn chunked(children: Vec<Layout>, has_metadata: bool) -> Self {
        Self {
            id: ChunkedLayoutSpec::ID,
            children: Some(children),
            metadata: Some(Bytes::copy_from_slice(&[has_metadata as u8])),
        }
    }

    pub fn column(children: Vec<Layout>) -> Self {
        Self {
            id: ColumnLayoutSpec::ID,
            children: Some(children),
            metadata: None,
        }
    }
}

impl WriteFlatBuffer for Layout {
    type Target<'a> = fb::Layout<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let metadata = self.metadata.as_ref().map(|b| fbb.create_vector(b));
        let child_offsets = self.children.as_ref().map(|children| {
            children
                .iter()
                .map(|layout| layout.write_flatbuffer(fbb))
                .collect::<Vec<_>>()
        });
        let children = child_offsets.map(|c| fbb.create_vector(&c));
        fb::Layout::create(
            fbb,
            &fb::LayoutArgs {
                children,
                encoding: self.id.0,
                metadata,
            },
        )
    }
}
