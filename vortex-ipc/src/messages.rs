use crate::context::IPCContext;
use crate::flatbuffers::ipc as fb;
use crate::flatbuffers::ipc::Compression;
use crate::ALIGNMENT;
use flatbuffers::{FlatBufferBuilder, WIPOffset};
use vortex::serde::data::ColumnData;
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};
use vortex_schema::DType;

pub(crate) enum IPCMessage<'a> {
    Context(&'a IPCContext),
    Schema(IPCSchema<'a>),
    Chunk(IPCChunk<'a>),
    ChunkColumn(IPCChunkColumn<'a>),
}

pub(crate) struct IPCSchema<'a>(pub &'a DType);
pub(crate) struct IPCChunk<'a>(pub &'a [usize]);
pub(crate) struct IPCChunkColumn<'a>(pub &'a ColumnData);

impl FlatBufferRoot for IPCMessage<'_> {}
impl WriteFlatBuffer for IPCMessage<'_> {
    type Target<'a> = fb::Message<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let header = match self {
            Self::Context(ctx) => ctx.write_flatbuffer(fbb).as_union_value(),
            Self::Schema(schema) => schema.write_flatbuffer(fbb).as_union_value(),
            Self::Chunk(chunk) => chunk.write_flatbuffer(fbb).as_union_value(),
            Self::ChunkColumn(column) => column.write_flatbuffer(fbb).as_union_value(),
        };

        let mut msg = fb::MessageBuilder::new(fbb);
        msg.add_version(Default::default());
        msg.add_header_type(match self {
            Self::Context(_) => fb::MessageHeader::Context,
            Self::Schema(_) => fb::MessageHeader::Schema,
            Self::Chunk(_) => fb::MessageHeader::Chunk,
            Self::ChunkColumn(_) => fb::MessageHeader::ChunkColumn,
        });
        msg.add_header(header);
        msg.add_body_len(0);
        msg.finish()
    }
}

impl<'a> WriteFlatBuffer for IPCSchema<'a> {
    type Target<'t> = fb::Schema<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let dtype = Some(self.0.write_flatbuffer(fbb));
        fb::Schema::create(fbb, &fb::SchemaArgs { dtype })
    }
}

impl<'a> WriteFlatBuffer for IPCChunk<'a> {
    type Target<'t> = fb::Chunk<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let offsets = fbb.create_vector_from_iter(self.0.iter().map(|offset| *offset as u64));
        fb::Chunk::create(
            fbb,
            &fb::ChunkArgs {
                column_offsets: Some(offsets),
            },
        )
    }
}

impl<'a> WriteFlatBuffer for IPCChunkColumn<'a> {
    type Target<'t> = fb::ChunkColumn<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        // Each chunk column just contains the encoding metadata.
        let buffer_offsets = self.0.buffer_offsets(ALIGNMENT);

        let mut fb_buffers = Vec::new();
        for (buffer, buffer_offset) in self.0.buffers().iter().zip(buffer_offsets.iter()) {
            fb_buffers.push(fb::Buffer::new(
                *buffer_offset as u64,
                buffer.len() as u64,
                Compression::None,
            ))
        }
        let fb_buffers = fbb.create_vector(&fb_buffers);

        fb::ChunkColumn::create(
            fbb,
            &fb::ChunkColumnArgs {
                buffers: Some(fb_buffers),
            },
        )
    }
}
