use flatbuffers::{FlatBufferBuilder, WIPOffset};
use itertools::Itertools;
use vortex::stats::ArrayStatistics;
use vortex::{flatbuffers as fba, Array};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};

use crate::flatbuffers::ipc as fb;
use crate::flatbuffers::ipc::Compression;
use crate::ALIGNMENT;

pub enum IPCMessage<'a> {
    Schema(IPCSchema<'a>),
    Chunk(IPCChunk<'a>),
    Page(IPCPage<'a>),
}

pub struct IPCSchema<'a>(pub &'a DType);
pub struct IPCChunk<'a>(pub &'a Array);
pub struct IPCArray<'a>(pub &'a Array);
pub struct IPCPage<'a>(pub &'a Buffer);

impl FlatBufferRoot for IPCMessage<'_> {}

impl WriteFlatBuffer for IPCMessage<'_> {
    type Target<'a> = fb::Message<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let header = match self {
            Self::Schema(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::Chunk(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::Page(f) => f.write_flatbuffer(fbb).as_union_value(),
        };

        let mut msg = fb::MessageBuilder::new(fbb);
        msg.add_version(Default::default());
        msg.add_header_type(match self {
            Self::Schema(_) => fb::MessageHeader::Schema,
            Self::Chunk(_) => fb::MessageHeader::Chunk,
            Self::Page(_) => fb::MessageHeader::Page,
        });
        msg.add_header(header);
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
        let array_data = self.0;
        let array = Some(IPCArray(array_data).write_flatbuffer(fbb));

        // Walk the ColumnData depth-first to compute the buffer offsets.
        let mut buffers = vec![];
        let mut offset = 0;
        for array_data in array_data.depth_first_traversal() {
            if let Some(buffer) = array_data.buffer() {
                buffers.push(fb::Buffer::new(
                    offset as u64,
                    buffer.len() as u64,
                    Compression::None,
                ));
                let aligned_size = (buffer.len() + (ALIGNMENT - 1)) & !(ALIGNMENT - 1);
                offset += aligned_size;
            }
        }
        let buffers = Some(fbb.create_vector(&buffers));

        fb::Chunk::create(
            fbb,
            &fb::ChunkArgs {
                array,
                buffers,
                buffer_size: offset as u64,
            },
        )
    }
}

impl<'a> WriteFlatBuffer for IPCArray<'a> {
    type Target<'t> = fba::Array<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let column_data = self.0;

        let encoding = column_data.encoding().id().code();
        let metadata = match column_data {
            Array::Data(d) => {
                let metadata = d
                    .metadata()
                    .try_serialize_metadata()
                    // TODO(ngates): should we serialize externally to here?
                    .unwrap();
                Some(fbb.create_vector(metadata.as_ref()))
            }
            Array::View(v) => Some(fbb.create_vector(v.metadata().unwrap())),
        };

        let children = column_data
            .children()
            .iter()
            .map(|child| IPCArray(child).write_flatbuffer(fbb))
            .collect_vec();
        let children = Some(fbb.create_vector(&children));

        let stats = Some(column_data.statistics().write_flatbuffer(fbb));

        fba::Array::create(
            fbb,
            &fba::ArrayArgs {
                version: Default::default(),
                has_buffer: column_data.buffer().is_some(),
                encoding,
                metadata,
                stats,
                children,
            },
        )
    }
}

impl<'a> WriteFlatBuffer for IPCPage<'a> {
    type Target<'t> = fb::Page<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let buffer_size = self.0.len();
        let aligned_size = (buffer_size + (ALIGNMENT - 1)) & !(ALIGNMENT - 1);
        let padding_size = aligned_size - buffer_size;

        fb::Page::create(
            fbb,
            &fb::PageArgs {
                buffer_size: buffer_size as u32,
                padding: padding_size as u16,
            },
        )
    }
}
