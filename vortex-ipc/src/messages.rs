use flatbuffers::{FlatBufferBuilder, WIPOffset};
use itertools::Itertools;
use vortex::encoding::find_encoding;
use vortex::flatbuffers::array as fba;
use vortex::serde::context::SerdeContext;
use vortex::serde::data::ColumnData;
use vortex_error::{vortex_err, VortexError};
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};
use vortex_schema::DType;

use crate::flatbuffers::ipc as fb;
use crate::flatbuffers::ipc::Compression;
use crate::{missing, ALIGNMENT};

pub(crate) enum IPCMessage<'a> {
    Context(IPCContext<'a>),
    Schema(IPCSchema<'a>),
    Chunk(IPCChunk<'a>),
    ChunkColumn(IPCChunkColumn<'a>),
}

pub(crate) struct IPCContext<'a>(pub &'a SerdeContext);
pub(crate) struct IPCSchema<'a>(pub &'a DType);
pub(crate) struct IPCChunk<'a>(pub &'a [u64]);
pub(crate) struct IPCChunkColumn<'a>(pub &'a SerdeContext, pub &'a ColumnData);
pub(crate) struct IPCArray<'a>(pub &'a SerdeContext, pub &'a ColumnData);

impl FlatBufferRoot for IPCMessage<'_> {}
impl WriteFlatBuffer for IPCMessage<'_> {
    type Target<'a> = fb::Message<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let header = match self {
            Self::Context(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::Schema(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::Chunk(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::ChunkColumn(f) => f.write_flatbuffer(fbb).as_union_value(),
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
        msg.finish()
    }
}

impl<'a> WriteFlatBuffer for IPCContext<'a> {
    type Target<'t> = fb::Context<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let fb_encodings = self
            .0
            .encodings()
            .iter()
            .map(|e| e.id().name())
            .map(|name| {
                let encoding_id = fbb.create_string(name);
                fb::Encoding::create(
                    fbb,
                    &fb::EncodingArgs {
                        id: Some(encoding_id),
                    },
                )
            })
            .collect_vec();
        let fb_encodings = fbb.create_vector(fb_encodings.as_slice());

        fb::Context::create(
            fbb,
            &fb::ContextArgs {
                encodings: Some(fb_encodings),
            },
        )
    }
}

impl<'a> TryFrom<fb::Context<'a>> for SerdeContext {
    type Error = VortexError;

    fn try_from(value: fb::Context<'a>) -> Result<Self, Self::Error> {
        let fb_encodings = value.encodings().ok_or_else(missing("encodings"))?;
        let mut encodings = Vec::with_capacity(fb_encodings.len());
        for fb_encoding in fb_encodings {
            let encoding_id = fb_encoding.id().ok_or_else(missing("encoding.id"))?;
            encodings.push(
                find_encoding(encoding_id)
                    .ok_or_else(|| vortex_err!("Stream uses unknown encoding {}", encoding_id))?,
            );
        }
        Ok(Self::new(encodings.into()))
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
        let offsets = fbb.create_vector_from_iter(self.0.iter().copied());
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
        let col_data = self.1;
        let array = Some(IPCArray(self.0, col_data).write_flatbuffer(fbb));

        // Walk the ColumnData depth-first to compute the buffer offsets.
        let mut buffers = Vec::with_capacity(col_data.buffers().len());
        let mut offset = 0;
        for col_data in col_data.depth_first_traversal() {
            for buffer in col_data.buffers() {
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

        fb::ChunkColumn::create(
            fbb,
            &fb::ChunkColumnArgs {
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
        let column_data = self.1;

        let encoding = self
            .0
            .encoding_idx(column_data.encoding())
            // TODO(ngates): return result from this writer?
            .unwrap_or_else(|| panic!("Encoding not found: {:?}", column_data.encoding()));

        let metadata = column_data
            .metadata()
            .map(|m| fbb.create_vector(m.as_slice()));

        let children = column_data
            .children()
            .iter()
            .map(|child| IPCArray(self.0, child).write_flatbuffer(fbb))
            .collect_vec();
        let children = Some(fbb.create_vector(&children));

        let nbuffers = column_data.buffers().len() as u16; // TODO(ngates): checked cast

        fba::Array::create(
            fbb,
            &fba::ArrayArgs {
                version: Default::default(),
                encoding,
                metadata,
                children,
                nbuffers,
            },
        )
    }
}
