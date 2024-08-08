use flatbuffers::{FlatBufferBuilder, Follow, WIPOffset};
use itertools::Itertools;
use vortex::stats::ArrayStatistics;
use vortex::{flatbuffers as fba, Array};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexError};
use vortex_flatbuffers::message::Compression;
use vortex_flatbuffers::{message as fb, FlatBufferRoot, ReadFlatBuffer, WriteFlatBuffer};

use crate::ALIGNMENT;

pub enum IPCMessage<'a> {
    Schema(IPCSchema<'a>),
    Batch(IPCBatch<'a>),
    Page(IPCPage<'a>),
}

pub struct IPCSchema<'a>(pub &'a DType);
pub struct IPCBatch<'a>(pub &'a Array);
pub struct IPCArray<'a>(pub &'a Array);
pub struct IPCPage<'a>(pub &'a Buffer);

pub struct IPCDType(pub DType);

impl FlatBufferRoot for IPCMessage<'_> {}

impl WriteFlatBuffer for IPCMessage<'_> {
    type Target<'a> = fb::Message<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let header = match self {
            Self::Schema(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::Batch(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::Page(f) => f.write_flatbuffer(fbb).as_union_value(),
        };

        let mut msg = fb::MessageBuilder::new(fbb);
        msg.add_version(Default::default());
        msg.add_header_type(match self {
            Self::Schema(_) => fb::MessageHeader::Schema,
            Self::Batch(_) => fb::MessageHeader::Batch,
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

impl ReadFlatBuffer for IPCDType {
    type Source<'t> = fb::Schema<'t>;
    type Error = VortexError;

    fn read_flatbuffer<'buf>(
        fb: &<Self::Source<'buf> as Follow<'buf>>::Inner,
    ) -> Result<Self, Self::Error> {
        let dtype = DType::try_from(
            fb.dtype()
                .ok_or_else(|| vortex_err!(InvalidSerde: "Schema missing DType"))?,
        )
        .map_err(|e| vortex_err!(InvalidSerde: "Failed to parse DType: {}", e))?;

        Ok(IPCDType(dtype))
    }
}

impl<'a> WriteFlatBuffer for IPCBatch<'a> {
    type Target<'t> = fb::Batch<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let array_data = self.0;
        let array = Some(IPCArray(array_data).write_flatbuffer(fbb));

        let length = array_data.len() as u64;

        // Walk the ColumnData depth-first to compute the buffer offsets.
        let mut buffers = vec![];
        let mut offset = 0;
        for array_data in array_data.depth_first_traversal() {
            if let Some(buffer) = array_data.buffer() {
                let aligned_size = (buffer.len() + (ALIGNMENT - 1)) & !(ALIGNMENT - 1);
                buffers.push(fb::Buffer::new(
                    offset as u64,
                    (aligned_size - buffer.len()) as u16,
                    Compression::None,
                ));
                offset += aligned_size;
            }
        }
        let buffers = Some(fbb.create_vector(&buffers));

        fb::Batch::create(
            fbb,
            &fb::BatchArgs {
                array,
                length,
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
