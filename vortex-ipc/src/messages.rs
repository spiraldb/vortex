use flatbuffers::{FlatBufferBuilder, WIPOffset};
use itertools::Itertools;
use vortex::flatbuffers as vfb;
use vortex::stats::Stat;
use vortex::{ArrayData, Context, ViewContext};
use vortex_dtype::{match_each_native_ptype, DType};
use vortex_error::{vortex_err, VortexError};
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};
use vortex_scalar::Scalar::Primitive;
use vortex_scalar::{ListScalarVec, PrimitiveScalar};

use crate::flatbuffers::ipc as fb;
use crate::flatbuffers::ipc::Compression;
use crate::{missing, ALIGNMENT};

pub(crate) enum IPCMessage<'a> {
    Context(IPCContext<'a>),
    Schema(IPCSchema<'a>),
    Chunk(IPCChunk<'a>),
}

pub(crate) struct IPCContext<'a>(pub &'a ViewContext);

pub(crate) struct IPCSchema<'a>(pub &'a DType);

pub(crate) struct IPCChunk<'a>(pub &'a ViewContext, pub &'a ArrayData);

pub(crate) struct IPCArray<'a>(pub &'a ViewContext, pub &'a ArrayData);

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
        };

        let mut msg = fb::MessageBuilder::new(fbb);
        msg.add_version(Default::default());
        msg.add_header_type(match self {
            Self::Context(_) => fb::MessageHeader::Context,
            Self::Schema(_) => fb::MessageHeader::Schema,
            Self::Chunk(_) => fb::MessageHeader::Chunk,
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
            .map(|e| e.id())
            .map(|id| {
                let encoding_id = fbb.create_string(id.as_ref());
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

pub struct SerdeContextDeserializer<'a> {
    pub(crate) fb: fb::Context<'a>,
    pub(crate) ctx: &'a Context,
}

impl<'a> TryFrom<SerdeContextDeserializer<'a>> for ViewContext {
    type Error = VortexError;

    fn try_from(deser: SerdeContextDeserializer<'a>) -> Result<Self, Self::Error> {
        let fb_encodings = deser.fb.encodings().ok_or_else(missing("encodings"))?;
        let mut encodings = Vec::with_capacity(fb_encodings.len());
        for fb_encoding in fb_encodings {
            let encoding_id = fb_encoding.id().ok_or_else(missing("encoding.id"))?;
            encodings.push(
                deser
                    .ctx
                    .lookup_encoding(encoding_id)
                    .ok_or_else(|| vortex_err!("Stream uses unknown encoding {}", encoding_id))?,
            );
        }
        Ok(Self::new(encodings))
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
        let array_data = self.1;
        let array = Some(IPCArray(self.0, array_data).write_flatbuffer(fbb));

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
    type Target<'t> = vfb::Array<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let ctx = self.0;
        let column_data = self.1;

        let encoding = ctx
            .encoding_idx(column_data.encoding().id())
            // FIXME(ngates): return result from this writer?
            .unwrap_or_else(|| panic!("Encoding not found: {:?}", column_data.encoding()));

        let metadata = Some(
            fbb.create_vector(
                column_data
                    .metadata()
                    .try_serialize_metadata()
                    // TODO(ngates): should we serialize externally to here?
                    .unwrap()
                    .as_ref(),
            ),
        );

        let children = column_data
            .children()
            .iter()
            .map(|child| IPCArray(self.0, child).write_flatbuffer(fbb))
            .collect_vec();
        let children = Some(fbb.create_vector(&children));

        let stats = collect_array_stats(fbb, self.1);

        vfb::Array::create(
            fbb,
            &vfb::ArrayArgs {
                version: Default::default(),
                has_buffer: column_data.buffer().is_some(),
                encoding,
                metadata,
                stats: Some(stats),
                children,
            },
        )
    }
}

/// Computes all stats and uses the results to create an ArrayStats table for the flatbuffer message
fn collect_array_stats<'a>(
    fbb: &'_ mut FlatBufferBuilder<'a>,
    array: &'_ ArrayData,
) -> WIPOffset<vfb::ArrayStats<'a>> {
    let primitive_ptype = match array.dtype() {
        DType::Primitive(ptype, _) => Some(ptype),
        _ => None,
    };

    let trailing_zero_freq = array
        .statistics()
        .get_as::<ListScalarVec<u64>>(Stat::TrailingZeroFreq)
        .map(|s| s.0)
        .ok()
        .map(|v| v.iter().copied().collect_vec())
        .map(|v| fbb.create_vector(v.as_slice()));

    let bit_width_freq = array
        .statistics()
        .get_as::<ListScalarVec<u64>>(Stat::BitWidthFreq)
        .map(|s| s.0)
        .ok()
        .map(|v| v.iter().copied().collect_vec())
        .map(|v| fbb.create_vector(v.as_slice()));

    let min = primitive_ptype.and_then(|ptype| {
        match_each_native_ptype!(ptype, |$T| {
            array.statistics().get_as::<$T>(Stat::Min).ok().map(|min| {
                Primitive(PrimitiveScalar::some(min)).write_flatbuffer(fbb)
            })
        })
    });

    let max = primitive_ptype.and_then(|ptype| {
        match_each_native_ptype!(ptype, |$T| {
            array.statistics().get_as::<$T>(Stat::Max).ok().map(|max| {
                Primitive(PrimitiveScalar::some(max)).write_flatbuffer(fbb)
            })
        })
    });

    let stat_args = &vfb::ArrayStatsArgs {
        min,
        max,
        is_sorted: array.statistics().get_as::<bool>(Stat::IsSorted).ok(),
        is_strict_sorted: array.statistics().get_as::<bool>(Stat::IsStrictSorted).ok(),
        is_constant: array.statistics().get_as::<bool>(Stat::IsConstant).ok(),
        run_count: array.statistics().get_as::<u64>(Stat::RunCount).ok(),
        true_count: array.statistics().get_as::<u64>(Stat::TrueCount).ok(),
        null_count: array.statistics().get_as::<u64>(Stat::NullCount).ok(),
        bit_width_freq,
        trailing_zero_freq,
    };

    vfb::ArrayStats::create(fbb, stat_args)
}
