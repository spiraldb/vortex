use flatbuffers::{FlatBufferBuilder, WIPOffset};
use itertools::Itertools;
use vortex::flatbuffers as fb;
use vortex::{ArrayData, Context, ViewContext};
use vortex_dtype::{match_each_native_ptype, DType};
use vortex_error::{vortex_err, VortexError};
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};
use vortex_scalar::Scalar::{Bool, Primitive};
use vortex_scalar::{BoolScalar, PrimitiveScalar};

use crate::flatbuffers::ipc as fbi;
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
    type Target<'a> = fbi::Message<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let header = match self {
            Self::Context(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::Schema(f) => f.write_flatbuffer(fbb).as_union_value(),
            Self::Chunk(f) => f.write_flatbuffer(fbb).as_union_value(),
        };

        let mut msg = fbi::MessageBuilder::new(fbb);
        msg.add_version(Default::default());
        msg.add_header_type(match self {
            Self::Context(_) => fbi::MessageHeader::Context,
            Self::Schema(_) => fbi::MessageHeader::Schema,
            Self::Chunk(_) => fbi::MessageHeader::Chunk,
        });
        msg.add_header(header);
        msg.finish()
    }
}

impl<'a> WriteFlatBuffer for IPCContext<'a> {
    type Target<'t> = fbi::Context<'t>;

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
                fbi::Encoding::create(
                    fbb,
                    &fbi::EncodingArgs {
                        id: Some(encoding_id),
                    },
                )
            })
            .collect_vec();
        let fb_encodings = fbb.create_vector(fb_encodings.as_slice());

        fbi::Context::create(
            fbb,
            &fbi::ContextArgs {
                encodings: Some(fb_encodings),
            },
        )
    }
}

pub struct SerdeContextDeserializer<'a> {
    pub(crate) fb: fbi::Context<'a>,
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
    type Target<'t> = fbi::Schema<'t>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let dtype = Some(self.0.write_flatbuffer(fbb));
        fbi::Schema::create(fbb, &fbi::SchemaArgs { dtype })
    }
}

impl<'a> WriteFlatBuffer for IPCChunk<'a> {
    type Target<'t> = fbi::Chunk<'t>;

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
                buffers.push(fbi::Buffer::new(
                    offset as u64,
                    buffer.len() as u64,
                    Compression::None,
                ));
                let aligned_size = (buffer.len() + (ALIGNMENT - 1)) & !(ALIGNMENT - 1);
                offset += aligned_size;
            }
        }
        let buffers = Some(fbb.create_vector(&buffers));

        fbi::Chunk::create(
            fbb,
            &fbi::ChunkArgs {
                array,
                buffers,
                buffer_size: offset as u64,
            },
        )
    }
}

impl<'a> WriteFlatBuffer for IPCArray<'a> {
    type Target<'t> = fb::Array<'t>;

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

        let stats = compute_and_build_stats(fbb, self.1);

        fb::Array::create(
            fbb,
            &fb::ArrayArgs {
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
fn compute_and_build_stats<'a>(
    fbb: &'_ mut FlatBufferBuilder<'a>,
    array: &'_ ArrayData,
) -> WIPOffset<fb::ArrayStats<'a>> {
    let primitive_ptype = match array.dtype() {
        DType::Primitive(ptype, _) => Some(ptype),
        _ => None,
    };

    let min = primitive_ptype.and_then(|ptype| {
        match_each_native_ptype!(ptype, |$T| {
            array.statistics().compute_min::<$T>().ok().map(|min| {
                Primitive(PrimitiveScalar::some(min)).write_flatbuffer(fbb)
            })
        })
    });
    let max = primitive_ptype.and_then(|ptype| {
        match_each_native_ptype!(ptype, |$T| {
            array.statistics().compute_max::<$T>().ok().map(|max| {
                Primitive(PrimitiveScalar::some(max)).write_flatbuffer(fbb)
            })
        })
    });

    let is_constant = array
        .statistics()
        .compute_is_constant()
        .ok()
        .map(|v| Bool(BoolScalar::some(v)).write_flatbuffer(fbb));
    let is_sorted = array
        .statistics()
        .compute_is_sorted()
        .ok()
        .map(|v| Bool(BoolScalar::some(v)).write_flatbuffer(fbb));
    let is_strict_sorted = array
        .statistics()
        .compute_is_strict_sorted()
        .ok()
        .map(|v| Bool(BoolScalar::some(v)).write_flatbuffer(fbb));

    let run_count = array
        .statistics()
        .compute_run_count()
        .ok()
        .map(|v| Primitive(PrimitiveScalar::some(v as u64)).write_flatbuffer(fbb));
    let true_count = array
        .statistics()
        .compute_true_count()
        .ok()
        .map(|v| Primitive(PrimitiveScalar::some(v as u64)).write_flatbuffer(fbb));
    let null_count = array
        .statistics()
        .compute_null_count()
        .ok()
        .map(|v| Primitive(PrimitiveScalar::some(v as u64)).write_flatbuffer(fbb));

    let bit_width_freq = array
        .statistics()
        .compute_bit_width_freq()
        .ok()
        .map(|v| {
            v.iter()
                .map(|&inner| inner as u64)
                .map(PrimitiveScalar::some)
                .map(Primitive)
                .map(|v| v.write_flatbuffer(fbb))
                .collect_vec()
        })
        .map(|v| fbb.create_vector(v.as_slice()));

    let trailing_zero_freq = array
        .statistics()
        .compute_trailing_zero_freq()
        .ok()
        .map(|v| {
            v.iter()
                .map(|&inner| inner as u64)
                .map(PrimitiveScalar::some)
                .map(Primitive)
                .map(|v| v.write_flatbuffer(fbb))
                .collect_vec()
        })
        .map(|v| fbb.create_vector(v.as_slice()));

    let stat_args = &fb::ArrayStatsArgs {
        min,
        max,
        is_sorted,
        is_strict_sorted,
        is_constant,
        run_count,
        true_count,
        null_count,
        bit_width_freq,
        trailing_zero_freq,
    };

    fb::ArrayStats::create(fbb, stat_args)
}
