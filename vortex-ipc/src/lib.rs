use crate::context::IPCContext;
use crate::flatbuffers::ipc as fb;
use crate::writer::StreamWriter;
use ::flatbuffers::{FlatBufferBuilder, WIPOffset};
use std::io::Write;
use vortex_error::VortexError;
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};
use vortex_schema::DType;

pub mod flatbuffers {
    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(clippy::all)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/message.rs"));
    }
    pub use generated::vortex::*;
}

pub(crate) mod flatbuffers_deps {
    pub mod dtype {
        pub use vortex_schema::flatbuffers as dtype;
    }
}

mod chunked;
pub mod context;
pub mod reader;
pub mod writer;

pub(crate) const fn missing(field: &'static str) -> impl FnOnce() -> VortexError {
    move || VortexError::InvalidSerde(format!("missing field: {}", field).into())
}

pub(crate) enum Message<'a> {
    Context(&'a IPCContext),
    Schema(&'a DType),
}

impl FlatBufferRoot for Message<'_> {}
impl WriteFlatBuffer for Message<'_> {
    type Target<'a> = fb::Message<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let header = match self {
            Self::Context(ctx) => ctx.write_flatbuffer(fbb).as_union_value(),
            Self::Schema(dtype) => dtype.write_flatbuffer(fbb).as_union_value(),
        };

        let mut msg = fb::MessageBuilder::new(fbb);
        msg.add_version(Default::default());
        msg.add_header_type(match self {
            Self::Context(_) => fb::MessageHeader::Context,
            Self::Schema(_) => fb::MessageHeader::Schema,
        });
        msg.add_header(header);
        msg.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunked::ArrayChunkReader;
    use crate::reader::StreamReader;
    use std::io::Cursor;
    use vortex::array::primitive::PrimitiveArray;

    #[test]
    fn test_write_flatbuffer() {
        let array = PrimitiveArray::from(vec![1, 2, 3, 4, 5]);

        let mut cursor = Cursor::new(Vec::new());
        let ctx = IPCContext::default();
        let mut writer = StreamWriter::try_new_unbuffered(&mut cursor, ctx).unwrap();
        writer.write(&array).unwrap();
        cursor.flush().unwrap();
        cursor.set_position(0);

        let mut reader = StreamReader::try_new_unbuffered(cursor).unwrap();
        let array_chunk_reader: &mut dyn ArrayChunkReader = reader.next_array().unwrap().unwrap();
        println!("Array Chunk Reader: {:?}", array_chunk_reader.dtype());

        // let array_chunk = array_chunk_reader.next().unwrap().unwrap();
        // println!("Array: {:?}", display_tree(&array_chunk));

        // let msg = "Hello, World!";
        //cursor.write_flatbuffer(&ctx).unwrap();
        // cursor.write_all(msg.as_bytes()).unwrap();
        // cursor.flush().unwrap();s
    }
}
