pub use message_reader::*;
use vortex_error::{vortex_err, VortexError};

pub const ALIGNMENT: usize = 64;

pub mod flatbuffers {
    pub use generated::vortex::*;

    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(clippy::all)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/message.rs"));
    }

    mod deps {
        pub mod array {
            pub use vortex::flatbuffers as array;
        }

        pub mod dtype {
            pub use vortex_dtype::flatbuffers as dtype;
        }

        pub mod scalar {
            #[allow(unused_imports)]
            pub use vortex_scalar::flatbuffers as scalar;
        }
    }
}

pub mod array_stream;
pub mod chunked_reader;
pub mod io;
pub mod iter;
mod message_reader;
mod messages;
pub mod reader;
pub mod writer;

pub(crate) const fn missing(field: &'static str) -> impl FnOnce() -> VortexError {
    move || vortex_err!(InvalidSerde: "missing field: {}", field)
}

#[cfg(test)]
pub mod test {
    use std::io::Cursor;

    use vortex::array::chunked::ChunkedArray;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::encoding::EncodingRef;
    use vortex::IntoArray;
    use vortex::{ArrayDType, Context};
    use vortex_alp::ALPEncoding;
    use vortex_fastlanes::BitPackedEncoding;

    use crate::writer::StreamWriter;

    pub fn create_stream() -> Vec<u8> {
        let ctx = Context::default().with_encodings([
            &ALPEncoding as EncodingRef,
            &BitPackedEncoding as EncodingRef,
        ]);
        let array = PrimitiveArray::from(vec![0, 1, 2]).into_array();
        let chunked_array =
            ChunkedArray::try_new(vec![array.clone(), array.clone()], array.dtype().clone())
                .unwrap()
                .into_array();

        let mut buffer = vec![];
        let mut cursor = Cursor::new(&mut buffer);
        {
            let mut writer = StreamWriter::try_new(&mut cursor, &ctx).unwrap();
            writer.write_array(&array).unwrap();
            writer.write_array(&chunked_array).unwrap();
        }

        // Push some extra bytes to test that the reader is well-behaved and doesn't read past the
        // end of the stream.
        // let _ = cursor.write(b"hello").unwrap();

        buffer
    }
}
