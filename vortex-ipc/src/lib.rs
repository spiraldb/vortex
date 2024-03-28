extern crate core;

use std::io::Write;

use lending_iterator::LendingIterator;

use vortex_error::VortexError;

use crate::context::IPCContext;
use crate::writer::StreamWriter;

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
}

pub(crate) mod flatbuffers_deps {
    pub mod dtype {
        pub use vortex_schema::flatbuffers as dtype;
    }
}

mod array;
pub use array::*;

mod chunked;
pub mod context;
mod messages;
pub mod reader;
pub mod writer;

pub(crate) const fn missing(field: &'static str) -> impl FnOnce() -> VortexError {
    move || VortexError::InvalidSerde(format!("missing field: {}", field).into())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use vortex::array::primitive::PrimitiveArray;
    use vortex::formatter::display_tree;

    use crate::reader::StreamReader;

    use super::*;

    #[test]
    fn test_write_flatbuffer() {
        let array = PrimitiveArray::from(vec![1, 2, 3, 4, 5]);

        let mut cursor = Cursor::new(Vec::new());
        let ctx = IPCContext::default();
        let mut writer = StreamWriter::try_new_unbuffered(&mut cursor, ctx).unwrap();
        writer.write(&array).unwrap();
        cursor.flush().unwrap();
        cursor.set_position(0);

        let mut ipc_reader = StreamReader::try_new_unbuffered(cursor).unwrap();
        // Read some number of arrays off the stream.
        while let Some(chunk_reader) = ipc_reader.next() {
            // Each array is split into chunks.
            let mut chunk_reader = chunk_reader.unwrap();
            println!("DType: {:?}", chunk_reader.dtype());
            while let Some(chunk) = chunk_reader.next() {
                let chunk = chunk.unwrap();

                // Do we assume an ArrayView has Array implemented?
                println!("Chunk: {:?}", display_tree(&chunk));
            }
            // let chunk = chunk_reader.next().unwrap();
            // println!("Array Chunk Reader: {:?}", chunk.dtype());
        }
    }
}
