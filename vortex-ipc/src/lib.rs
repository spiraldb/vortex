extern crate core;

use crate::context::IPCContext;
use crate::iter::FallibleLendingIterator;
use crate::writer::StreamWriter;
use vortex_error::VortexError;

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
            pub use vortex::flatbuffers::array;
        }
        pub mod dtype {
            pub use vortex_schema::flatbuffers as dtype;
        }
    }
}

mod chunked;
pub mod context;
mod iter;
mod messages;
pub mod reader;
pub mod writer;

pub(crate) const fn missing(field: &'static str) -> impl FnOnce() -> VortexError {
    move || VortexError::InvalidSerde(format!("missing field: {}", field).into())
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use vortex::array::primitive::PrimitiveData;
    use vortex::formatter::display_tree;

    use crate::reader::StreamReader;

    use super::*;

    #[test]
    fn test_write_flatbuffer() {
        let array = PrimitiveData::from(vec![1, 2, 3, 4, 5]);
        // let array = PrimitiveArray::from(vec![1, 2, 3, 4, 5]);

        let mut cursor = Cursor::new(Vec::new());
        let ctx = IPCContext::default();
        let mut writer = StreamWriter::try_new_unbuffered(&mut cursor, ctx).unwrap();
        writer.write(&array).unwrap();
        cursor.flush().unwrap();
        cursor.set_position(0);

        let mut ipc_reader = StreamReader::try_new_unbuffered(cursor).unwrap();

        // Read some number of arrays off the stream.
        while let Ok(Some(array_reader)) = ipc_reader.next() {
            let mut array_reader = array_reader;
            println!("DType: {:?}", array_reader.dtype());
            while let Ok(Some(chunk)) = array_reader.next() {
                println!("Chunk: {:?}", display_tree(&chunk));
            }
        }
    }
}
