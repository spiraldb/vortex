extern crate core;

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
    use vortex::array2::ViewContext;

    use vortex::array::primitive::PrimitiveArray;
    use vortex::compute::take::take;

    use crate::reader::StreamReader;

    use super::*;

    #[test]
    fn test_write_flatbuffer() {
        let array = PrimitiveArray::from_iter(vec![Some(1), None, None, Some(4), Some(5)]);

        let mut cursor = Cursor::new(Vec::new());
        let ctx = ViewContext::default();
        let mut writer = StreamWriter::try_new_unbuffered(&mut cursor, ctx).unwrap();
        writer.write(&array).unwrap();
        cursor.flush().unwrap();
        cursor.set_position(0);

        let mut ipc_reader = StreamReader::try_new_unbuffered(cursor).unwrap();

        // Read some number of arrays off the stream.
        while let Some(array_reader) = ipc_reader.next().unwrap() {
            let mut array_reader = array_reader;
            println!("DType: {:?}", array_reader.dtype());
            // Read some number of chunks from the stream.
            while let Some(chunk) = array_reader.next().unwrap() {
                println!("VIEW: {:?}", &chunk);
                let taken = take(&chunk, &PrimitiveArray::from(vec![0, 1, 0, 1])).unwrap();
                println!("Taken: {:?}", &taken);
            }
        }
    }
}
