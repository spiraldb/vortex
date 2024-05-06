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
            pub use vortex::flatbuffers::array;
        }

        pub mod dtype {
            pub use vortex_dtype::flatbuffers as dtype;
        }
    }
}

pub mod iter;
mod messages;
pub mod reader;
pub mod writer;

pub(crate) const fn missing(field: &'static str) -> impl FnOnce() -> VortexError {
    move || vortex_err!(InvalidSerde: "missing field: {}", field)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use vortex::array::primitive::PrimitiveArray;
    use vortex::array::r#struct::StructArray;
    use vortex::validity::Validity;
    use vortex::Context;
    use vortex::{IntoArray, IntoArrayData};

    use crate::iter::FallibleLendingIterator;
    use crate::reader::StreamReader;
    use crate::writer::StreamWriter;

    #[monoio::test_all]
    async fn test_write_flatbuffer() {
        let col = PrimitiveArray::from(vec![0, 1, 2]).into_array();
        let nested_struct = StructArray::try_new(
            ["x".into(), "y".into()].into(),
            vec![col.clone(), col.clone()],
            3,
            Validity::AllValid,
        )
        .unwrap();

        let arr = StructArray::try_new(
            ["a".into(), "b".into()].into(),
            vec![col.clone(), nested_struct.into_array()],
            3,
            Validity::AllValid,
        )
        .unwrap()
        .into_array();

        let ctx = Context::default();
        let mut buf = Vec::new();
        let mut cursor = Cursor::new(&mut buf);
        {
            let mut writer = StreamWriter::try_new_unbuffered(&mut cursor, &ctx).unwrap();
            writer.write_array(&arr).unwrap();
        }
        cursor.flush().unwrap();
        cursor.set_position(0);

        let mut ipc_reader = StreamReader::try_new_unbuffered(buf.as_slice(), &ctx)
            .await
            .unwrap();

        // Read some number of arrays off the stream.
        while let Some(array_reader) = ipc_reader.next().await.unwrap() {
            let mut array_reader = array_reader;
            println!("DType: {:?}", array_reader.dtype());
            // Read some number of chunks from the stream.
            while let Some(chunk) = array_reader.next().await.unwrap() {
                println!("VIEW: {:?}", &chunk);
                let _data = chunk.into_array_data();
                // let taken = take(&chunk, &PrimitiveArray::from(vec![0, 3, 0, 1])).unwrap();
                // let taken = taken.as_primitive().typed_data::<i32>();
                // println!("Taken: {:?}", &taken);
            }
        }
    }
}
