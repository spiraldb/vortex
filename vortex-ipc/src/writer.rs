use std::io::{BufWriter, Write};

use vortex::array::Array;

use vortex_error::VortexResult;
use vortex_flatbuffers::FlatBufferWriter;

use crate::context::IPCContext;

#[allow(dead_code)]
pub struct StreamWriter<W: Write> {
    write: W,
    ctx: IPCContext,
}

impl<W: Write> StreamWriter<BufWriter<W>> {
    pub fn try_new(write: W, ctx: IPCContext) -> VortexResult<Self> {
        Self::try_new_unbuffered(BufWriter::new(write), ctx)
    }
}

impl<W: Write> StreamWriter<W> {
    pub fn try_new_unbuffered(mut write: W, ctx: IPCContext) -> VortexResult<Self> {
        // Write the IPC context to the stream
        write.write_flatbuffer(&ctx)?;

        Ok(Self { write, ctx })
    }

    pub fn write(&mut self, _array: &dyn Array) -> VortexResult<Self> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::StreamReader;
    use std::io::Cursor;
    use vortex::array::primitive::PrimitiveArray;
    use vortex::formatter::display_tree;

    #[test]
    fn test_write_flatbuffer() {
        let array = PrimitiveArray::from(vec![1, 2, 3, 4, 5]);

        let mut cursor = Cursor::new(Vec::new());
        let ctx = IPCContext::default();
        let mut writer = StreamWriter::try_new_unbuffered(&mut cursor, ctx).unwrap();
        writer.write(&array).unwrap();
        cursor.flush().unwrap();

        let mut reader = StreamReader::try_new_unbuffered(cursor).unwrap();
        let read_array = reader.next_array().unwrap().unwrap();
        println!("Array: {:?}", display_tree(&read_array));
        //
        // let msg = "Hello, World!";
        // cursor.write_flatbuffer(&ctx).unwrap();
        // cursor.write_all(msg.as_bytes()).unwrap();
        // cursor.flush().unwrap();s
    }
}
