use flatbuffers::{root, FlatBufferBuilder, Follow, Verifiable, WIPOffset};
use std::io;
use std::io::{Read, Write};

// FIXME(ngates): This is a temporary solution to avoid a cyclic dependency between vortex-error and vortex-flatbuffers.
#[derive(Debug)]
pub enum Error {
    IOError(io::Error),
    FlatBufferError(flatbuffers::InvalidFlatbuffer),
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Error::IOError(value)
    }
}

impl From<flatbuffers::InvalidFlatbuffer> for Error {
    fn from(value: flatbuffers::InvalidFlatbuffer) -> Self {
        Error::FlatBufferError(value)
    }
}

pub trait FlatBufferReader {
    /// Returns Ok(None) if the reader has reached EOF.
    fn read_flatbuffer<'a, F>(&mut self, buffer: &'a mut Vec<u8>) -> Result<Option<F>>
    where
        F: 'a + Follow<'a, Inner = F> + Verifiable;
}

impl<R: Read> FlatBufferReader for R {
    fn read_flatbuffer<'a, F>(&mut self, buffer: &'a mut Vec<u8>) -> Result<Option<F>>
    where
        F: 'a + Follow<'a, Inner = F> + Verifiable,
    {
        let mut msg_size: [u8; 4] = [0; 4];
        if let Err(e) = self.read_exact(&mut msg_size) {
            return match e.kind() {
                io::ErrorKind::UnexpectedEof => Ok(None),
                _ => Err(e.into()),
            };
        }

        let msg_size = u32::from_le_bytes(msg_size) as u64;
        println!("Reading FB {}", msg_size);
        self.take(msg_size).read_to_end(buffer)?;
        Ok(Some(root::<F>(buffer)?))
    }
}

pub trait WriteFlatBuffer {
    type Target<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>>;
}

pub trait FlatBufferRoot {}

pub trait FlatBufferWriter {
    fn write_flatbuffer<F: WriteFlatBuffer + FlatBufferRoot>(
        &mut self,
        msg: &F,
    ) -> io::Result<usize>;
}

impl<W: Write> FlatBufferWriter for W {
    fn write_flatbuffer<F: WriteFlatBuffer + FlatBufferRoot>(
        &mut self,
        msg: &F,
    ) -> io::Result<usize> {
        let mut fbb = FlatBufferBuilder::new();
        let root = msg.write_flatbuffer(&mut fbb);
        fbb.finish_minimal(root);
        let data = fbb.finished_data();
        let data_size: [u8; 4] = (data.len() as u32).to_le_bytes();
        println!("Writing FB {}", data.len());
        self.write(&data_size)?;
        self.write(data)
    }
}
