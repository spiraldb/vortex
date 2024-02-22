use std::io;
use std::io::{ErrorKind, Read, Write};

use crate::array::{Array, ArrayRef, EncodingId, ENCODINGS};
use crate::dtype::DType;
use crate::scalar::{ScalarReader, ScalarWriter};
pub use crate::serde::dtype::{DTypeReader, DTypeWriter, TimeUnitTag};

mod dtype;

pub trait ArraySerde {
    fn write(&self, ctx: &mut WriteCtx) -> io::Result<()>;
}

pub trait EncodingSerde {
    fn read(&self, ctx: &mut ReadCtx) -> io::Result<ArrayRef>;
}

pub struct ReadCtx<'a> {
    schema: &'a DType,
    encodings: Vec<&'static EncodingId>,
    r: &'a mut dyn Read,
}

impl<'a> ReadCtx<'a> {
    pub fn new(schema: &'a DType, r: &'a mut dyn Read) -> Self {
        // TODO(robert): Once write emits encountered encodings support passing them in
        let encodings = ENCODINGS.iter().map(|e| e.id()).collect::<Vec<_>>();
        Self {
            schema,
            encodings,
            r,
        }
    }

    #[inline]
    pub fn schema(&self) -> &DType {
        self.schema
    }

    pub fn subfield(&mut self, idx: usize) -> ReadCtx {
        let DType::Struct(_, fs) = self.schema else {
            panic!("wrong field type")
        };
        ReadCtx::new(&fs[idx], self.reader())
    }

    #[inline]
    pub fn reader(&mut self) -> &mut dyn Read {
        self.r
    }

    #[inline]
    pub fn dtype(&mut self) -> DTypeReader {
        DTypeReader::new(self.reader())
    }

    #[inline]
    pub fn scalar(&mut self) -> ScalarReader {
        ScalarReader::new(self.reader())
    }

    pub fn read_nbytes<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut bytes: [u8; N] = [0; N];
        self.reader().read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub fn read_usize(&mut self) -> io::Result<usize> {
        leb128::read::unsigned(self.reader())
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
            .map(|u| u as usize)
    }

    pub fn read(&mut self) -> io::Result<ArrayRef> {
        let encoding_id = self.read_usize()?;
        if let Some(serde) = ENCODINGS
            .iter()
            .filter(|e| e.id().name() == self.encodings[encoding_id].name())
            .flat_map(|e| e.serde())
            .next()
        {
            serde.read(self)
        } else {
            Err(io::Error::new(ErrorKind::InvalidData, "unknown encoding"))
        }
    }
}

pub struct WriteCtx<'a> {
    w: &'a mut dyn Write,
    encodings: Vec<&'static EncodingId>,
}

impl<'a> WriteCtx<'a> {
    pub fn new(w: &'a mut dyn Write) -> Self {
        let encodings = ENCODINGS.iter().map(|e| e.id()).collect::<Vec<_>>();
        Self { w, encodings }
    }

    #[inline]
    pub fn writer(&mut self) -> &mut dyn Write {
        self.w
    }

    #[inline]
    pub fn dtype(&mut self) -> DTypeWriter {
        DTypeWriter::new(self.writer())
    }

    #[inline]
    pub fn scalar(&mut self) -> ScalarWriter {
        ScalarWriter::new(self.writer())
    }

    pub fn write_usize(&mut self, u: usize) -> io::Result<()> {
        leb128::write::unsigned(self.writer(), u as u64).map(|_| ())
    }

    pub fn write(&mut self, array: &dyn Array) -> io::Result<()> {
        let encoding_id = self
            .encodings
            .iter()
            .position(|e| e.name() == array.encoding().id().name())
            .ok_or(io::Error::new(ErrorKind::InvalidInput, "unknown encoding"))?;
        self.write_usize(encoding_id)?;
        array.serde().write(self)
    }
}
