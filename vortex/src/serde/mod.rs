// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io;
use std::io::{ErrorKind, Read, Write};

use arrow::buffer::{Buffer, MutableBuffer};

use crate::array::{Array, ArrayRef, EncodingId, ENCODINGS};
use crate::dtype::{DType, IntWidth, Nullability, Signedness};
use crate::scalar::{Scalar, ScalarReader, ScalarWriter};
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
            panic!("Schema was not a struct")
        };
        self.with_schema(&fs[idx])
    }

    #[inline]
    pub fn with_schema<'b>(&'b mut self, schema: &'b DType) -> ReadCtx {
        ReadCtx::new(schema, self.r)
    }

    #[inline]
    pub fn bytes(&mut self) -> ReadCtx {
        self.with_schema(&DType::Int(
            IntWidth::_8,
            Signedness::Unsigned,
            Nullability::NonNullable,
        ))
    }

    #[inline]
    pub fn validity(&mut self) -> ReadCtx {
        self.with_schema(&DType::Bool(Nullability::NonNullable))
    }

    #[inline]
    pub fn dtype(&mut self) -> io::Result<DType> {
        DTypeReader::new(self.r).read()
    }

    #[inline]
    pub fn scalar(&mut self) -> io::Result<Box<dyn Scalar>> {
        ScalarReader::new(self.r).read()
    }

    pub fn read_slice(&mut self) -> io::Result<Vec<u8>> {
        let len = self.read_usize()?;
        let mut data = Vec::<u8>::with_capacity(len);
        self.r.take(len as u64).read_to_end(&mut data)?;
        Ok(data)
    }

    pub fn read_buffer<F: Fn(usize) -> usize>(
        &mut self,
        byte_len: F,
    ) -> io::Result<(usize, Buffer)> {
        let logical_len = self.read_usize()?;
        let buffer_len = byte_len(logical_len);
        let mut buffer = MutableBuffer::from_len_zeroed(buffer_len);
        self.r.read_exact(&mut buffer)?;
        Ok((logical_len, buffer.into()))
    }

    pub fn read_nbytes<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut bytes: [u8; N] = [0; N];
        self.r.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub fn read_usize(&mut self) -> io::Result<usize> {
        leb128::read::unsigned(self.r)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
            .map(|u| u as usize)
    }

    pub fn read_option_tag(&mut self) -> io::Result<bool> {
        let mut tag = [0; 1];
        self.r.read_exact(&mut tag)?;
        Ok(tag[0] == 0x01)
    }

    pub fn read_optional_array(&mut self) -> io::Result<Option<ArrayRef>> {
        if self.read_option_tag()? {
            self.read().map(Some)
        } else {
            Ok(None)
        }
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
    available_encodings: Vec<&'static EncodingId>,
}

impl<'a> WriteCtx<'a> {
    pub fn new(w: &'a mut dyn Write) -> Self {
        let available_encodings = ENCODINGS.iter().map(|e| e.id()).collect::<Vec<_>>();
        Self {
            w,
            available_encodings,
        }
    }

    pub fn dtype(&mut self, dtype: &DType) -> io::Result<()> {
        DTypeWriter::new(self).write(dtype)
    }

    pub fn scalar(&mut self, scalar: &dyn Scalar) -> io::Result<()> {
        ScalarWriter::new(self).write(scalar)
    }

    pub fn write_usize(&mut self, u: usize) -> io::Result<()> {
        leb128::write::unsigned(self.w, u as u64).map(|_| ())
    }

    pub fn write_fixed_slice<const N: usize>(&mut self, slice: [u8; N]) -> io::Result<()> {
        self.w.write_all(&slice)
    }

    pub fn write_slice(&mut self, slice: &[u8]) -> io::Result<()> {
        self.write_usize(slice.len())?;
        self.w.write_all(slice)
    }

    pub fn write_buffer(&mut self, logical_len: usize, buf: &Buffer) -> io::Result<()> {
        self.write_usize(logical_len)?;
        self.w.write_all(buf.as_slice())
    }

    pub fn write_option_tag(&mut self, present: bool) -> io::Result<()> {
        self.w.write_all(&[if present { 0x01 } else { 0x00 }])
    }

    pub fn write_optional_array(&mut self, array: Option<&dyn Array>) -> io::Result<()> {
        self.write_option_tag(array.is_some())?;
        if let Some(array) = array {
            self.write(array)
        } else {
            Ok(())
        }
    }

    pub fn write(&mut self, array: &dyn Array) -> io::Result<()> {
        let encoding_id = self
            .available_encodings
            .iter()
            .position(|e| e.name() == array.encoding().id().name())
            .ok_or(io::Error::new(ErrorKind::InvalidInput, "unknown encoding"))?;
        self.write_usize(encoding_id)?;
        array.serde().write(self)
    }
}

#[cfg(test)]
pub mod test {
    use std::io;

    use crate::array::{Array, ArrayRef};
    use crate::serde::{ReadCtx, WriteCtx};

    pub fn roundtrip_array(array: &dyn Array) -> io::Result<ArrayRef> {
        let mut buf = Vec::<u8>::new();
        let mut write_ctx = WriteCtx::new(&mut buf);
        write_ctx.write(array)?;
        let mut read = buf.as_slice();
        let mut read_ctx = ReadCtx::new(array.dtype(), &mut read);
        read_ctx.read()
    }
}
