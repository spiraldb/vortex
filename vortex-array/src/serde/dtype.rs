use leb128::read::Error;
use std::io::Read;
use std::sync::Arc;

use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::dtype::DType::*;
use crate::dtype::{DType, FloatWidth, IntWidth, Nullability, Signedness};
use crate::error::{VortexError, VortexResult};
use crate::serde::WriteCtx;

pub struct DTypeReader<'a> {
    reader: &'a mut dyn Read,
}

impl<'a> DTypeReader<'a> {
    pub fn new(reader: &'a mut dyn Read) -> Self {
        Self { reader }
    }

    fn read_nbytes<const N: usize>(&mut self) -> VortexResult<[u8; N]> {
        let mut bytes: [u8; N] = [0; N];
        self.reader.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    fn read_usize(&mut self) -> VortexResult<usize> {
        leb128::read::unsigned(self.reader)
            .map_err(|e| match e {
                Error::IoError(io_err) => io_err.into(),
                Error::Overflow => VortexError::InvalidArgument("overflow".into()),
            })
            .map(|u| u as usize)
    }

    fn read_slice(&mut self) -> VortexResult<Vec<u8>> {
        let len = self.read_usize()?;
        let mut slice = Vec::with_capacity(len);
        self.reader
            .take(len as u64)
            .read_to_end(&mut slice)
            .map_err(VortexError::from)?;
        Ok(slice)
    }

    pub fn read(&mut self) -> VortexResult<DType> {
        let dtype = DTypeTag::try_from(self.read_nbytes::<1>()?[0])
            .map_err(|_| VortexError::InvalidArgument("Failed to parse dtype tag".into()))?;
        match dtype {
            DTypeTag::Null => Ok(Null),
            DTypeTag::Bool => Ok(Bool(self.read_nullability()?)),
            DTypeTag::Int => {
                let nullability = self.read_nullability()?;
                Ok(Int(
                    self.read_int_width()?,
                    self.read_signedness()?,
                    nullability,
                ))
            }
            DTypeTag::Float => {
                let nullability = self.read_nullability()?;
                Ok(Float(self.read_float_width()?, nullability))
            }
            DTypeTag::Utf8 => Ok(Utf8(self.read_nullability()?)),
            DTypeTag::Binary => Ok(Binary(self.read_nullability()?)),
            DTypeTag::Decimal => {
                let nullability = self.read_nullability()?;
                let precision_scale: [u8; 2] = self.read_nbytes()?;
                Ok(Decimal(
                    precision_scale[0],
                    precision_scale[1] as i8,
                    nullability,
                ))
            }
            DTypeTag::List => {
                let nullability = self.read_nullability()?;
                Ok(List(Box::new(self.read()?), nullability))
            }
            DTypeTag::Struct => {
                let field_num = self.read_usize()?;
                let mut names = Vec::with_capacity(field_num);
                for _ in 0..field_num {
                    let name = unsafe { String::from_utf8_unchecked(self.read_slice()?) };
                    names.push(Arc::new(name));
                }

                let mut fields = Vec::with_capacity(field_num);
                for _ in 0..field_num {
                    fields.push(self.read()?);
                }
                Ok(Struct(names, fields))
            }
            DTypeTag::Composite => {
                let name = unsafe { String::from_utf8_unchecked(self.read_slice()?) };
                let dtype = self.read()?;
                let metadata = self.read_slice()?;
                Ok(Composite(Arc::new(name), Box::new(dtype), metadata))
            }
        }
    }

    fn read_signedness(&mut self) -> VortexResult<Signedness> {
        SignednessTag::try_from(self.read_nbytes::<1>()?[0])
            .map_err(|_| VortexError::InvalidArgument("Failed to parse signedness tag".into()))
            .map(Signedness::from)
    }

    fn read_nullability(&mut self) -> VortexResult<Nullability> {
        NullabilityTag::try_from(self.read_nbytes::<1>()?[0])
            .map_err(|_| VortexError::InvalidArgument("Failed to parse nullability tag".into()))
            .map(Nullability::from)
    }

    fn read_int_width(&mut self) -> VortexResult<IntWidth> {
        IntWidthTag::try_from(self.read_nbytes::<1>()?[0])
            .map_err(|_| VortexError::InvalidArgument("Failed to parse int width tag".into()))
            .map(IntWidth::from)
    }

    fn read_float_width(&mut self) -> VortexResult<FloatWidth> {
        FloatWidthTag::try_from(self.read_nbytes::<1>()?[0])
            .map_err(|_| VortexError::InvalidArgument("Failed to parse float width tag".into()))
            .map(FloatWidth::from)
    }
}

pub struct DTypeWriter<'a, 'b> {
    writer: &'b mut WriteCtx<'a>,
}

impl<'a, 'b> DTypeWriter<'a, 'b> {
    pub fn new(writer: &'b mut WriteCtx<'a>) -> Self {
        Self { writer }
    }

    pub fn write(&mut self, dtype: &DType) -> VortexResult<()> {
        self.writer
            .write_fixed_slice([DTypeTag::from(dtype).into()])?;
        match dtype {
            Null => {}
            Bool(n) => self.write_nullability(*n)?,
            Int(w, s, n) => {
                self.write_nullability(*n)?;
                self.write_int_width(*w)?;
                self.write_signedness(*s)?
            }
            Decimal(p, w, n) => {
                self.write_nullability(*n)?;
                self.writer.write_fixed_slice([*p, *w as u8])?
            }
            Float(w, n) => {
                self.write_nullability(*n)?;
                self.write_float_width(*w)?
            }
            Utf8(n) => self.write_nullability(*n)?,
            Binary(n) => self.write_nullability(*n)?,
            Struct(ns, fs) => {
                self.writer.write_usize(ns.len())?;
                for name in ns {
                    self.writer.write_slice(name.as_bytes())?;
                }
                for field in fs {
                    self.write(field)?
                }
            }
            List(e, n) => {
                self.write_nullability(*n)?;
                self.write(e.as_ref())?
            }
            Composite(n, d, m) => {
                self.writer.write_slice(n.as_bytes())?;
                self.writer.dtype(d)?;
                self.writer.write_slice(m)?
            }
        }

        Ok(())
    }

    fn write_signedness(&mut self, signedness: Signedness) -> VortexResult<()> {
        self.writer
            .write_fixed_slice([SignednessTag::from(signedness).into()])
    }

    fn write_nullability(&mut self, nullability: Nullability) -> VortexResult<()> {
        self.writer
            .write_fixed_slice([NullabilityTag::from(nullability).into()])
    }

    fn write_int_width(&mut self, int_width: IntWidth) -> VortexResult<()> {
        self.writer
            .write_fixed_slice([IntWidthTag::from(int_width).into()])
    }

    fn write_float_width(&mut self, float_width: FloatWidth) -> VortexResult<()> {
        self.writer
            .write_fixed_slice([FloatWidthTag::from(float_width).into()])
    }
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum DTypeTag {
    Null,
    Bool,
    Int,
    Float,
    Utf8,
    Binary,
    Decimal,
    List,
    Struct,
    Composite,
}

impl From<&DType> for DTypeTag {
    fn from(value: &DType) -> Self {
        match value {
            Null => DTypeTag::Null,
            Bool(_) => DTypeTag::Bool,
            Int(_, _, _) => DTypeTag::Int,
            Float(_, _) => DTypeTag::Float,
            Utf8(_) => DTypeTag::Utf8,
            Binary(_) => DTypeTag::Binary,
            Decimal(_, _, _) => DTypeTag::Decimal,
            List(_, _) => DTypeTag::List,
            Struct(_, _) => DTypeTag::Struct,
            Composite(_, _, _) => DTypeTag::Composite,
        }
    }
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum NullabilityTag {
    Nullable,
    NonNullable,
}

impl From<Nullability> for NullabilityTag {
    fn from(value: Nullability) -> Self {
        use Nullability::*;
        match value {
            NonNullable => NullabilityTag::NonNullable,
            Nullable => NullabilityTag::Nullable,
        }
    }
}

impl From<NullabilityTag> for Nullability {
    fn from(value: NullabilityTag) -> Self {
        use Nullability::*;
        match value {
            NullabilityTag::Nullable => Nullable,
            NullabilityTag::NonNullable => NonNullable,
        }
    }
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum SignednessTag {
    Unknown,
    Unsigned,
    Signed,
}

impl From<Signedness> for SignednessTag {
    fn from(value: Signedness) -> Self {
        use Signedness::*;
        match value {
            Unknown => SignednessTag::Unknown,
            Unsigned => SignednessTag::Unsigned,
            Signed => SignednessTag::Signed,
        }
    }
}

impl From<SignednessTag> for Signedness {
    fn from(value: SignednessTag) -> Self {
        use Signedness::*;
        match value {
            SignednessTag::Unknown => Unknown,
            SignednessTag::Unsigned => Unsigned,
            SignednessTag::Signed => Signed,
        }
    }
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum FloatWidthTag {
    Unknown,
    _16,
    _32,
    _64,
}

#[allow(clippy::just_underscores_and_digits)]
impl From<FloatWidth> for FloatWidthTag {
    fn from(value: FloatWidth) -> Self {
        use FloatWidth::*;
        match value {
            Unknown => FloatWidthTag::Unknown,
            _16 => FloatWidthTag::_16,
            _32 => FloatWidthTag::_32,
            _64 => FloatWidthTag::_64,
        }
    }
}

impl From<FloatWidthTag> for FloatWidth {
    fn from(value: FloatWidthTag) -> Self {
        use FloatWidth::*;
        match value {
            FloatWidthTag::Unknown => Unknown,
            FloatWidthTag::_16 => _16,
            FloatWidthTag::_32 => _32,
            FloatWidthTag::_64 => _64,
        }
    }
}

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum IntWidthTag {
    Unknown,
    _8,
    _16,
    _32,
    _64,
}

#[allow(clippy::just_underscores_and_digits)]
impl From<IntWidth> for IntWidthTag {
    fn from(value: IntWidth) -> Self {
        use IntWidth::*;
        match value {
            Unknown => IntWidthTag::Unknown,
            _8 => IntWidthTag::_8,
            _16 => IntWidthTag::_16,
            _32 => IntWidthTag::_32,
            _64 => IntWidthTag::_64,
        }
    }
}

impl From<IntWidthTag> for IntWidth {
    fn from(value: IntWidthTag) -> Self {
        use IntWidth::*;
        match value {
            IntWidthTag::Unknown => Unknown,
            IntWidthTag::_8 => _8,
            IntWidthTag::_16 => _16,
            IntWidthTag::_32 => _32,
            IntWidthTag::_64 => _64,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::dtype::DType::Int;
    use crate::dtype::IntWidth::_64;
    use crate::dtype::Nullability::NonNullable;
    use crate::dtype::Signedness::Unsigned;
    use crate::serde::{DTypeReader, WriteCtx};

    #[test]
    fn roundtrip() {
        let mut buffer: Vec<u8> = Vec::new();
        let dtype = Int(_64, Unsigned, NonNullable);
        let mut ctx = WriteCtx::new(&mut buffer);
        ctx.dtype(&dtype).unwrap();
        assert_eq!(buffer, [0x02, 0x01, 0x04, 0x01]);
        let read_dtype = DTypeReader::new(&mut buffer.as_slice()).read().unwrap();
        assert_eq!(dtype, read_dtype);
    }
}
