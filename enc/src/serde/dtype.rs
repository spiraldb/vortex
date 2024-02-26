use std::io;
use std::io::{ErrorKind, Read};
use std::sync::Arc;

use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::dtype::DType::*;
use crate::dtype::{DType, FloatWidth, IntWidth, Nullability, Signedness, TimeUnit};
use crate::serde::WriteCtx;

pub struct DTypeReader<'a> {
    reader: &'a mut dyn Read,
}

impl<'a> DTypeReader<'a> {
    pub fn new(reader: &'a mut dyn Read) -> Self {
        Self { reader }
    }

    fn read_byte(&mut self) -> io::Result<u8> {
        let mut buf: [u8; 1] = [0; 1];
        self.reader.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    pub fn read(&mut self) -> io::Result<DType> {
        let dtype = DTypeTag::try_from(self.read_byte()?)
            .map_err(|e| io::Error::new(ErrorKind::InvalidInput, e))?;
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
                let mut precision_scale: [u8; 2] = [0; 2];
                self.reader.read_exact(&mut precision_scale)?;
                Ok(Decimal(
                    precision_scale[0],
                    precision_scale[1] as i8,
                    nullability,
                ))
            }
            DTypeTag::LocalTime => {
                let nullability = self.read_nullability()?;
                Ok(LocalTime(self.read_time_unit()?, nullability))
            }
            DTypeTag::LocalDate => Ok(LocalDate(self.read_nullability()?)),
            DTypeTag::Instant => {
                let nullability = self.read_nullability()?;
                Ok(Instant(self.read_time_unit()?, nullability))
            }
            DTypeTag::ZonedDateTime => {
                let nullability = self.read_nullability()?;
                Ok(ZonedDateTime(self.read_time_unit()?, nullability))
            }
            DTypeTag::List => {
                let nullability = self.read_nullability()?;
                Ok(List(Box::new(self.read()?), nullability))
            }
            DTypeTag::Map => {
                let nullability = self.read_nullability()?;
                Ok(Map(
                    Box::new(self.read()?),
                    Box::new(self.read()?),
                    nullability,
                ))
            }
            DTypeTag::Struct => {
                let field_num = leb128::read::unsigned(self.reader)
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
                let mut names = Vec::<Arc<String>>::with_capacity(field_num as usize);
                for _ in 0..field_num {
                    let len = leb128::read::unsigned(self.reader)
                        .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
                    let mut name = String::with_capacity(len as usize);
                    self.reader.take(len).read_to_string(&mut name)?;
                    names.push(Arc::new(name));
                }

                let mut fields = Vec::<DType>::with_capacity(field_num as usize);
                for _ in 0..field_num {
                    fields.push(self.read()?);
                }
                Ok(Struct(names, fields))
            }
        }
    }

    fn read_signedness(&mut self) -> io::Result<Signedness> {
        SignednessTag::try_from(self.read_byte()?)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
            .map(Signedness::from)
    }

    fn read_nullability(&mut self) -> io::Result<Nullability> {
        NullabilityTag::try_from(self.read_byte()?)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
            .map(Nullability::from)
    }

    fn read_int_width(&mut self) -> io::Result<IntWidth> {
        IntWidthTag::try_from(self.read_byte()?)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
            .map(IntWidth::from)
    }

    fn read_float_width(&mut self) -> io::Result<FloatWidth> {
        FloatWidthTag::try_from(self.read_byte()?)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
            .map(FloatWidth::from)
    }

    fn read_time_unit(&mut self) -> io::Result<TimeUnit> {
        TimeUnitTag::try_from(self.read_byte()?)
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
            .map(TimeUnit::from)
    }
}

pub struct DTypeWriter<'a, 'b> {
    writer: &'b mut WriteCtx<'a>,
}

impl<'a, 'b> DTypeWriter<'a, 'b> {
    pub fn new(writer: &'b mut WriteCtx<'a>) -> Self {
        Self { writer }
    }

    pub fn write(&mut self, dtype: &DType) -> io::Result<()> {
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
            LocalTime(u, n) => {
                self.write_nullability(*n)?;
                self.write_time_unit(*u)?
            }
            LocalDate(n) => self.write_nullability(*n)?,
            Instant(u, n) => {
                self.write_nullability(*n)?;
                self.write_time_unit(*u)?
            }
            ZonedDateTime(u, n) => {
                self.write_nullability(*n)?;
                self.write_time_unit(*u)?
            }
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
            Map(k, v, n) => {
                self.write_nullability(*n)?;
                self.write(k.as_ref())?;
                self.write(v.as_ref())?
            }
        }

        Ok(())
    }

    fn write_signedness(&mut self, signedness: Signedness) -> io::Result<()> {
        self.writer
            .write_fixed_slice([SignednessTag::from(signedness).into()])
    }

    fn write_nullability(&mut self, nullability: Nullability) -> io::Result<()> {
        self.writer
            .write_fixed_slice([NullabilityTag::from(nullability).into()])
    }

    fn write_int_width(&mut self, int_width: IntWidth) -> io::Result<()> {
        self.writer
            .write_fixed_slice([IntWidthTag::from(int_width).into()])
    }

    fn write_float_width(&mut self, float_width: FloatWidth) -> io::Result<()> {
        self.writer
            .write_fixed_slice([FloatWidthTag::from(float_width).into()])
    }

    fn write_time_unit(&mut self, time_unit: TimeUnit) -> io::Result<()> {
        self.writer
            .write_fixed_slice([TimeUnitTag::from(time_unit).into()])
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
    LocalTime,
    LocalDate,
    Instant,
    ZonedDateTime,
    List,
    Map,
    Struct,
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
            LocalTime(_, _) => DTypeTag::LocalTime,
            LocalDate(_) => DTypeTag::LocalDate,
            Instant(_, _) => DTypeTag::Instant,
            ZonedDateTime(_, _) => DTypeTag::ZonedDateTime,
            List(_, _) => DTypeTag::List,
            Map(_, _, _) => DTypeTag::Map,
            Struct(_, _) => DTypeTag::Struct,
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

#[derive(IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
pub enum TimeUnitTag {
    Ns,
    Us,
    Ms,
    S,
}

impl From<TimeUnit> for TimeUnitTag {
    fn from(value: TimeUnit) -> Self {
        use TimeUnit::*;
        match value {
            Ns => TimeUnitTag::Ns,
            Us => TimeUnitTag::Us,
            Ms => TimeUnitTag::Ms,
            S => TimeUnitTag::S,
        }
    }
}

impl From<TimeUnitTag> for TimeUnit {
    fn from(value: TimeUnitTag) -> Self {
        use TimeUnit::*;
        match value {
            TimeUnitTag::Ns => Ns,
            TimeUnitTag::Us => Us,
            TimeUnitTag::Ms => Ms,
            TimeUnitTag::S => S,
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
