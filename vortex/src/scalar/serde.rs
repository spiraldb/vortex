use std::io;
use std::io::{ErrorKind, Read};

use half::f16;
use num_enum::{IntoPrimitive, TryFromPrimitive};

use crate::dtype::{DType, FloatWidth, IntWidth, Signedness, TimeUnit};
use crate::scalar::{
    BinaryScalar, BoolScalar, ListScalar, LocalTimeScalar, NullScalar, NullableScalar, PScalar,
    Scalar, ScalarRef, StructScalar, Utf8Scalar,
};
use crate::serde::{DTypeReader, TimeUnitTag, WriteCtx};

pub struct ScalarReader<'a> {
    reader: &'a mut dyn Read,
}

impl<'a> ScalarReader<'a> {
    pub fn new(reader: &'a mut dyn Read) -> Self {
        Self { reader }
    }

    fn read_nbytes<const N: usize>(&mut self) -> io::Result<[u8; N]> {
        let mut bytes: [u8; N] = [0; N];
        self.reader.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    pub fn read(&mut self) -> io::Result<ScalarRef> {
        let tag = ScalarTag::try_from(self.read_nbytes::<1>()?[0])
            .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
        match tag {
            ScalarTag::Binary => {
                let len = leb128::read::unsigned(self.reader)
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
                let mut value = Vec::<u8>::with_capacity(len as usize);
                self.reader.take(len).read_to_end(&mut value)?;
                Ok(BinaryScalar::new(value).boxed())
            }
            ScalarTag::Bool => Ok(BoolScalar::new(self.read_nbytes::<1>()?[0] != 0).boxed()),
            ScalarTag::F16 => {
                Ok(PScalar::F16(f16::from_le_bytes(self.read_nbytes::<2>()?)).boxed())
            }
            ScalarTag::F32 => {
                Ok(PScalar::F32(f32::from_le_bytes(self.read_nbytes::<4>()?)).boxed())
            }
            ScalarTag::F64 => {
                Ok(PScalar::F64(f64::from_le_bytes(self.read_nbytes::<8>()?)).boxed())
            }
            ScalarTag::I16 => {
                Ok(PScalar::I16(i16::from_le_bytes(self.read_nbytes::<2>()?)).boxed())
            }
            ScalarTag::I32 => {
                Ok(PScalar::I32(i32::from_le_bytes(self.read_nbytes::<4>()?)).boxed())
            }
            ScalarTag::I64 => {
                Ok(PScalar::I64(i64::from_le_bytes(self.read_nbytes::<8>()?)).boxed())
            }
            ScalarTag::I8 => Ok(PScalar::I8(i8::from_le_bytes(self.read_nbytes::<1>()?)).boxed()),
            ScalarTag::List => {
                let elems = leb128::read::unsigned(self.reader)
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
                if elems == 0 {
                    let dtype = DTypeReader::new(self.reader).read()?;
                    Ok(ListScalar::new(dtype, Vec::new()).boxed())
                } else {
                    let mut values = Vec::<ScalarRef>::with_capacity(elems as usize);
                    for value in values.iter_mut() {
                        *value = self.read()?;
                    }
                    Ok(ListScalar::new(values[0].dtype().clone(), values).boxed())
                }
            }
            ScalarTag::LocalTime => {
                let pscalar = self
                    .read()?
                    .into_any()
                    .downcast::<PScalar>()
                    .map_err(|_e| io::Error::new(ErrorKind::InvalidData, "invalid scalar"))?;
                let time_unit = TimeUnitTag::try_from(self.read_nbytes::<1>()?[0])
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))
                    .map(TimeUnit::from)?;

                Ok(LocalTimeScalar::new(*pscalar, time_unit).boxed())
            }
            ScalarTag::Null => Ok(NullScalar::new().boxed()),
            ScalarTag::Nullable => {
                let tag = self.read_nbytes::<1>()?[0];
                match tag {
                    0x00 => Ok(NullableScalar::none(DTypeReader::new(self.reader).read()?).boxed()),
                    0x01 => Ok(NullableScalar::some(self.read()?).boxed()),
                    _ => Err(io::Error::new(
                        ErrorKind::InvalidData,
                        "Invalid NullableScalar tag",
                    )),
                }
            }
            ScalarTag::Struct => {
                let dtype = DTypeReader::new(self.reader).read()?;
                let DType::Struct(ns, _fs) = &dtype else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid dtype"));
                };
                let mut values = Vec::<ScalarRef>::with_capacity(ns.len());
                for value in values.iter_mut() {
                    *value = self.read()?;
                }
                Ok(StructScalar::new(dtype, values).boxed())
            }
            ScalarTag::U16 => {
                Ok(PScalar::U16(u16::from_le_bytes(self.read_nbytes::<2>()?)).boxed())
            }
            ScalarTag::U32 => {
                Ok(PScalar::U32(u32::from_le_bytes(self.read_nbytes::<4>()?)).boxed())
            }
            ScalarTag::U64 => {
                Ok(PScalar::U64(u64::from_le_bytes(self.read_nbytes::<8>()?)).boxed())
            }
            ScalarTag::U8 => Ok(PScalar::U8(u8::from_le_bytes(self.read_nbytes::<1>()?)).boxed()),
            ScalarTag::Utf8 => {
                let len = leb128::read::unsigned(self.reader)
                    .map_err(|e| io::Error::new(ErrorKind::InvalidData, e))?;
                let mut value = Vec::<u8>::with_capacity(len as usize);
                self.reader.take(len).read_to_end(&mut value)?;
                Ok(Utf8Scalar::new(unsafe { String::from_utf8_unchecked(value) }).boxed())
            }
        }
    }
}

pub struct ScalarWriter<'a, 'b> {
    writer: &'b mut WriteCtx<'a>,
}

impl<'a, 'b> ScalarWriter<'a, 'b> {
    pub fn new(writer: &'b mut WriteCtx<'a>) -> Self {
        Self { writer }
    }

    pub fn write(&mut self, scalar: &dyn Scalar) -> io::Result<()> {
        let tag = ScalarTag::from(scalar);
        self.writer.write_fixed_slice([tag.into()])?;
        match tag {
            ScalarTag::Binary => {
                let binary = scalar.as_any().downcast_ref::<BinaryScalar>().unwrap();
                self.writer.write_slice(binary.value().as_slice())
            }
            ScalarTag::Bool => self.writer.write_fixed_slice([scalar
                .as_any()
                .downcast_ref::<BoolScalar>()
                .unwrap()
                .value() as u8]),
            ScalarTag::F16 => {
                let PScalar::F16(f) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(f.to_le_bytes())
            }
            ScalarTag::F32 => {
                let PScalar::F32(f) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(f.to_le_bytes())
            }
            ScalarTag::F64 => {
                let PScalar::F64(f) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(f.to_le_bytes())
            }
            ScalarTag::I16 => {
                let PScalar::I16(i) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(i.to_le_bytes())
            }
            ScalarTag::I32 => {
                let PScalar::I32(i) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(i.to_le_bytes())
            }
            ScalarTag::I64 => {
                let PScalar::I64(i) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(i.to_le_bytes())
            }
            ScalarTag::I8 => {
                let PScalar::I8(i) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(i.to_le_bytes())
            }
            ScalarTag::List => {
                let ls = scalar.as_any().downcast_ref::<ListScalar>().unwrap();
                self.writer.write_usize(ls.values().len())?;
                if ls.values().is_empty() {
                    self.writer.dtype(ls.dtype())?;
                    Ok(())
                } else {
                    for elem in ls.values() {
                        self.write(elem.as_ref())?;
                    }
                    Ok(())
                }
            }
            ScalarTag::LocalTime => {
                let lt = scalar.as_any().downcast_ref::<LocalTimeScalar>().unwrap();
                self.write(lt.value())?;
                self.writer
                    .write_fixed_slice([TimeUnitTag::from(lt.time_unit()).into()])
            }
            ScalarTag::Null => Ok(()),
            ScalarTag::Nullable => {
                let ns = scalar.as_any().downcast_ref::<NullableScalar>().unwrap();
                self.writer
                    .write_option_tag(matches!(ns, NullableScalar::Some(_, _)))?;
                match ns {
                    NullableScalar::None(d) => self.writer.dtype(d),
                    NullableScalar::Some(s, _) => self.write(s.as_ref()),
                }
            }
            ScalarTag::Struct => {
                let s = scalar.as_any().downcast_ref::<StructScalar>().unwrap();
                self.writer.dtype(s.dtype())?;
                for field in s.values() {
                    self.write(field.as_ref())?;
                }
                Ok(())
            }
            ScalarTag::U16 => {
                let PScalar::U16(u) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(u.to_le_bytes())
            }
            ScalarTag::U32 => {
                let PScalar::U32(u) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(u.to_le_bytes())
            }
            ScalarTag::U64 => {
                let PScalar::U64(u) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(u.to_le_bytes())
            }
            ScalarTag::U8 => {
                let PScalar::U8(u) = scalar.as_any().downcast_ref::<PScalar>().unwrap() else {
                    return Err(io::Error::new(ErrorKind::InvalidData, "invalid scalar"));
                };
                self.writer.write_fixed_slice(u.to_le_bytes())
            }
            ScalarTag::Utf8 => {
                let utf8 = scalar.as_any().downcast_ref::<Utf8Scalar>().unwrap();
                self.writer.write_slice(utf8.value().as_bytes())
            }
        }
    }
}

#[derive(Copy, Clone, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum ScalarTag {
    Binary,
    Bool,
    F16,
    F32,
    F64,
    I16,
    I32,
    I64,
    I8,
    List,
    LocalTime,
    Null,
    Nullable,
    Struct,
    U16,
    U32,
    U64,
    U8,
    Utf8,
}

impl From<&dyn Scalar> for ScalarTag {
    fn from(value: &dyn Scalar) -> Self {
        if value.dtype().is_nullable() {
            return ScalarTag::Nullable;
        }

        match value.dtype() {
            DType::Null => ScalarTag::Null,
            DType::Bool(_) => ScalarTag::Bool,
            DType::Int(w, s, _) => match (w, s) {
                (IntWidth::Unknown, Signedness::Unknown | Signedness::Signed) => ScalarTag::I64,
                (IntWidth::_8, Signedness::Unknown | Signedness::Signed) => ScalarTag::I8,
                (IntWidth::_16, Signedness::Unknown | Signedness::Signed) => ScalarTag::I16,
                (IntWidth::_32, Signedness::Unknown | Signedness::Signed) => ScalarTag::I32,
                (IntWidth::_64, Signedness::Unknown | Signedness::Signed) => ScalarTag::I64,
                (IntWidth::Unknown, Signedness::Unsigned) => ScalarTag::U64,
                (IntWidth::_8, Signedness::Unsigned) => ScalarTag::U8,
                (IntWidth::_16, Signedness::Unsigned) => ScalarTag::U16,
                (IntWidth::_32, Signedness::Unsigned) => ScalarTag::U32,
                (IntWidth::_64, Signedness::Unsigned) => ScalarTag::U64,
            },
            DType::Decimal(_, _, _) => unimplemented!("decimal scalar"),
            DType::Float(w, _) => match w {
                FloatWidth::Unknown => ScalarTag::F64,
                FloatWidth::_16 => ScalarTag::F16,
                FloatWidth::_32 => ScalarTag::F32,
                FloatWidth::_64 => ScalarTag::F64,
            },
            DType::Utf8(_) => ScalarTag::Utf8,
            DType::Binary(_) => ScalarTag::Binary,
            DType::LocalTime(_, _) => ScalarTag::LocalTime,
            DType::LocalDate(_) => unimplemented!("local date"),
            DType::Instant(_, _) => unimplemented!("instant scalar"),
            DType::ZonedDateTime(_, _) => unimplemented!("zoned date time scalar"),
            DType::Struct(_, _) => ScalarTag::Struct,
            DType::List(_, _) => ScalarTag::List,
            DType::Map(_, _, _) => unimplemented!("map scalar"),
        }
    }
}
