use flatbuffers::{root, FlatBufferBuilder};
use std::io;
use std::sync::Arc;

use half::f16;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use vortex_schema::DType;

use crate::error::{VortexError, VortexResult};
use crate::flatbuffers::scalar;
use crate::flatbuffers::scalar::{
    Binary, BinaryArgs, Bool, BoolArgs, PrimitiveArgs, ScalarArgs, Type,
};
use crate::ptype::PType;
use crate::scalar::composite::CompositeScalar;
use crate::scalar::{
    BinaryScalar, BoolScalar, ListScalar, NullScalar, PScalar, PrimitiveScalar, Scalar,
    StructScalar, Utf8Scalar,
};
use crate::serde::{ReadCtx, WriteCtx};

pub struct ScalarReader<'a, 'b> {
    reader: &'b mut ReadCtx<'a>,
}

impl<'a, 'b> ScalarReader<'a, 'b> {
    pub fn new(reader: &'b mut ReadCtx<'a>) -> Self {
        Self { reader }
    }

    pub fn read(&mut self) -> VortexResult<Scalar> {
        let bytes = self.reader.read_slice()?;
        let scalar = root::<scalar::Scalar>(&bytes)
            .map_err(|_e| VortexError::InvalidArgument("Invalid FlatBuffer".into()))
            .unwrap();

        match scalar.type_type() {
            Type::Binary => todo!(),
            Type::Bool => todo!(),
            Type::List => todo!(),
            Type::Null => todo!(),
            Type::Primitive => {
                let primitive = scalar.type__as_primitive().unwrap();
                match primitive.ptype() {
                    scalar::PType::U8 => primitive
                        .bytes()
                        .map(|b| {
                            u8::from_le_bytes(
                                b.bytes().try_into().expect("slice with incorrect length"),
                            )
                        })
                        .map(|v| PScalar::U8(v))
                        .map(|ps| PrimitiveScalar::new(PType::U8, Some(ps)))
                        .unwrap_or_else(|| PrimitiveScalar::new(PType::U8, None)),
                    _ => panic!("Unsupported ptype"),
                }
            }
            Type::Struct_ => todo!(),
            Type::UTF8 => todo!(),
            Type::Composite => todo!(),
            _ => {
                panic!("Unsupported scalar type")
            }
        };

        let tag = ScalarTag::try_from(self.reader.read_nbytes::<1>()?[0])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        match tag {
            ScalarTag::Binary => {
                let slice = self.reader.read_optional_slice()?;
                Ok(BinaryScalar::new(slice).into())
            }
            ScalarTag::Bool => {
                let is_present = self.reader.read_option_tag()?;
                if is_present {
                    Ok(BoolScalar::some(self.reader.read_nbytes::<1>()?[0] != 0).into())
                } else {
                    Ok(BoolScalar::none().into())
                }
            }
            ScalarTag::PrimitiveS => self.read_primitive_scalar().map(|p| p.into()),
            ScalarTag::List => {
                let is_present = self.reader.read_option_tag()?;
                if is_present {
                    let elems = self.reader.read_usize()?;
                    let mut values = Vec::with_capacity(elems);
                    for _ in 0..elems {
                        values.push(self.read()?);
                    }
                    Ok(ListScalar::new(values[0].dtype().clone(), Some(values)).into())
                } else {
                    Ok(ListScalar::new(self.reader.dtype()?, None).into())
                }
            }
            ScalarTag::Null => Ok(NullScalar::new().into()),
            ScalarTag::Struct => {
                let field_num = self.reader.read_usize()?;
                let mut names = Vec::with_capacity(field_num);
                for _ in 0..field_num {
                    names.push(Arc::new(
                        self.reader
                            .read_slice()
                            .map(|v| unsafe { String::from_utf8_unchecked(v) })?,
                    ));
                }
                let mut values = Vec::with_capacity(field_num);
                for _ in 0..field_num {
                    values.push(self.read()?);
                }
                let dtypes = values.iter().map(|s| s.dtype().clone()).collect::<Vec<_>>();
                Ok(StructScalar::new(DType::Struct(names, dtypes), values).into())
            }
            ScalarTag::Utf8 => {
                let value = self.reader.read_optional_slice()?;
                Ok(
                    Utf8Scalar::new(value.map(|v| unsafe { String::from_utf8_unchecked(v) }))
                        .into(),
                )
            }
            ScalarTag::Composite => {
                let dtype = self.reader.dtype()?;
                let scalar = self.read()?;
                Ok(CompositeScalar::new(dtype, Box::new(scalar)).into())
            }
        }
    }

    fn read_primitive_scalar(&mut self) -> VortexResult<PrimitiveScalar> {
        let ptype = self.reader.ptype()?;
        let is_present = self.reader.read_option_tag()?;
        if is_present {
            let pscalar = match ptype {
                PType::U8 => PrimitiveScalar::some(PScalar::U8(u8::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::U16 => PrimitiveScalar::some(PScalar::U16(u16::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::U32 => PrimitiveScalar::some(PScalar::U32(u32::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::U64 => PrimitiveScalar::some(PScalar::U64(u64::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::I8 => PrimitiveScalar::some(PScalar::I8(i8::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::I16 => PrimitiveScalar::some(PScalar::I16(i16::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::I32 => PrimitiveScalar::some(PScalar::I32(i32::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::I64 => PrimitiveScalar::some(PScalar::I64(i64::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::F16 => PrimitiveScalar::some(PScalar::F16(f16::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::F32 => PrimitiveScalar::some(PScalar::F32(f32::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
                PType::F64 => PrimitiveScalar::some(PScalar::F64(f64::from_le_bytes(
                    self.reader.read_nbytes()?,
                ))),
            };
            Ok(pscalar)
        } else {
            Ok(PrimitiveScalar::none(ptype))
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

    pub fn write(&mut self, scalar: &Scalar) -> VortexResult<()> {
        let mut fbb = FlatBufferBuilder::new();

        self.writer
            .write_fixed_slice([ScalarTag::from(scalar).into()])?;
        let union = match scalar {
            Scalar::Binary(b) => {
                let bytes = b.value().map(|bytes| fbb.create_vector(bytes));
                ScalarArgs {
                    type_type: Type::Binary,
                    type_: bytes.map(|b| {
                        Binary::create(&mut fbb, &BinaryArgs { value: Some(b) }).as_union_value()
                    }),
                }
            }
            Scalar::Bool(b) => ScalarArgs {
                type_type: Type::Bool,
                type_: b
                    .value()
                    .map(|value| Bool::create(&mut fbb, &BoolArgs { value }).as_union_value()),
            },
            Scalar::List(_) => panic!(),
            Scalar::Null(_) => ScalarArgs {
                type_type: Type::Null,
                type_: None,
            },
            Scalar::Primitive(p) => {
                // TODO(ngates): clean this up.
                // We could probably just keep PScalar as PType + Option<[u8]> internally too?
                let primitive = p
                    .value()
                    .map(|pscalar| match pscalar {
                        PScalar::U8(v) => {
                            let pbytes = fbb.create_vector(&v.to_le_bytes());
                            PrimitiveArgs {
                                ptype: scalar::PType::U8,
                                bytes: Some(pbytes),
                            }
                        }
                        _ => panic!(),
                    })
                    .unwrap_or_else(|| match p.ptype() {
                        PType::U8 => PrimitiveArgs {
                            ptype: scalar::PType::U8,
                            bytes: None,
                        },
                        PType::U16 => PrimitiveArgs {
                            ptype: scalar::PType::U16,
                            bytes: None,
                        },
                        PType::U32 => PrimitiveArgs {
                            ptype: scalar::PType::U32,
                            bytes: None,
                        },
                        PType::U64 => PrimitiveArgs {
                            ptype: scalar::PType::U64,
                            bytes: None,
                        },
                        PType::I8 => PrimitiveArgs {
                            ptype: scalar::PType::I8,
                            bytes: None,
                        },
                        PType::I16 => PrimitiveArgs {
                            ptype: scalar::PType::I16,
                            bytes: None,
                        },
                        PType::I32 => PrimitiveArgs {
                            ptype: scalar::PType::I32,
                            bytes: None,
                        },
                        PType::I64 => PrimitiveArgs {
                            ptype: scalar::PType::I64,
                            bytes: None,
                        },
                        PType::F16 => PrimitiveArgs {
                            ptype: scalar::PType::F16,
                            bytes: None,
                        },
                        PType::F32 => PrimitiveArgs {
                            ptype: scalar::PType::F32,
                            bytes: None,
                        },
                        PType::F64 => PrimitiveArgs {
                            ptype: scalar::PType::F64,
                            bytes: None,
                        },
                    });

                let primitive = scalar::Primitive::create(&mut fbb, &primitive);
                ScalarArgs {
                    type_type: Type::Primitive,
                    type_: Some(primitive.as_union_value()),
                }
            }
            Scalar::Struct(_) => panic!(),
            Scalar::Utf8(_) => panic!(),
            Scalar::Composite(_) => panic!(),
        };

        let scalar = scalar::Scalar::create(&mut fbb, &union);
        fbb.finish_minimal(scalar);
        let (vec, offset) = fbb.collapse();
        self.writer.write_slice(&vec[offset..])
    }
}

#[derive(Copy, Clone, IntoPrimitive, TryFromPrimitive)]
#[repr(u8)]
enum ScalarTag {
    Binary,
    Bool,
    List,
    Null,
    // TODO(robert): rename to primitive once we stop using enum for serialization
    PrimitiveS,
    Struct,
    Utf8,
    Composite,
}

impl From<&Scalar> for ScalarTag {
    fn from(value: &Scalar) -> Self {
        match value {
            Scalar::Binary(_) => ScalarTag::Binary,
            Scalar::Bool(_) => ScalarTag::Bool,
            Scalar::List(_) => ScalarTag::List,
            Scalar::Null(_) => ScalarTag::Null,
            Scalar::Primitive(_) => ScalarTag::PrimitiveS,
            Scalar::Struct(_) => ScalarTag::Struct,
            Scalar::Utf8(_) => ScalarTag::Utf8,
            Scalar::Composite(_) => ScalarTag::Composite,
        }
    }
}
