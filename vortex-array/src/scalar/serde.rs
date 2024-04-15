use flatbuffers::{FlatBufferBuilder, WIPOffset};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vortex_error::VortexError;
use vortex_flatbuffers::{ReadFlatBuffer, WriteFlatBuffer};
use vortex_schema::DTypeSerdeContext;

use crate::flatbuffers::scalar as fb;
use crate::ptype::PType;
use crate::scalar::{PScalar, Scalar};

impl WriteFlatBuffer for Scalar {
    type Target<'a> = fb::Scalar<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let union = match self {
            Scalar::Binary(b) => {
                let bytes = b.value().map(|bytes| fbb.create_vector(bytes));
                fb::ScalarArgs {
                    type_type: fb::Type::Binary,
                    type_: bytes.map(|b| {
                        fb::Binary::create(fbb, &fb::BinaryArgs { value: Some(b) }).as_union_value()
                    }),
                }
            }
            Scalar::Bool(b) => fb::ScalarArgs {
                type_type: fb::Type::Bool,
                type_: b
                    .value()
                    .map(|&value| fb::Bool::create(fbb, &fb::BoolArgs { value }).as_union_value()),
            },
            Scalar::List(_) => panic!(),
            Scalar::Null(_) => fb::ScalarArgs {
                type_type: fb::Type::Null,
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
                            fb::PrimitiveArgs {
                                ptype: fb::PType::U8,
                                bytes: Some(pbytes),
                            }
                        }
                        _ => panic!(),
                    })
                    .unwrap_or_else(|| match p.ptype() {
                        PType::U8 => fb::PrimitiveArgs {
                            ptype: fb::PType::U8,
                            bytes: None,
                        },
                        PType::U16 => fb::PrimitiveArgs {
                            ptype: fb::PType::U16,
                            bytes: None,
                        },
                        PType::U32 => fb::PrimitiveArgs {
                            ptype: fb::PType::U32,
                            bytes: None,
                        },
                        PType::U64 => fb::PrimitiveArgs {
                            ptype: fb::PType::U64,
                            bytes: None,
                        },
                        PType::I8 => fb::PrimitiveArgs {
                            ptype: fb::PType::I8,
                            bytes: None,
                        },
                        PType::I16 => fb::PrimitiveArgs {
                            ptype: fb::PType::I16,
                            bytes: None,
                        },
                        PType::I32 => fb::PrimitiveArgs {
                            ptype: fb::PType::I32,
                            bytes: None,
                        },
                        PType::I64 => fb::PrimitiveArgs {
                            ptype: fb::PType::I64,
                            bytes: None,
                        },
                        PType::F16 => fb::PrimitiveArgs {
                            ptype: fb::PType::F16,
                            bytes: None,
                        },
                        PType::F32 => fb::PrimitiveArgs {
                            ptype: fb::PType::F32,
                            bytes: None,
                        },
                        PType::F64 => fb::PrimitiveArgs {
                            ptype: fb::PType::F64,
                            bytes: None,
                        },
                    });

                let primitive = fb::Primitive::create(fbb, &primitive);
                fb::ScalarArgs {
                    type_type: fb::Type::Primitive,
                    type_: Some(primitive.as_union_value()),
                }
            }
            Scalar::Struct(_) => panic!(),
            Scalar::Utf8(_) => panic!(),
            Scalar::Composite(_) => panic!(),
        };

        fb::Scalar::create(fbb, &union)
    }
}

impl ReadFlatBuffer<DTypeSerdeContext> for Scalar {
    type Source<'a> = fb::Scalar<'a>;
    type Error = VortexError;

    fn read_flatbuffer(
        _ctx: &DTypeSerdeContext,
        _fb: &Self::Source<'_>,
    ) -> Result<Self, Self::Error> {
        todo!()
    }
}

impl Serialize for Scalar {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        todo!()
    }
}

impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D>(_deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!()
    }
}

// impl<'a, 'b> ScalarReader<'a, 'b> {
//     pub fn read(&mut self) -> VortexResult<Scalar> {
//         let bytes = self.reader.read_slice()?;
//         let scalar = root::<scalar::Scalar>(&bytes)
//             .map_err(|_e| VortexError::InvalidArgument("Invalid FlatBuffer".into()))
//             .unwrap();

//     }
//
//     fn read_primitive_scalar(&mut self) -> VortexResult<PrimitiveScalar> {
//         let ptype = self.reader.ptype()?;
//         let is_present = self.reader.read_option_tag()?;
//         if is_present {
//             let pscalar = match ptype {
//                 PType::U8 => PrimitiveScalar::some(PScalar::U8(u8::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::U16 => PrimitiveScalar::some(PScalar::U16(u16::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::U32 => PrimitiveScalar::some(PScalar::U32(u32::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::U64 => PrimitiveScalar::some(PScalar::U64(u64::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::I8 => PrimitiveScalar::some(PScalar::I8(i8::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::I16 => PrimitiveScalar::some(PScalar::I16(i16::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::I32 => PrimitiveScalar::some(PScalar::I32(i32::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::I64 => PrimitiveScalar::some(PScalar::I64(i64::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::F16 => PrimitiveScalar::some(PScalar::F16(f16::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::F32 => PrimitiveScalar::some(PScalar::F32(f32::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//                 PType::F64 => PrimitiveScalar::some(PScalar::F64(f64::from_le_bytes(
//                     self.reader.read_nbytes()?,
//                 ))),
//             };
//             Ok(pscalar)
//         } else {
//             Ok(PrimitiveScalar::none(ptype))
//         }
//     }
// }
