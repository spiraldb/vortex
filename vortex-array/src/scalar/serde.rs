use flatbuffers::{root, FlatBufferBuilder, WIPOffset};
use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vortex_error::{vortex_bail, VortexError};
use vortex_flatbuffers::{ReadFlatBuffer, WriteFlatBuffer};
use vortex_schema::DTypeSerdeContext;

use crate::flatbuffers::scalar as fb;
use crate::ptype::PType;
use crate::scalar::{PScalar, Scalar, Utf8Scalar};

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
                let bytes = p.value().map(|pscalar| match pscalar {
                    PScalar::U8(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::U16(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::U32(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::U64(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::I8(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::I16(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::I32(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::I64(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::F16(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::F32(v) => fbb.create_vector(&v.to_le_bytes()),
                    PScalar::F64(v) => fbb.create_vector(&v.to_le_bytes()),
                });
                let primitive = fb::Primitive::create(
                    fbb,
                    &fb::PrimitiveArgs {
                        ptype: p.ptype().into(),
                        bytes,
                    },
                );
                fb::ScalarArgs {
                    type_type: fb::Type::Primitive,
                    type_: Some(primitive.as_union_value()),
                }
            }
            Scalar::Struct(_) => panic!(),
            Scalar::Utf8(utf) => {
                let value = utf.value().map(|utf| fbb.create_string(utf));
                let value = fb::UTF8::create(fbb, &fb::UTF8Args { value }).as_union_value();
                fb::ScalarArgs {
                    type_type: fb::Type::UTF8,
                    type_: Some(value),
                }
            }
            Scalar::Composite(_) => panic!(),
        };

        fb::Scalar::create(fbb, &union)
    }
}

impl From<PType> for fb::PType {
    fn from(value: PType) -> Self {
        match value {
            PType::U8 => fb::PType::U8,
            PType::U16 => fb::PType::U16,
            PType::U32 => fb::PType::U32,
            PType::U64 => fb::PType::U64,
            PType::I8 => fb::PType::I8,
            PType::I16 => fb::PType::I16,
            PType::I32 => fb::PType::I32,
            PType::I64 => fb::PType::I64,
            PType::F16 => fb::PType::F16,
            PType::F32 => fb::PType::F32,
            PType::F64 => fb::PType::F64,
        }
    }
}

impl ReadFlatBuffer<DTypeSerdeContext> for Scalar {
    type Source<'a> = fb::Scalar<'a>;
    type Error = VortexError;

    fn read_flatbuffer(
        _ctx: &DTypeSerdeContext,
        fb: &Self::Source<'_>,
    ) -> Result<Self, Self::Error> {
        match fb.type_type() {
            fb::Type::Binary => {
                todo!()
            }
            fb::Type::Bool => {
                todo!()
            }
            fb::Type::List => {
                todo!()
            }
            fb::Type::Null => {
                todo!()
            }
            fb::Type::Primitive => {
                todo!()
            }
            fb::Type::Struct_ => {
                todo!()
            }
            fb::Type::UTF8 => {
                Utf8Scalar::try_new(
                    fb.type__as_utf8()
                        .expect("missing UTF8 value")
                        .value()
                        .map(|s| s.to_string()),
                );
            }
            fb::Type::Composite => {
                todo!()
            }
            _ => vortex_bail!(InvalidSerde: "Unrecognized scalar type"),
        }
    }
}

impl Serialize for Scalar {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fbb = FlatBufferBuilder::new();
        let root = self.write_flatbuffer(&mut fbb);
        fbb.finish_minimal(root);
        serializer.serialize_bytes(fbb.finished_data())
    }
}

struct ScalarDeserializer(DTypeSerdeContext);

impl<'de> Visitor<'de> for ScalarDeserializer {
    type Value = Scalar;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a vortex dtype")
    }

    fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        let fb = root::<fb::Scalar>(v).map_err(E::custom)?;
        Scalar::read_flatbuffer(&self.0, &fb).map_err(E::custom)
    }
}

// TODO(ngates): Should we just inline composites in scalars?
impl<'de> Deserialize<'de> for Scalar {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let ctx = DTypeSerdeContext::new(vec![]);
        deserializer.deserialize_bytes(ScalarDeserializer(ctx))
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
