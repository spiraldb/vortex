use flatbuffers::{FlatBufferBuilder, WIPOffset};
use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use vortex_error::VortexError;
use vortex_flatbuffers::{ReadFlatBuffer, WriteFlatBuffer};
use vortex_schema::DTypeSerdeContext;

use crate::flatbuffers::scalar as fb;
use crate::scalar::Scalar;

impl WriteFlatBuffer for Scalar {
    type Target<'a> = fb::Scalar<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        _fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        todo!()
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

// impl<'a, 'b> ScalarWriter<'a, 'b> {
//     pub fn new(writer: &'b mut WriteCtx<'a>) -> Self {
//         Self { writer }
//     }
//
//     pub fn write(&mut self, scalar: &Scalar) -> VortexResult<()> {
//         let mut fbb = FlatBufferBuilder::new();
//
//         self.writer
//             .write_fixed_slice([ScalarTag::from(scalar).into()])?;
//         let union = match scalar {
//             Scalar::Binary(b) => {
//                 let bytes = b.value().map(|bytes| fbb.create_vector(bytes));
//                 ScalarArgs {
//                     type_type: Type::Binary,
//                     type_: bytes.map(|b| {
//                         Binary::create(&mut fbb, &BinaryArgs { value: Some(b) }).as_union_value()
//                     }),
//                 }
//             }
//             Scalar::Bool(b) => ScalarArgs {
//                 type_type: Type::Bool,
//                 type_: b
//                     .value()
//                     .map(|value| Bool::create(&mut fbb, &BoolArgs { value }).as_union_value()),
//             },
//             Scalar::List(_) => panic!(),
//             Scalar::Null(_) => ScalarArgs {
//                 type_type: Type::Null,
//                 type_: None,
//             },
//             Scalar::Primitive(p) => {
//                 // TODO(ngates): clean this up.
//                 // We could probably just keep PScalar as PType + Option<[u8]> internally too?
//                 let primitive = p
//                     .value()
//                     .map(|pscalar| match pscalar {
//                         PScalar::U8(v) => {
//                             let pbytes = fbb.create_vector(&v.to_le_bytes());
//                             PrimitiveArgs {
//                                 ptype: scalar::PType::U8,
//                                 bytes: Some(pbytes),
//                             }
//                         }
//                         _ => panic!(),
//                     })
//                     .unwrap_or_else(|| match p.ptype() {
//                         PType::U8 => PrimitiveArgs {
//                             ptype: scalar::PType::U8,
//                             bytes: None,
//                         },
//                         PType::U16 => PrimitiveArgs {
//                             ptype: scalar::PType::U16,
//                             bytes: None,
//                         },
//                         PType::U32 => PrimitiveArgs {
//                             ptype: scalar::PType::U32,
//                             bytes: None,
//                         },
//                         PType::U64 => PrimitiveArgs {
//                             ptype: scalar::PType::U64,
//                             bytes: None,
//                         },
//                         PType::I8 => PrimitiveArgs {
//                             ptype: scalar::PType::I8,
//                             bytes: None,
//                         },
//                         PType::I16 => PrimitiveArgs {
//                             ptype: scalar::PType::I16,
//                             bytes: None,
//                         },
//                         PType::I32 => PrimitiveArgs {
//                             ptype: scalar::PType::I32,
//                             bytes: None,
//                         },
//                         PType::I64 => PrimitiveArgs {
//                             ptype: scalar::PType::I64,
//                             bytes: None,
//                         },
//                         PType::F16 => PrimitiveArgs {
//                             ptype: scalar::PType::F16,
//                             bytes: None,
//                         },
//                         PType::F32 => PrimitiveArgs {
//                             ptype: scalar::PType::F32,
//                             bytes: None,
//                         },
//                         PType::F64 => PrimitiveArgs {
//                             ptype: scalar::PType::F64,
//                             bytes: None,
//                         },
//                     });
//
//                 let primitive = scalar::Primitive::create(&mut fbb, &primitive);
//                 ScalarArgs {
//                     type_type: Type::Primitive,
//                     type_: Some(primitive.as_union_value()),
//                 }
//             }
//             Scalar::Struct(_) => panic!(),
//             Scalar::Utf8(_) => panic!(),
//             Scalar::Composite(_) => panic!(),
//         };
//
//         let scalar = scalar::Scalar::create(&mut fbb, &union);
//         fbb.finish_minimal(scalar);
//         let (vec, offset) = fbb.collapse();
//         self.writer.write_slice(&vec[offset..])
//     }
// }

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
