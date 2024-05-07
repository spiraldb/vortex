#![cfg(feature = "flatbuffers")]

use std::sync::Arc;

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use itertools::Itertools;
use vortex_error::{vortex_bail, vortex_err, VortexError, VortexResult};
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};

use crate::{flatbuffers as fb, ExtDType, ExtID, ExtMetadata, PType};
use crate::{DType, StructDType};

impl TryFrom<fb::DType<'_>> for DType {
    type Error = VortexError;

    fn try_from(fb: fb::DType<'_>) -> Result<Self, Self::Error> {
        match fb.type_type() {
            fb::Type::Null => Ok(DType::Null),
            fb::Type::Bool => Ok(DType::Bool(fb.type__as_bool().unwrap().nullable().into())),
            fb::Type::Primitive => {
                let fb_primitive = fb.type__as_primitive().unwrap();
                Ok(DType::Primitive(
                    fb_primitive.ptype().try_into()?,
                    fb_primitive.nullable().into(),
                ))
            }
            fb::Type::Binary => Ok(DType::Binary(
                fb.type__as_binary().unwrap().nullable().into(),
            )),
            fb::Type::Utf8 => Ok(DType::Utf8(fb.type__as_utf_8().unwrap().nullable().into())),
            fb::Type::List => {
                let fb_list = fb.type__as_list().unwrap();
                let element_dtype = DType::try_from(fb_list.element_type().unwrap())?;
                Ok(DType::List(
                    Arc::new(element_dtype),
                    fb_list.nullable().into(),
                ))
            }
            fb::Type::Struct_ => {
                let fb_struct = fb.type__as_struct_().unwrap();
                let names = fb_struct
                    .names()
                    .unwrap()
                    .iter()
                    .map(|n| (*n).into())
                    .collect_vec()
                    .into();
                let dtypes: Vec<DType> = fb_struct
                    .dtypes()
                    .unwrap()
                    .iter()
                    .map(DType::try_from)
                    .collect::<VortexResult<Vec<_>>>()?;
                Ok(DType::Struct(
                    StructDType::new(names, dtypes),
                    fb_struct.nullable().into(),
                ))
            }
            fb::Type::Extension => {
                let fb_ext = fb.type__as_extension().unwrap();
                let id = ExtID::from(fb_ext.id().unwrap());
                let metadata = fb_ext.metadata().map(|m| ExtMetadata::from(m.bytes()));
                Ok(DType::Extension(
                    ExtDType::new(id, metadata),
                    fb_ext.nullable().into(),
                ))
            }
            _ => Err(vortex_err!("Unknown DType variant")),
        }
    }
}

impl FlatBufferRoot for DType {}
impl WriteFlatBuffer for DType {
    type Target<'a> = fb::DType<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let dtype_union = match self {
            DType::Null => fb::Null::create(fbb, &fb::NullArgs {}).as_union_value(),
            DType::Bool(n) => fb::Bool::create(
                fbb,
                &fb::BoolArgs {
                    nullable: (*n).into(),
                },
            )
            .as_union_value(),
            DType::Primitive(ptype, n) => fb::Primitive::create(
                fbb,
                &fb::PrimitiveArgs {
                    ptype: (*ptype).into(),
                    nullable: (*n).into(),
                },
            )
            .as_union_value(),
            DType::Utf8(n) => fb::Utf8::create(
                fbb,
                &fb::Utf8Args {
                    nullable: (*n).into(),
                },
            )
            .as_union_value(),
            DType::Binary(n) => fb::Binary::create(
                fbb,
                &fb::BinaryArgs {
                    nullable: (*n).into(),
                },
            )
            .as_union_value(),
            DType::Struct(st, n) => {
                let names = st
                    .names()
                    .iter()
                    .map(|n| fbb.create_string(n.as_ref()))
                    .collect_vec();
                let names = Some(fbb.create_vector(&names));

                let dtypes = st
                    .dtypes()
                    .iter()
                    .map(|dtype| dtype.write_flatbuffer(fbb))
                    .collect_vec();
                let dtypes = Some(fbb.create_vector(&dtypes));

                fb::Struct_::create(
                    fbb,
                    &fb::Struct_Args {
                        names,
                        dtypes,
                        nullable: (*n).into(),
                    },
                )
                .as_union_value()
            }
            DType::List(e, n) => {
                let element_type = Some(e.as_ref().write_flatbuffer(fbb));
                fb::List::create(
                    fbb,
                    &fb::ListArgs {
                        element_type,
                        nullable: (*n).into(),
                    },
                )
                .as_union_value()
            }
            DType::Extension(ext, n) => {
                let id = Some(fbb.create_string(ext.id().as_ref()));
                let metadata = ext.metadata().map(|m| fbb.create_vector(m.as_ref()));
                fb::Extension::create(
                    fbb,
                    &fb::ExtensionArgs {
                        id,
                        metadata,
                        nullable: (*n).into(),
                    },
                )
                .as_union_value()
            }
        };

        let dtype_type = match self {
            DType::Null => fb::Type::Null,
            DType::Bool(_) => fb::Type::Bool,
            DType::Primitive(..) => fb::Type::Primitive,
            DType::Utf8(_) => fb::Type::Utf8,
            DType::Binary(_) => fb::Type::Binary,
            DType::Struct(..) => fb::Type::Struct_,
            DType::List(..) => fb::Type::List,
            DType::Extension { .. } => fb::Type::Extension,
        };

        fb::DType::create(
            fbb,
            &fb::DTypeArgs {
                type_type: dtype_type,
                type_: Some(dtype_union),
            },
        )
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

impl TryFrom<fb::PType> for PType {
    type Error = VortexError;

    fn try_from(value: fb::PType) -> Result<Self, Self::Error> {
        Ok(match value {
            fb::PType::U8 => PType::U8,
            fb::PType::U16 => PType::U16,
            fb::PType::U32 => PType::U32,
            fb::PType::U64 => PType::U64,
            fb::PType::I8 => PType::I8,
            fb::PType::I16 => PType::I16,
            fb::PType::I32 => PType::I32,
            fb::PType::I64 => PType::I64,
            fb::PType::F16 => PType::F16,
            fb::PType::F32 => PType::F32,
            fb::PType::F64 => PType::F64,
            _ => vortex_bail!(InvalidSerde: "Unknown PType variant"),
        })
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use flatbuffers::root;
    use vortex_flatbuffers::FlatBufferToBytes;

    use crate::nullability::Nullability;
    use crate::DType;
    use crate::{flatbuffers as fb, PType, StructDType};

    fn roundtrip_dtype(dtype: DType) {
        let bytes = dtype.with_flatbuffer_bytes(|bytes| bytes.to_vec());
        let deserialized = DType::try_from(root::<fb::DType>(&bytes).unwrap()).unwrap();
        assert_eq!(dtype, deserialized);
    }

    #[test]
    fn roundtrip() {
        roundtrip_dtype(DType::Null);
        roundtrip_dtype(DType::Bool(Nullability::NonNullable));
        roundtrip_dtype(DType::Primitive(PType::U64, Nullability::NonNullable));
        roundtrip_dtype(DType::Binary(Nullability::NonNullable));
        roundtrip_dtype(DType::Utf8(Nullability::NonNullable));
        roundtrip_dtype(DType::List(
            Arc::new(DType::Primitive(PType::F32, Nullability::Nullable)),
            Nullability::NonNullable,
        ));
        roundtrip_dtype(DType::Struct(
            StructDType::new(
                ["strings".into(), "ints".into()].into(),
                vec![
                    DType::Utf8(Nullability::NonNullable),
                    DType::Primitive(PType::U16, Nullability::Nullable),
                ],
            ),
            Nullability::NonNullable,
        ))
    }
}
