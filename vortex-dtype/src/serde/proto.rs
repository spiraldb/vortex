#![cfg(feature = "proto")]

use std::sync::Arc;

use vortex_error::{vortex_err, VortexError, VortexResult};

use crate::proto::dtype::d_type::Type;
use crate::{proto::dtype as pb, DType, ExtDType, ExtID, ExtMetadata, PType, StructDType};

impl TryFrom<&pb::DType> for DType {
    type Error = VortexError;

    fn try_from(value: &pb::DType) -> Result<Self, Self::Error> {
        match value
            .r#type
            .as_ref()
            .ok_or_else(|| vortex_err!(InvalidSerde: "Unrecognized DType"))?
        {
            Type::Null(_) => Ok(Self::Null),
            Type::Bool(b) => Ok(Self::Bool(b.nullable.into())),
            Type::Primitive(p) => Ok(Self::Primitive(p.r#type().into(), p.nullable.into())),
            Type::Decimal(_) => todo!("Not Implemented"),
            Type::Utf8(u) => Ok(Self::Utf8(u.nullable.into())),
            Type::Binary(b) => Ok(Self::Binary(b.nullable.into())),
            Type::Struct(s) => Ok(Self::Struct(
                StructDType::new(
                    s.names.iter().map(|s| s.as_str().into()).collect(),
                    s.dtypes
                        .iter()
                        .map(TryInto::<Self>::try_into)
                        .collect::<VortexResult<Vec<_>>>()?,
                ),
                s.nullable.into(),
            )),
            Type::List(l) => {
                let nullable = l.nullable.into();
                Ok(Self::List(
                    l.element_type
                        .as_ref()
                        .ok_or_else(|| vortex_err!(InvalidSerde: "Invalid list element type"))?
                        .as_ref()
                        .try_into()
                        .map(Arc::new)?,
                    nullable,
                ))
            }
            Type::Extension(e) => Ok(Self::Extension(
                ExtDType::new(
                    ExtID::from(e.id.as_str()),
                    e.metadata.as_ref().map(|m| ExtMetadata::from(m.as_ref())),
                ),
                e.nullable.into(),
            )),
        }
    }
}

impl From<&DType> for pb::DType {
    fn from(value: &DType) -> Self {
        Self {
            r#type: Some(match value {
                DType::Null => Type::Null(pb::Null {}),
                DType::Bool(n) => Type::Bool(pb::Bool {
                    nullable: (*n).into(),
                }),
                DType::Primitive(ptype, n) => Type::Primitive(pb::Primitive {
                    r#type: pb::PType::from(*ptype).into(),
                    nullable: (*n).into(),
                }),
                DType::Utf8(n) => Type::Utf8(pb::Utf8 {
                    nullable: (*n).into(),
                }),
                DType::Binary(n) => Type::Binary(pb::Binary {
                    nullable: (*n).into(),
                }),
                DType::Struct(s, n) => Type::Struct(pb::Struct {
                    names: s.names().iter().map(|s| s.as_ref().to_string()).collect(),
                    dtypes: s.dtypes().iter().map(Into::into).collect(),
                    nullable: (*n).into(),
                }),
                DType::List(l, n) => Type::List(Box::new(pb::List {
                    element_type: Some(Box::new(l.as_ref().into())),
                    nullable: (*n).into(),
                })),
                DType::Extension(e, n) => Type::Extension(pb::Extension {
                    id: e.id().as_ref().into(),
                    metadata: e.metadata().map(|m| m.as_ref().into()),
                    nullable: (*n).into(),
                }),
            }),
        }
    }
}

impl From<pb::PType> for PType {
    fn from(value: pb::PType) -> Self {
        use pb::PType::*;
        match value {
            U8 => Self::U8,
            U16 => Self::U16,
            U32 => Self::U32,
            U64 => Self::U64,
            I8 => Self::I8,
            I16 => Self::I16,
            I32 => Self::I32,
            I64 => Self::I64,
            F16 => Self::F16,
            F32 => Self::F32,
            F64 => Self::F64,
        }
    }
}

impl From<PType> for pb::PType {
    fn from(value: PType) -> Self {
        use pb::PType::*;
        match value {
            PType::U8 => U8,
            PType::U16 => U16,
            PType::U32 => U32,
            PType::U64 => U64,
            PType::I8 => I8,
            PType::I16 => I16,
            PType::I32 => I32,
            PType::I64 => I64,
            PType::F16 => F16,
            PType::F32 => F32,
            PType::F64 => F64,
        }
    }
}
