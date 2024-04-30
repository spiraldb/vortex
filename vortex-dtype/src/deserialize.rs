use std::sync::Arc;

use vortex_error::{vortex_err, VortexError, VortexResult};
use vortex_flatbuffers::ReadFlatBuffer;

use crate::{flatbuffers as fb, Nullability};
use crate::{CompositeID, DType};

impl ReadFlatBuffer<()> for DType {
    type Source<'a> = fb::DType<'a>;
    type Error = VortexError;

    fn read_flatbuffer(_ctx: &(), fb: &Self::Source<'_>) -> Result<Self, Self::Error> {
        match fb.type_type() {
            fb::Type::Null => Ok(DType::Null),
            fb::Type::Bool => Ok(DType::Bool(
                fb.type__as_bool().unwrap().nullability().try_into()?,
            )),
            fb::Type::Primitive => {
                let fb_primitive = fb.type__as_primitive().unwrap();
                Ok(DType::Primitive(
                    fb_primitive.ptype().try_into()?,
                    fb_primitive.nullability().try_into()?,
                ))
            }
            fb::Type::Binary => Ok(DType::Binary(
                fb.type__as_binary().unwrap().nullability().try_into()?,
            )),
            fb::Type::Utf8 => Ok(DType::Utf8(
                fb.type__as_utf_8().unwrap().nullability().try_into()?,
            )),
            fb::Type::List => {
                let fb_list = fb.type__as_list().unwrap();
                let element_dtype = DType::read_flatbuffer(&(), &fb_list.element_type().unwrap())?;
                Ok(DType::List(
                    Box::new(element_dtype),
                    fb_list.nullability().try_into()?,
                ))
            }
            fb::Type::Struct_ => {
                let fb_struct = fb.type__as_struct_().unwrap();
                let names = fb_struct
                    .names()
                    .unwrap()
                    .iter()
                    .map(|n| Arc::new(n.to_string()))
                    .collect::<Vec<_>>();
                let dtypes: Vec<DType> = fb_struct
                    .fields()
                    .unwrap()
                    .iter()
                    .map(|f| DType::read_flatbuffer(&(), &f))
                    .collect::<VortexResult<Vec<_>>>()?;
                Ok(DType::Struct { names, dtypes })
            }
            fb::Type::Composite => {
                let fb_composite = fb.type__as_composite().unwrap();
                let id = CompositeID::try_from(fb_composite.id().unwrap())?;
                Ok(DType::Composite(id, fb_composite.nullability().try_into()?))
            }
            _ => Err(vortex_err!("Unknown DType variant")),
        }
    }
}

impl TryFrom<fb::Nullability> for Nullability {
    type Error = VortexError;

    fn try_from(value: fb::Nullability) -> VortexResult<Self> {
        match value {
            fb::Nullability::NonNullable => Ok(Nullability::NonNullable),
            fb::Nullability::Nullable => Ok(Nullability::Nullable),
            _ => Err(vortex_err!("Unknown nullability value")),
        }
    }
}
