use crate::{flatbuffers as fb, FloatWidth, IntWidth, Nullability, Signedness};
use crate::{CompositeID, DType};
use std::sync::Arc;
use vortex_error::{vortex_err, VortexError, VortexResult};
use vortex_flatbuffers::ReadFlatBuffer;

#[allow(dead_code)]
pub struct DTypeSerdeContext {
    composite_ids: Vec<CompositeID>,
}

impl DTypeSerdeContext {
    pub fn new(composite_ids: Vec<CompositeID>) -> Self {
        Self { composite_ids }
    }

    pub fn find_composite_id(&self, id: &str) -> Option<CompositeID> {
        self.composite_ids.iter().find(|c| c.0 == id).copied()
    }
}

impl ReadFlatBuffer<DTypeSerdeContext> for DType {
    type Source<'a> = fb::DType<'a>;
    type Error = VortexError;

    fn read_flatbuffer<'a>(
        ctx: &DTypeSerdeContext,
        fb: &Self::Source<'a>,
    ) -> Result<Self, Self::Error> {
        match fb.type_type() {
            fb::Type::Null => Ok(DType::Null),
            fb::Type::Bool => Ok(DType::Bool(
                fb.type__as_bool().unwrap().nullability().try_into()?,
            )),
            fb::Type::Int => {
                let fb_int = fb.type__as_int().unwrap();
                Ok(DType::Int(
                    fb_int.width().try_into()?,
                    fb_int.signedness().try_into()?,
                    fb_int.nullability().try_into()?,
                ))
            }
            fb::Type::Float => {
                let fb_float = fb.type__as_float().unwrap();
                Ok(DType::Float(
                    fb_float.width().try_into()?,
                    fb_float.nullability().try_into()?,
                ))
            }
            fb::Type::Decimal => {
                let fb_decimal = fb.type__as_decimal().unwrap();
                Ok(DType::Decimal(
                    fb_decimal.precision(),
                    fb_decimal.scale(),
                    fb_decimal.nullability().try_into()?,
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
                let element_dtype = DType::read_flatbuffer(ctx, &fb_list.element_type().unwrap())?;
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
                let fields: Vec<DType> = fb_struct
                    .fields()
                    .unwrap()
                    .iter()
                    .map(|f| DType::read_flatbuffer(ctx, &f))
                    .collect::<VortexResult<Vec<_>>>()?;
                Ok(DType::Struct(names, fields))
            }
            fb::Type::Composite => {
                let fb_composite = fb.type__as_composite().unwrap();
                let id = ctx
                    .find_composite_id(fb_composite.id().unwrap())
                    .ok_or_else(|| vortex_err!("Couldn't find composite id"))?;
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

impl TryFrom<fb::IntWidth> for IntWidth {
    type Error = VortexError;

    fn try_from(value: fb::IntWidth) -> VortexResult<Self> {
        match value {
            fb::IntWidth::_8 => Ok(IntWidth::_8),
            fb::IntWidth::_16 => Ok(IntWidth::_16),
            fb::IntWidth::_32 => Ok(IntWidth::_32),
            fb::IntWidth::_64 => Ok(IntWidth::_64),
            _ => Err(vortex_err!("Unknown IntWidth value")),
        }
    }
}

impl TryFrom<fb::Signedness> for Signedness {
    type Error = VortexError;

    fn try_from(value: fb::Signedness) -> VortexResult<Self> {
        match value {
            fb::Signedness::Unsigned => Ok(Signedness::Unsigned),
            fb::Signedness::Signed => Ok(Signedness::Signed),
            _ => Err(vortex_err!("Unknown Signedness value")),
        }
    }
}

impl TryFrom<fb::FloatWidth> for FloatWidth {
    type Error = VortexError;

    fn try_from(value: fb::FloatWidth) -> VortexResult<Self> {
        match value {
            fb::FloatWidth::_16 => Ok(FloatWidth::_16),
            fb::FloatWidth::_32 => Ok(FloatWidth::_32),
            fb::FloatWidth::_64 => Ok(FloatWidth::_64),
            _ => Err(vortex_err!("Unknown IntWidth value")),
        }
    }
}
