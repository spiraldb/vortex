use std::sync::Arc;

use flatbuffers::{FlatBufferBuilder, WIPOffset};
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};

use crate::flatbuffers as fb;
use crate::flatbuffers::root_as_dtype;
use crate::{
    CompositeID, DType, FloatWidth, IntWidth, Nullability, SchemaError, SchemaResult, Signedness,
};

#[allow(dead_code)]
pub trait Serialize<'a> {
    type OffsetType;

    // Convert self to flatbuffer representation, returns written bytes and index of valid data
    // If you want to serialize multiple objects you should prefer serialize_to_builder to reuse the allocated memory
    fn serialize(&self) -> (Vec<u8>, usize) {
        let mut fbb = FlatBufferBuilder::new();
        let wip_dtype = self.serialize_to_builder(&mut fbb);
        fbb.finish_minimal(wip_dtype);
        fbb.collapse()
    }

    fn serialize_to_builder(&self, fbb: &mut FlatBufferBuilder<'a>) -> WIPOffset<Self::OffsetType>;
}

pub trait Deserialize<'a>: Sized {
    type OffsetType;

    fn deserialize(bytes: &[u8], find_id: fn(&str) -> Option<CompositeID>) -> SchemaResult<Self>;

    fn convert_from_fb(
        fb_type: Self::OffsetType,
        find_id: fn(&str) -> Option<CompositeID>,
    ) -> SchemaResult<Self>;
}

impl FlatBufferRoot for DType {}
impl WriteFlatBuffer for DType {
    type Target<'a> = fb::DType<'a>;

    fn write_flatbuffer<'fb>(
        &self,
        fbb: &mut FlatBufferBuilder<'fb>,
    ) -> WIPOffset<Self::Target<'fb>> {
        let dtype_type = match self {
            DType::Null => fb::Type::Null,
            DType::Bool(_) => fb::Type::Bool,
            DType::Int(_, _, _) => fb::Type::Int,
            DType::Decimal(_, _, _) => fb::Type::Decimal,
            DType::Float(_, _) => fb::Type::Float,
            DType::Utf8(_) => fb::Type::Utf8,
            DType::Binary(_) => fb::Type::Binary,
            DType::Struct(_, _) => fb::Type::Struct_,
            DType::List(_, _) => fb::Type::List,
            DType::Composite(_, _) => fb::Type::Composite,
        };

        let dtype_union = match self {
            DType::Null => fb::Null::create(fbb, &fb::NullArgs {}).as_union_value(),
            DType::Bool(n) => fb::Bool::create(
                fbb,
                &fb::BoolArgs {
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Int(width, signednedss, n) => fb::Int::create(
                fbb,
                &fb::IntArgs {
                    width: width.into(),
                    signedness: signednedss.into(),
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Decimal(_, _, _) => todo!(),
            DType::Float(_, _) => todo!(),
            DType::Utf8(_) => todo!(),
            DType::Binary(_) => todo!(),
            DType::Struct(_, _) => todo!(),
            DType::List(_, _) => todo!(),
            DType::Composite(_, _) => todo!(),
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

impl From<Nullability> for fb::Nullability {
    fn from(value: Nullability) -> Self {
        match value {
            Nullability::NonNullable => fb::Nullability::NonNullable,
            Nullability::Nullable => fb::Nullability::Nullable,
        }
    }
}

impl<'a> Serialize<'a> for DType {
    type OffsetType = fb::DType<'a>;

    fn serialize_to_builder(&self, fbb: &mut FlatBufferBuilder<'a>) -> WIPOffset<Self::OffsetType> {
        let (dtype_union, dtype_union_variant) = match self {
            DType::Null => (
                fb::Null::create(fbb, &fb::NullArgs {}).as_union_value(),
                fb::Type::Null,
            ),
            DType::Bool(n) => (
                fb::Bool::create(
                    fbb,
                    &fb::BoolArgs {
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                fb::Type::Bool,
            ),
            DType::Int(w, s, n) => (
                fb::Int::create(
                    fbb,
                    &fb::IntArgs {
                        width: w.into(),
                        signedness: s.into(),
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                fb::Type::Int,
            ),
            DType::Decimal(p, s, n) => (
                fb::Decimal::create(
                    fbb,
                    &fb::DecimalArgs {
                        precision: *p,
                        scale: *s,
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                fb::Type::Decimal,
            ),
            DType::Float(w, n) => (
                fb::Float::create(
                    fbb,
                    &fb::FloatArgs {
                        width: w.into(),
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                fb::Type::Float,
            ),
            DType::Utf8(n) => (
                fb::Utf8::create(
                    fbb,
                    &fb::Utf8Args {
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                fb::Type::Utf8,
            ),
            DType::Binary(n) => (
                fb::Binary::create(
                    fbb,
                    &fb::BinaryArgs {
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                fb::Type::Binary,
            ),
            DType::Struct(ns, fs) => {
                let name_offsets = ns
                    .iter()
                    .map(|n| fbb.create_string(n.as_ref()))
                    .collect::<Vec<_>>();
                fbb.start_vector::<WIPOffset<&str>>(ns.len());
                for name in name_offsets.iter().rev() {
                    fbb.push(name);
                }
                let names_vector = fbb.end_vector(ns.len());

                let dtype_offsets = fs
                    .iter()
                    .map(|f| f.serialize_to_builder(fbb))
                    .collect::<Vec<_>>();
                fbb.start_vector::<WIPOffset<fb::DType>>(fs.len());
                for doff in dtype_offsets.iter().rev() {
                    fbb.push(doff);
                }
                let fields_vector = fbb.end_vector(fs.len());

                (
                    fb::Struct_::create(
                        fbb,
                        &fb::Struct_Args {
                            names: Some(names_vector),
                            fields: Some(fields_vector),
                        },
                    )
                    .as_union_value(),
                    fb::Type::Struct_,
                )
            }
            DType::List(e, n) => {
                let fb_dtype = e.as_ref().serialize_to_builder(fbb);
                (
                    fb::List::create(
                        fbb,
                        &fb::ListArgs {
                            element_type: Some(fb_dtype),
                            nullability: n.into(),
                        },
                    )
                    .as_union_value(),
                    fb::Type::List,
                )
            }
            DType::Composite(id, n) => {
                let id = fbb.create_string(id.0);
                (
                    fb::Composite::create(
                        fbb,
                        &fb::CompositeArgs {
                            id: Some(id),
                            nullability: n.into(),
                        },
                    )
                    .as_union_value(),
                    fb::Type::Composite,
                )
            }
        };

        fb::DType::create(
            fbb,
            &fb::DTypeArgs {
                type_type: dtype_union_variant,
                type_: Some(dtype_union),
            },
        )
    }
}

impl<'a> Deserialize<'a> for DType {
    type OffsetType = fb::DType<'a>;

    fn deserialize(bytes: &[u8], find_id: fn(&str) -> Option<CompositeID>) -> SchemaResult<Self> {
        root_as_dtype(bytes)
            .map_err(|e| {
                SchemaError::InvalidArgument(format!("Unable to read bytes as DType: {}", e).into())
            })
            .and_then(|d| Self::convert_from_fb(d, find_id))
    }

    fn convert_from_fb(
        fb_type: Self::OffsetType,
        find_id: fn(&str) -> Option<CompositeID>,
    ) -> SchemaResult<Self> {
        match fb_type.type_type() {
            fb::Type::Null => Ok(DType::Null),
            fb::Type::Bool => Ok(DType::Bool(
                fb_type.type__as_bool().unwrap().nullability().try_into()?,
            )),
            fb::Type::Int => {
                let fb_int = fb_type.type__as_int().unwrap();
                Ok(DType::Int(
                    fb_int.width().try_into()?,
                    fb_int.signedness().try_into()?,
                    fb_int.nullability().try_into()?,
                ))
            }
            fb::Type::Float => {
                let fb_float = fb_type.type__as_float().unwrap();
                Ok(DType::Float(
                    fb_float.width().try_into()?,
                    fb_float.nullability().try_into()?,
                ))
            }
            fb::Type::Decimal => {
                let fb_decimal = fb_type.type__as_decimal().unwrap();
                Ok(DType::Decimal(
                    fb_decimal.precision(),
                    fb_decimal.scale(),
                    fb_decimal.nullability().try_into()?,
                ))
            }
            fb::Type::Binary => Ok(DType::Binary(
                fb_type
                    .type__as_binary()
                    .unwrap()
                    .nullability()
                    .try_into()?,
            )),
            fb::Type::Utf8 => Ok(DType::Utf8(
                fb_type.type__as_utf_8().unwrap().nullability().try_into()?,
            )),
            fb::Type::List => {
                let fb_list = fb_type.type__as_list().unwrap();
                let element_dtype =
                    DType::convert_from_fb(fb_list.element_type().unwrap(), find_id)?;
                Ok(DType::List(
                    Box::new(element_dtype),
                    fb_list.nullability().try_into()?,
                ))
            }
            fb::Type::Struct_ => {
                let fb_struct = fb_type.type__as_struct_().unwrap();
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
                    .map(|f| DType::convert_from_fb(f, find_id))
                    .collect::<SchemaResult<Vec<_>>>()?;
                Ok(DType::Struct(names, fields))
            }
            fb::Type::Composite => {
                let fb_composite = fb_type.type__as_composite().unwrap();
                let id = find_id(fb_composite.id().unwrap()).ok_or_else(|| {
                    SchemaError::InvalidArgument("Couldn't find composite id".into())
                })?;
                Ok(DType::Composite(id, fb_composite.nullability().try_into()?))
            }
            _ => Err(SchemaError::InvalidArgument("Unknown DType variant".into())),
        }
    }
}

impl From<&Nullability> for fb::Nullability {
    fn from(value: &Nullability) -> Self {
        match value {
            Nullability::NonNullable => fb::Nullability::NonNullable,
            Nullability::Nullable => fb::Nullability::Nullable,
        }
    }
}

impl TryFrom<fb::Nullability> for Nullability {
    type Error = SchemaError;

    fn try_from(value: fb::Nullability) -> SchemaResult<Self> {
        match value {
            fb::Nullability::NonNullable => Ok(Nullability::NonNullable),
            fb::Nullability::Nullable => Ok(Nullability::Nullable),
            _ => Err(SchemaError::InvalidArgument(
                "Unknown nullability value".into(),
            )),
        }
    }
}

impl From<&IntWidth> for fb::IntWidth {
    fn from(value: &IntWidth) -> Self {
        match value {
            IntWidth::_8 => fb::IntWidth::_8,
            IntWidth::_16 => fb::IntWidth::_16,
            IntWidth::_32 => fb::IntWidth::_32,
            IntWidth::_64 => fb::IntWidth::_64,
        }
    }
}

impl TryFrom<fb::IntWidth> for IntWidth {
    type Error = SchemaError;

    fn try_from(value: fb::IntWidth) -> SchemaResult<Self> {
        match value {
            fb::IntWidth::_8 => Ok(IntWidth::_8),
            fb::IntWidth::_16 => Ok(IntWidth::_16),
            fb::IntWidth::_32 => Ok(IntWidth::_32),
            fb::IntWidth::_64 => Ok(IntWidth::_64),
            _ => Err(SchemaError::InvalidArgument(
                "Unknown IntWidth value".into(),
            )),
        }
    }
}

impl From<&Signedness> for fb::Signedness {
    fn from(value: &Signedness) -> Self {
        match value {
            Signedness::Unsigned => fb::Signedness::Unsigned,
            Signedness::Signed => fb::Signedness::Signed,
        }
    }
}

impl TryFrom<fb::Signedness> for Signedness {
    type Error = SchemaError;

    fn try_from(value: fb::Signedness) -> SchemaResult<Self> {
        match value {
            fb::Signedness::Unsigned => Ok(Signedness::Unsigned),
            fb::Signedness::Signed => Ok(Signedness::Signed),
            _ => Err(SchemaError::InvalidArgument(
                "Unknown Signedness value".into(),
            )),
        }
    }
}

impl From<&FloatWidth> for fb::FloatWidth {
    fn from(value: &FloatWidth) -> Self {
        match value {
            FloatWidth::_16 => fb::FloatWidth::_16,
            FloatWidth::_32 => fb::FloatWidth::_32,
            FloatWidth::_64 => fb::FloatWidth::_64,
        }
    }
}

impl TryFrom<fb::FloatWidth> for FloatWidth {
    type Error = SchemaError;

    fn try_from(value: fb::FloatWidth) -> SchemaResult<Self> {
        match value {
            fb::FloatWidth::_16 => Ok(FloatWidth::_16),
            fb::FloatWidth::_32 => Ok(FloatWidth::_32),
            fb::FloatWidth::_64 => Ok(FloatWidth::_64),
            _ => Err(SchemaError::InvalidArgument(
                "Unknown IntWidth value".into(),
            )),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::{DType, Deserialize, FloatWidth, IntWidth, Nullability, Serialize, Signedness};

    fn roundtrip_dtype(dtype: DType) {
        let (bytes, head) = dtype.serialize();
        let deserialized =
            DType::deserialize(&bytes[head..], |_| panic!("no composite ids")).unwrap();
        assert_eq!(dtype, deserialized);
    }

    #[test]
    fn roundtrip() {
        roundtrip_dtype(DType::Null);
        roundtrip_dtype(DType::Bool(Nullability::NonNullable));
        roundtrip_dtype(DType::Int(
            IntWidth::_64,
            Signedness::Unsigned,
            Nullability::NonNullable,
        ));
        roundtrip_dtype(DType::Decimal(18, 9, Nullability::NonNullable));
        roundtrip_dtype(DType::Float(FloatWidth::_64, Nullability::NonNullable));
        roundtrip_dtype(DType::Binary(Nullability::NonNullable));
        roundtrip_dtype(DType::Utf8(Nullability::NonNullable));
        roundtrip_dtype(DType::List(
            Box::new(DType::Float(FloatWidth::_32, Nullability::Nullable)),
            Nullability::NonNullable,
        ));
        roundtrip_dtype(DType::Struct(
            vec![Arc::new("strings".into()), Arc::new("ints".into())],
            vec![
                DType::Utf8(Nullability::NonNullable),
                DType::Int(IntWidth::_16, Signedness::Unsigned, Nullability::Nullable),
            ],
        ))
    }
}
