use flatbuffers::{FlatBufferBuilder, WIPOffset};
use itertools::Itertools;
use vortex_flatbuffers::{FlatBufferRoot, WriteFlatBuffer};

use crate::flatbuffers as fb;
use crate::{DType, FloatWidth, IntWidth, Nullability, Signedness};

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
            DType::Decimal(p, s, n) => fb::Decimal::create(
                fbb,
                &fb::DecimalArgs {
                    precision: *p,
                    scale: *s,
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Float(width, n) => fb::Float::create(
                fbb,
                &fb::FloatArgs {
                    width: width.into(),
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Utf8(n) => fb::Utf8::create(
                fbb,
                &fb::Utf8Args {
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Binary(n) => fb::Binary::create(
                fbb,
                &fb::BinaryArgs {
                    nullability: n.into(),
                },
            )
            .as_union_value(),
            DType::Struct(names, dtypes) => {
                let names = names
                    .iter()
                    .map(|n| fbb.create_string(n.as_str()))
                    .collect_vec();
                let names = Some(fbb.create_vector(&names));

                let dtypes = dtypes
                    .iter()
                    .map(|dtype| dtype.write_flatbuffer(fbb))
                    .collect_vec();
                let fields = Some(fbb.create_vector(&dtypes));

                fb::Struct_::create(fbb, &fb::Struct_Args { names, fields }).as_union_value()
            }
            DType::List(e, n) => {
                let element_type = Some(e.as_ref().write_flatbuffer(fbb));
                fb::List::create(
                    fbb,
                    &fb::ListArgs {
                        element_type,
                        nullability: n.into(),
                    },
                )
                .as_union_value()
            }
            DType::Composite(id, n) => {
                let id = Some(fbb.create_string(id.0));
                fb::Composite::create(
                    fbb,
                    &fb::CompositeArgs {
                        id,
                        nullability: n.into(),
                    },
                )
                .as_union_value()
            }
        };

        let dtype_type = match self {
            DType::Null => fb::Type::Null,
            DType::Bool(_) => fb::Type::Bool,
            DType::Int(..) => fb::Type::Int,
            DType::Decimal(..) => fb::Type::Decimal,
            DType::Float(..) => fb::Type::Float,
            DType::Utf8(_) => fb::Type::Utf8,
            DType::Binary(_) => fb::Type::Binary,
            DType::Struct(..) => fb::Type::Struct_,
            DType::List(..) => fb::Type::List,
            DType::Composite(..) => fb::Type::Composite,
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

impl From<&Nullability> for fb::Nullability {
    fn from(value: &Nullability) -> Self {
        match value {
            Nullability::NonNullable => fb::Nullability::NonNullable,
            Nullability::Nullable => fb::Nullability::Nullable,
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

impl From<&Signedness> for fb::Signedness {
    fn from(value: &Signedness) -> Self {
        match value {
            Signedness::Unsigned => fb::Signedness::Unsigned,
            Signedness::Signed => fb::Signedness::Signed,
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use flatbuffers::{root, FlatBufferBuilder};
    use vortex_flatbuffers::{ReadFlatBuffer, WriteFlatBuffer};

    use crate::flatbuffers as fb;
    use crate::{DType, DTypeSerdeContext, FloatWidth, IntWidth, Nullability, Signedness};

    fn roundtrip_dtype(dtype: DType) {
        let mut fbb = FlatBufferBuilder::new();
        let root_offset = dtype.write_flatbuffer(&mut fbb);
        fbb.finish_minimal(root_offset);

        let bytes = fbb.finished_data();
        let deserialized = DType::read_flatbuffer(
            &DTypeSerdeContext::new(vec![]),
            &root::<fb::DType>(bytes).unwrap(),
        )
        .unwrap();
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
