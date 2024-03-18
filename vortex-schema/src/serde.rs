use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer, WIPOffset};

use crate::generated::schema::{
    root_as_dtype, Bool, BoolArgs, Composite, CompositeArgs, Int, IntArgs, List, ListArgs, Null,
    NullArgs, Struct_, Struct_Args, Type,
};
use crate::generated::schema::{Binary, BinaryArgs, Signedness as FbSignedness};
use crate::generated::schema::{DType as FbDType, DTypeArgs};
use crate::generated::schema::{Decimal, DecimalArgs, FloatWidth as FbFloatWidth};
use crate::generated::schema::{Float, FloatArgs, IntWidth as FbIntWidth};
use crate::generated::schema::{Nullability as FbNullability, Utf8, Utf8Args};
use crate::{DType, FloatWidth, IntWidth, Nullability, Signedness};

pub trait FbSerialize<'a> {
    type OffsetType;

    fn serialize(&self) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();
        let wip_dtype = self.write_to_builder(&mut fbb);
        fbb.finish(wip_dtype, None);
        fbb.finished_data().to_vec()
    }

    fn write_to_builder(&self, fbb: &mut FlatBufferBuilder<'a>) -> WIPOffset<Self::OffsetType>;
}

pub trait FbDeserialize: Sized {
    fn deserialize(bytes: &[u8]) -> Self;
}

impl<'a> FbSerialize<'a> for DType {
    type OffsetType = FbDType<'a>;

    fn write_to_builder(&self, fbb: &mut FlatBufferBuilder<'a>) -> WIPOffset<Self::OffsetType> {
        let (dtype_union, dtype_union_variant) = match self {
            DType::Null => (Null::create(fbb, &NullArgs {}).as_union_value(), Type::Null),
            DType::Bool(n) => (
                Bool::create(
                    fbb,
                    &BoolArgs {
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Bool,
            ),
            DType::Int(w, s, n) => (
                Int::create(
                    fbb,
                    &IntArgs {
                        width: w.into(),
                        signedness: s.into(),
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Int,
            ),
            DType::Decimal(p, s, n) => (
                Decimal::create(
                    fbb,
                    &DecimalArgs {
                        precision: *p,
                        scale: *s,
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Decimal,
            ),
            DType::Float(w, n) => (
                Float::create(
                    fbb,
                    &FloatArgs {
                        width: w.into(),
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Float,
            ),
            DType::Utf8(n) => (
                Utf8::create(
                    fbb,
                    &Utf8Args {
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Utf8,
            ),
            DType::Binary(n) => (
                Binary::create(
                    fbb,
                    &BinaryArgs {
                        nullability: n.into(),
                    },
                )
                .as_union_value(),
                Type::Binary,
            ),
            DType::Struct(ns, fs) => {
                let name_offsets = ns
                    .iter()
                    .map(|n| fbb.create_string(n.as_ref()))
                    .collect::<Vec<_>>();
                fbb.start_vector::<WIPOffset<&str>>(ns.len());
                for name in name_offsets {
                    fbb.push(name);
                }
                let names_vector = fbb.end_vector(ns.len());

                let dtype_offsets = fs
                    .iter()
                    .map(|f| f.write_to_builder(fbb))
                    .collect::<Vec<_>>();
                fbb.start_vector::<WIPOffset<FbDType>>(fs.len());
                for doff in dtype_offsets {
                    fbb.push(doff);
                }
                let fields_vector = fbb.end_vector(fs.len());

                (
                    Struct_::create(
                        fbb,
                        &Struct_Args {
                            names: Some(names_vector),
                            fields: Some(fields_vector),
                        },
                    )
                    .as_union_value(),
                    Type::Struct_,
                )
            }
            DType::List(e, n) => {
                let fb_dtype = e.as_ref().write_to_builder(fbb);
                (
                    List::create(
                        fbb,
                        &ListArgs {
                            element_type: Some(fb_dtype),
                            nullability: n.into(),
                        },
                    )
                    .as_union_value(),
                    Type::List,
                )
            }
            DType::Composite(id, n) => {
                let id = fbb.create_string(id.0);
                (
                    Composite::create(
                        fbb,
                        &CompositeArgs {
                            id: Some(id),
                            nullability: n.into(),
                        },
                    )
                    .as_union_value(),
                    Type::Composite,
                )
            }
        };

        FbDType::create(
            fbb,
            &DTypeArgs {
                type_type: dtype_union_variant,
                type_: Some(dtype_union),
            },
        )
    }
}

pub fn bytes_as_dtype(bytes: &[u8]) -> Result<DType, InvalidFlatbuffer> {
    root_as_dtype(bytes).map(fb_to_dtype)
}

pub fn fb_to_dtype(fb_dtype: FbDType) -> DType {
    todo!()
}

impl From<&Nullability> for FbNullability {
    fn from(value: &Nullability) -> Self {
        match value {
            Nullability::NonNullable => FbNullability::NonNullable,
            Nullability::Nullable => FbNullability::Nullable,
        }
    }
}

impl From<FbNullability> for Nullability {
    fn from(value: FbNullability) -> Self {
        match value {
            FbNullability::NonNullable => Nullability::NonNullable,
            FbNullability::Nullable => Nullability::Nullable,
            _ => panic!("Unknown nullability value"),
        }
    }
}

impl From<&IntWidth> for FbIntWidth {
    fn from(value: &IntWidth) -> Self {
        match value {
            IntWidth::Unknown => FbIntWidth::Unknown,
            IntWidth::_8 => FbIntWidth::_8,
            IntWidth::_16 => FbIntWidth::_16,
            IntWidth::_32 => FbIntWidth::_32,
            IntWidth::_64 => FbIntWidth::_64,
        }
    }
}

impl From<FbIntWidth> for IntWidth {
    fn from(value: FbIntWidth) -> Self {
        match value {
            FbIntWidth::Unknown => IntWidth::Unknown,
            FbIntWidth::_8 => IntWidth::_8,
            FbIntWidth::_16 => IntWidth::_16,
            FbIntWidth::_32 => IntWidth::_32,
            FbIntWidth::_64 => IntWidth::_64,
            _ => panic!("Unknown IntWidth value"),
        }
    }
}

impl From<&Signedness> for FbSignedness {
    fn from(value: &Signedness) -> Self {
        match value {
            Signedness::Unknown => FbSignedness::Unknown,
            Signedness::Unsigned => FbSignedness::Unsigned,
            Signedness::Signed => FbSignedness::Signed,
        }
    }
}

impl From<FbSignedness> for Signedness {
    fn from(value: FbSignedness) -> Self {
        match value {
            FbSignedness::Unknown => Signedness::Unknown,
            FbSignedness::Unsigned => Signedness::Unsigned,
            FbSignedness::Signed => Signedness::Signed,
            _ => panic!("Unknown Signedness value"),
        }
    }
}

impl From<&FloatWidth> for FbFloatWidth {
    fn from(value: &FloatWidth) -> Self {
        match value {
            FloatWidth::Unknown => FbFloatWidth::Unknown,
            FloatWidth::_16 => FbFloatWidth::_16,
            FloatWidth::_32 => FbFloatWidth::_32,
            FloatWidth::_64 => FbFloatWidth::_64,
        }
    }
}

impl From<FbFloatWidth> for FloatWidth {
    fn from(value: FbFloatWidth) -> Self {
        match value {
            FbFloatWidth::Unknown => FloatWidth::Unknown,
            FbFloatWidth::_16 => FloatWidth::_16,
            FbFloatWidth::_32 => FloatWidth::_32,
            FbFloatWidth::_64 => FloatWidth::_64,
            _ => panic!("Unknown IntWidth value"),
        }
    }
}
