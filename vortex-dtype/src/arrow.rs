//! Conversion helpers between Vortex [crate::DType] and Arrow [arrow_schema::Schema].

use arrow_schema::{DataType, Field as ArrowField, Field, FieldRef, Fields, Schema, SchemaBuilder};
use vortex_error::{vortex_bail, VortexError};

use crate::{DType, Nullability, PType};

impl TryFrom<&DType> for Schema {
    type Error = VortexError;

    fn try_from(dtype: &DType) -> Result<Self, Self::Error> {
        let DType::Struct(struct_dtype, nullable) = dtype else {
            vortex_bail!(InvalidArgument: "only DType::Struct can be converted to arrow schema");
        };

        if *nullable != Nullability::NonNullable {
            vortex_bail!(InvalidArgument: "top-level struct in Schema must be NonNullable");
        }

        let mut builder = SchemaBuilder::with_capacity(struct_dtype.names().len());
        for (field_name, field_dtype) in struct_dtype
            .names()
            .iter()
            .zip(struct_dtype.dtypes().iter())
        {
            builder.push(FieldRef::from(ArrowField::new(
                field_name.to_string(),
                DataType::try_from(field_dtype)?,
                field_dtype.is_nullable(),
            )));
        }

        Ok(builder.finish())
    }
}

impl TryFrom<DType> for Schema {
    type Error = VortexError;

    fn try_from(value: DType) -> Result<Self, Self::Error> {
        Self::try_from(&value)
    }
}

impl TryFrom<&DType> for DataType {
    type Error = VortexError;

    fn try_from(dtype: &DType) -> Result<Self, Self::Error> {
        match dtype {
            DType::Null => Ok(DataType::Null),
            DType::Bool(_) => Ok(DataType::Boolean),
            DType::Primitive(ptype, _) => Ok(match ptype {
                PType::U8 => DataType::UInt8,
                PType::U16 => DataType::UInt16,
                PType::U32 => DataType::UInt32,
                PType::U64 => DataType::UInt64,
                PType::I8 => DataType::Int8,
                PType::I16 => DataType::Int16,
                PType::I32 => DataType::Int32,
                PType::I64 => DataType::Int64,
                PType::F16 => DataType::Float16,
                PType::F32 => DataType::Float32,
                PType::F64 => DataType::Float64,
            }),
            DType::Utf8(_) => Ok(DataType::Utf8),
            DType::Binary(_) => Ok(DataType::Binary),
            DType::Struct(struct_dtype, _) => {
                let mut fields = Vec::with_capacity(struct_dtype.names().len());
                for (field_name, field_dt) in struct_dtype
                    .names()
                    .iter()
                    .zip(struct_dtype.dtypes().iter())
                {
                    fields.push(FieldRef::from(Field::new(
                        field_name.to_string(),
                        DataType::try_from(field_dt)?,
                        field_dt.is_nullable(),
                    )));
                }

                Ok(DataType::Struct(Fields::from(fields)))
            }
            DType::List(list_dt, _) => {
                let dtype: &DType = list_dt;
                Ok(DataType::List(FieldRef::from(Field::new(
                    "element",
                    DataType::try_from(dtype)?,
                    dtype.is_nullable(),
                ))))
            }
            DType::Extension(..) => {
                vortex_bail!(InvalidArgument: "Extension DType conversion to Arrow not supported")
            }
        }
    }
}

impl TryFrom<DType> for DataType {
    type Error = VortexError;

    fn try_from(dtype: DType) -> Result<Self, Self::Error> {
        DataType::try_from(&dtype)
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, FieldRef, Fields, Schema};

    use crate::{DType, ExtDType, ExtID, FieldName, FieldNames, Nullability, PType, StructDType};

    #[test]
    fn test_dtype_conversion() {
        assert_eq!(DataType::try_from(DType::Null).unwrap(), DataType::Null);

        assert_eq!(
            DataType::try_from(DType::Bool(Nullability::NonNullable)).unwrap(),
            DataType::Boolean
        );

        assert_eq!(
            DataType::try_from(DType::Primitive(PType::U64, Nullability::NonNullable)).unwrap(),
            DataType::UInt64
        );

        assert_eq!(
            DataType::try_from(DType::Utf8(Nullability::NonNullable)).unwrap(),
            DataType::Utf8
        );

        assert_eq!(
            DataType::try_from(DType::Binary(Nullability::NonNullable)).unwrap(),
            DataType::Binary
        );

        assert_eq!(
            DataType::try_from(DType::List(
                Arc::new(DType::Bool(Nullability::NonNullable)),
                Nullability::Nullable,
            ))
            .unwrap(),
            DataType::List(FieldRef::from(Field::new(
                "element".to_string(),
                DataType::Boolean,
                false,
            )))
        );

        assert_eq!(
            DataType::try_from(DType::Struct(
                StructDType::new(
                    FieldNames::from(vec![FieldName::from("field_a"), FieldName::from("field_b")]),
                    vec![DType::Bool(false.into()), DType::Utf8(true.into())],
                ),
                Nullability::NonNullable,
            ))
            .unwrap(),
            DataType::Struct(Fields::from(vec![
                FieldRef::from(Field::new("field_a", DataType::Boolean, false)),
                FieldRef::from(Field::new("field_b", DataType::Utf8, true)),
            ]))
        );

        assert!(DataType::try_from(DType::Extension(
            ExtDType::new(ExtID::from("my-fake-ext-dtype"), None),
            Nullability::NonNullable,
        ))
        .is_err())
    }

    #[test]
    fn test_schema_conversion() {
        let struct_dtype = StructDType::new(
            FieldNames::from([
                FieldName::from("field_a"),
                FieldName::from("field_b"),
                FieldName::from("field_c"),
            ]),
            vec![
                DType::Bool(Nullability::NonNullable),
                DType::Utf8(Nullability::NonNullable),
                DType::Primitive(PType::I32, Nullability::Nullable),
            ],
        );

        let schema_nonnull = DType::Struct(struct_dtype.clone(), Nullability::NonNullable);

        assert_eq!(
            Schema::try_from(&schema_nonnull).unwrap(),
            Schema::new(Fields::from(vec![
                Field::new("field_a", DataType::Boolean, false),
                Field::new("field_b", DataType::Utf8, false),
                Field::new("field_c", DataType::Int32, true),
            ]))
        );

        let schema_null = DType::Struct(struct_dtype.clone(), Nullability::Nullable);

        assert!(Schema::try_from(&schema_null).is_err());
    }
}
