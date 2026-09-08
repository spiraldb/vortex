// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use arrow_array::StructArray;
use arrow_schema::Field;
use arrow_schema::Fields;
use datafusion_common::ScalarValue;
use vortex::dtype::DType;
use vortex::dtype::NativeDecimalType;
use vortex::dtype::PType;
use vortex::dtype::half::f16;
use vortex::dtype::i256;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::extension::datetime::AnyTemporal;
use vortex::extension::datetime::TemporalMetadata;
use vortex::extension::datetime::TimeUnit;
use vortex::scalar::Scalar;

use crate::convert::TryToDataFusion;

impl TryToDataFusion<ScalarValue> for Scalar {
    fn try_to_df(&self) -> VortexResult<ScalarValue> {
        Ok(match self.dtype() {
            DType::Null => ScalarValue::Null,
            DType::Bool(_) => ScalarValue::Boolean(self.as_bool().value()),
            DType::Primitive(ptype, _) => {
                let pscalar = self.as_primitive();
                match ptype {
                    PType::U8 => ScalarValue::UInt8(pscalar.typed_value::<u8>()),
                    PType::U16 => ScalarValue::UInt16(pscalar.typed_value::<u16>()),
                    PType::U32 => ScalarValue::UInt32(pscalar.typed_value::<u32>()),
                    PType::U64 => ScalarValue::UInt64(pscalar.typed_value::<u64>()),
                    PType::I8 => ScalarValue::Int8(pscalar.typed_value::<i8>()),
                    PType::I16 => ScalarValue::Int16(pscalar.typed_value::<i16>()),
                    PType::I32 => ScalarValue::Int32(pscalar.typed_value::<i32>()),
                    PType::I64 => ScalarValue::Int64(pscalar.typed_value::<i64>()),
                    PType::F16 => ScalarValue::Float16(pscalar.typed_value::<f16>()),
                    PType::F32 => ScalarValue::Float32(pscalar.typed_value::<f32>()),
                    PType::F64 => ScalarValue::Float64(pscalar.typed_value::<f64>()),
                }
            }
            DType::Decimal(decimal_type, _) => {
                let dscalar = self.as_decimal();
                let precision = decimal_type.precision();
                let scale = decimal_type.scale();

                if precision <= i32::MAX_PRECISION {
                    match dscalar.decimal_value() {
                        None => ScalarValue::Decimal32(None, precision, scale),
                        Some(value) => match value.cast::<i32>() {
                            Some(v32) => ScalarValue::Decimal32(Some(v32), precision, scale),
                            None => {
                                vortex_bail!(
                                    "invalid ScalarValue {value} for decimal with precision {precision}",
                                )
                            }
                        },
                    }
                } else if precision <= i64::MAX_PRECISION {
                    match dscalar.decimal_value() {
                        None => ScalarValue::Decimal64(None, precision, scale),
                        Some(value) => match value.cast::<i64>() {
                            Some(v64) => ScalarValue::Decimal64(Some(v64), precision, scale),
                            None => {
                                vortex_bail!(
                                    "invalid ScalarValue {value} for decimal with precision {precision}",
                                )
                            }
                        },
                    }
                } else if precision <= i128::MAX_PRECISION {
                    match dscalar.decimal_value() {
                        None => ScalarValue::Decimal128(None, precision, scale),
                        Some(value) => match value.cast::<i128>() {
                            Some(v128) => ScalarValue::Decimal128(Some(v128), precision, scale),
                            None => {
                                vortex_bail!(
                                    "invalid ScalarValue {value} for decimal with precision {precision}",
                                )
                            }
                        },
                    }
                } else {
                    match dscalar.decimal_value() {
                        None => ScalarValue::Decimal256(None, precision, scale),
                        Some(value) => match value.cast::<i256>() {
                            Some(v256) => {
                                ScalarValue::Decimal256(Some(v256.into()), precision, scale)
                            }
                            None => {
                                vortex_bail!(
                                    "invalid ScalarValue {value} for decimal with precision {precision}",
                                )
                            }
                        },
                    }
                }
            }
            // SAFETY: By construction Utf8 scalar values are utf8
            DType::Utf8(_) => ScalarValue::Utf8(self.as_utf8().value().cloned().map(|s| unsafe {
                String::from_utf8_unchecked(Vec::<u8>::from(s.into_inner().into_bytes()))
            })),
            DType::Binary(_) => ScalarValue::Binary(
                self.as_binary()
                    .value()
                    .cloned()
                    .map(|b| Vec::<u8>::from(b.into_bytes())),
            ),
            dtype @ DType::List(..) => vortex_bail!(
                "cannot convert Vortex scalar dtype {dtype} to DataFusion ScalarValue: unsupported scalar type"
            ),
            dtype @ DType::FixedSizeList(..) => vortex_bail!(
                "cannot convert Vortex scalar dtype {dtype} to DataFusion ScalarValue: unsupported scalar type"
            ),
            dtype @ DType::Map(..) => vortex_bail!(
                "cannot convert Vortex scalar dtype {dtype} to DataFusion ScalarValue: unsupported scalar type"
            ),
            DType::Struct(..) => struct_to_df(self)?,
            dtype @ DType::Union(..) => vortex_bail!(
                "cannot convert Vortex scalar dtype {dtype} to DataFusion ScalarValue: unsupported scalar type"
            ),
            DType::Variant(_) => vortex_bail!("Variant scalars aren't supported with DF"),
            DType::Extension(ext) => {
                let storage_scalar = self.as_extension().to_storage_scalar();

                let Some(temporal) = ext.metadata_opt::<AnyTemporal>() else {
                    // Unknown extension type: perform scalar conversion using the canonical
                    // scalar DType.
                    return storage_scalar.try_to_df();
                };

                // Special handling: temporal extension types in Vortex correspond to Arrow's
                // temporal physical types.
                let pv = storage_scalar.as_primitive();
                match temporal {
                    TemporalMetadata::Timestamp(unit, tz) => match unit {
                        TimeUnit::Nanoseconds => {
                            ScalarValue::TimestampNanosecond(pv.as_::<i64>(), tz.clone())
                        }
                        TimeUnit::Microseconds => {
                            ScalarValue::TimestampMicrosecond(pv.as_::<i64>(), tz.clone())
                        }
                        TimeUnit::Milliseconds => {
                            ScalarValue::TimestampMillisecond(pv.as_::<i64>(), tz.clone())
                        }
                        TimeUnit::Seconds => {
                            ScalarValue::TimestampSecond(pv.as_::<i64>(), tz.clone())
                        }
                        TimeUnit::Days => {
                            unreachable!("Unsupported TimeUnit {unit} for {}", ext.id())
                        }
                    },
                    TemporalMetadata::Date(unit) => match unit {
                        TimeUnit::Milliseconds => ScalarValue::Date64(pv.as_::<i64>()),
                        TimeUnit::Days => ScalarValue::Date32(pv.as_::<i32>()),
                        _ => unreachable!("Unsupported TimeUnit {unit} for {}", ext.id()),
                    },
                    TemporalMetadata::Time(unit) => match unit {
                        TimeUnit::Nanoseconds => ScalarValue::Time64Nanosecond(pv.as_::<i64>()),
                        TimeUnit::Microseconds => ScalarValue::Time64Microsecond(pv.as_::<i64>()),
                        TimeUnit::Milliseconds => ScalarValue::Time32Millisecond(pv.as_::<i32>()),
                        TimeUnit::Seconds => ScalarValue::Time32Second(pv.as_::<i32>()),
                        TimeUnit::Days => {
                            unreachable!("Unsupported TimeUnit {unit} for {}", ext.id())
                        }
                    },
                }
            }
        })
    }
}

/// Converts a Vortex struct scalar to a DataFusion `ScalarValue::Struct`.
fn struct_to_df(scalar: &Scalar) -> VortexResult<ScalarValue> {
    let scalar = scalar.as_struct();
    let struct_fields = scalar.struct_fields();
    let (fields, arrays): (Vec<Field>, Vec<_>) = struct_fields
        .names()
        .iter()
        .zip(struct_fields.fields())
        .enumerate()
        .map(|(idx, (name, field_dtype))| {
            let nullable = field_dtype.is_nullable();
            let child = if scalar.is_null() {
                Scalar::null(field_dtype)
            } else {
                scalar
                    .field_by_idx(idx)
                    .ok_or_else(|| vortex_err!("missing struct field {name}"))?
            };
            let array = child
                .try_to_df()?
                .to_array()
                .map_err(|e| vortex_err!("failed to build struct field array: {e}"))?;
            Ok((
                Field::new(name.as_ref(), array.data_type().clone(), nullable),
                array,
            ))
        })
        .collect::<VortexResult<Vec<_>>>()?
        .into_iter()
        .unzip();

    let fields = Fields::from(fields);
    let struct_array = if scalar.is_null() {
        StructArray::new_null(fields, 1)
    } else {
        StructArray::try_new(fields, arrays, None)
            .map_err(|e| vortex_err!("failed to build struct scalar array: {e}"))?
    };
    Ok(ScalarValue::Struct(Arc::new(struct_array)))
}

#[cfg(test)]
mod tests {
    use datafusion_common::ScalarValue;
    use datafusion_common::arrow::datatypes::i256 as arrow_i256;
    use rstest::rstest;
    use vortex::buffer::ByteBuffer;
    use vortex::dtype::DType;
    use vortex::dtype::DecimalDType;
    use vortex::dtype::FieldNames;
    use vortex::dtype::Nullability;
    use vortex::dtype::PType;
    use vortex::dtype::StructFields;
    use vortex::dtype::UnionVariants;
    use vortex::dtype::i256;
    use vortex::scalar::DecimalValue;
    use vortex::scalar::Scalar;

    use super::*;

    #[rstest]
    #[case::u8_some(Scalar::from(42u8), ScalarValue::UInt8(Some(42)))]
    #[case::u8_null(
        Scalar::null(DType::Primitive(PType::U8, Nullability::Nullable)),
        ScalarValue::UInt8(None)
    )]
    #[case::u16_some(Scalar::from(1234u16), ScalarValue::UInt16(Some(1234)))]
    #[case::u16_null(
        Scalar::null(DType::Primitive(PType::U16, Nullability::Nullable)),
        ScalarValue::UInt16(None)
    )]
    #[case::u32_some(Scalar::from(123456u32), ScalarValue::UInt32(Some(123456)))]
    #[case::u32_null(
        Scalar::null(DType::Primitive(PType::U32, Nullability::Nullable)),
        ScalarValue::UInt32(None)
    )]
    #[case::u64_some(Scalar::from(12345678u64), ScalarValue::UInt64(Some(12345678)))]
    #[case::u64_null(
        Scalar::null(DType::Primitive(PType::U64, Nullability::Nullable)),
        ScalarValue::UInt64(None)
    )]
    #[case::i8_some(Scalar::from(-42i8), ScalarValue::Int8(Some(-42)))]
    #[case::i8_null(
        Scalar::null(DType::Primitive(PType::I8, Nullability::Nullable)),
        ScalarValue::Int8(None)
    )]
    #[case::i16_some(Scalar::from(-1234i16), ScalarValue::Int16(Some(-1234)))]
    #[case::i16_null(
        Scalar::null(DType::Primitive(PType::I16, Nullability::Nullable)),
        ScalarValue::Int16(None)
    )]
    #[case::i32_some(Scalar::from(-123456i32), ScalarValue::Int32(Some(-123456)))]
    #[case::i32_null(
        Scalar::null(DType::Primitive(PType::I32, Nullability::Nullable)),
        ScalarValue::Int32(None)
    )]
    #[case::i64_some(Scalar::from(-12345678i64), ScalarValue::Int64(Some(-12345678)))]
    #[case::i64_null(
        Scalar::null(DType::Primitive(PType::I64, Nullability::Nullable)),
        ScalarValue::Int64(None)
    )]
    #[case::f32_some(Scalar::from(1.5f32), ScalarValue::Float32(Some(1.5)))]
    #[case::f32_null(
        Scalar::null(DType::Primitive(PType::F32, Nullability::Nullable)),
        ScalarValue::Float32(None)
    )]
    #[case::f64_some(Scalar::from(2.5f64), ScalarValue::Float64(Some(2.5)))]
    #[case::f64_null(
        Scalar::null(DType::Primitive(PType::F64, Nullability::Nullable)),
        ScalarValue::Float64(None)
    )]
    fn test_primitive_to_datafusion(
        #[case] vortex_scalar: Scalar,
        #[case] expected_df_scalar: ScalarValue,
    ) {
        let result = vortex_scalar.try_to_df().unwrap();
        assert_eq!(result, expected_df_scalar);
    }

    #[rstest]
    #[case::bool_some(Scalar::from(true), ScalarValue::Boolean(Some(true)))]
    #[case::bool_some_false(Scalar::from(false), ScalarValue::Boolean(Some(false)))]
    #[case::bool_null(
        Scalar::null(DType::Bool(Nullability::Nullable)),
        ScalarValue::Boolean(None)
    )]
    #[case::null_type(Scalar::null(DType::Null), ScalarValue::Null)]
    fn test_bool_and_null_to_datafusion(
        #[case] vortex_scalar: Scalar,
        #[case] expected_df_scalar: ScalarValue,
    ) {
        let result = vortex_scalar.try_to_df().unwrap();
        assert_eq!(result, expected_df_scalar);
    }

    #[rstest]
    #[case::utf8_some(Scalar::from("hello"), ScalarValue::Utf8(Some("hello".to_string())))]
    #[case::utf8_null(
        Scalar::null(DType::Utf8(Nullability::Nullable)),
        ScalarValue::Utf8(None)
    )]
    #[case::binary_some(
        Scalar::binary(ByteBuffer::from(vec![1u8, 2, 3, 4]), Nullability::NonNullable),
        ScalarValue::Binary(Some(vec![1u8, 2, 3, 4]))
    )]
    #[case::binary_null(
        Scalar::null(DType::Binary(Nullability::Nullable)),
        ScalarValue::Binary(None)
    )]
    fn test_string_and_binary_to_datafusion(
        #[case] vortex_scalar: Scalar,
        #[case] expected_df_scalar: ScalarValue,
    ) {
        let result = vortex_scalar.try_to_df().unwrap();
        assert_eq!(result, expected_df_scalar);
    }

    #[rstest]
    #[case::decimal32_some(
        Scalar::decimal(
            DecimalValue::I32(1234),
            DecimalDType::new(5, 2),
            Nullability::NonNullable
        ),
        ScalarValue::Decimal32(Some(1234), 5, 2)
    )]
    #[case::decimal32_null(
        Scalar::null(DType::Decimal(DecimalDType::new(5, 2), Nullability::Nullable)),
        ScalarValue::Decimal32(None, 5, 2)
    )]
    #[case::decimal64_some(
        Scalar::decimal(
            DecimalValue::I64(12345),
            DecimalDType::new(10, 2),
            Nullability::NonNullable
        ),
        ScalarValue::Decimal64(Some(12345), 10, 2)
    )]
    #[case::decimal64_null(
        Scalar::null(DType::Decimal(DecimalDType::new(10, 2), Nullability::Nullable)),
        ScalarValue::Decimal64(None, 10, 2)
    )]
    #[case::decimal128_some(
        Scalar::decimal(
            DecimalValue::I128(12345),
            DecimalDType::new(20, 2),
            Nullability::NonNullable
        ),
        ScalarValue::Decimal128(Some(12345), 20, 2)
    )]
    #[case::decimal128_null(
        Scalar::null(DType::Decimal(DecimalDType::new(20, 2), Nullability::Nullable)),
        ScalarValue::Decimal128(None, 20, 2)
    )]
    #[case::decimal256_some(
        Scalar::decimal(
            DecimalValue::I256(i256::from(arrow_i256::from_i128(12345))),
            DecimalDType::new(50, 10),
            Nullability::NonNullable
        ),
        ScalarValue::Decimal256(Some(arrow_i256::from_i128(12345)), 50, 10)
    )]
    #[case::decimal256_null(
        Scalar::null(DType::Decimal(DecimalDType::new(50, 10), Nullability::Nullable)),
        ScalarValue::Decimal256(None, 50, 10)
    )]
    fn test_decimal_to_datafusion(
        #[case] vortex_scalar: Scalar,
        #[case] expected_df_scalar: ScalarValue,
    ) {
        let result = vortex_scalar.try_to_df().unwrap();
        assert_eq!(result, expected_df_scalar);
    }

    #[rstest]
    #[case::null_type(Scalar::null(DType::Null), ScalarValue::Null)]
    #[case::null_bool(
        Scalar::null(DType::Bool(Nullability::Nullable)),
        ScalarValue::Boolean(None)
    )]
    #[case::null_i32(
        Scalar::null(DType::Primitive(PType::I32, Nullability::Nullable)),
        ScalarValue::Int32(None)
    )]
    #[case::null_f64(
        Scalar::null(DType::Primitive(PType::F64, Nullability::Nullable)),
        ScalarValue::Float64(None)
    )]
    #[case::null_utf8(
        Scalar::null(DType::Utf8(Nullability::Nullable)),
        ScalarValue::Utf8(None)
    )]
    #[case::null_binary(
        Scalar::null(DType::Binary(Nullability::Nullable)),
        ScalarValue::Binary(None)
    )]
    #[case::null_decimal128(
        Scalar::null(DType::Decimal(DecimalDType::new(20, 2), Nullability::Nullable)),
        ScalarValue::Decimal128(None, 20, 2)
    )]
    #[case::null_decimal64(
        Scalar::null(DType::Decimal(DecimalDType::new(10, 2), Nullability::Nullable)),
        ScalarValue::Decimal64(None, 10, 2)
    )]
    #[case::null_decimal32(
        Scalar::null(DType::Decimal(DecimalDType::new(5, 2), Nullability::Nullable)),
        ScalarValue::Decimal32(None, 5, 2)
    )]
    fn test_null_handling(
        #[case] vortex_null: Scalar,
        #[case] expected_df_null: ScalarValue,
    ) -> VortexResult<()> {
        assert_eq!(vortex_null.try_to_df()?, expected_df_null);
        Ok(())
    }

    #[test]
    fn test_struct_scalar_to_datafusion() -> VortexResult<()> {
        let dtype = DType::Struct(
            StructFields::new(
                FieldNames::from(["x", "y"]),
                vec![
                    DType::Primitive(PType::F64, Nullability::NonNullable),
                    DType::Primitive(PType::F64, Nullability::NonNullable),
                ],
            ),
            Nullability::NonNullable,
        );
        let original = Scalar::struct_(
            dtype,
            vec![Scalar::from(-111.7610f64), Scalar::from(34.8697f64)],
        );

        let expected = StructArray::try_new(
            vec![
                Field::new("x", arrow_schema::DataType::Float64, false),
                Field::new("y", arrow_schema::DataType::Float64, false),
            ]
            .into(),
            vec![
                Arc::new(arrow_array::Float64Array::from(vec![-111.7610])),
                Arc::new(arrow_array::Float64Array::from(vec![34.8697])),
            ],
            None,
        )?;
        assert_eq!(
            original.try_to_df()?,
            ScalarValue::Struct(Arc::new(expected))
        );
        Ok(())
    }

    #[test]
    fn test_null_struct_scalar_to_datafusion() -> VortexResult<()> {
        let dtype = DType::Struct(
            StructFields::new(
                FieldNames::from(["x", "y"]),
                vec![
                    DType::Primitive(PType::F64, Nullability::Nullable),
                    DType::Primitive(PType::F64, Nullability::Nullable),
                ],
            ),
            Nullability::Nullable,
        );

        let df = Scalar::null(dtype).try_to_df()?;
        assert!(matches!(df, ScalarValue::Struct(_)));
        assert!(df.is_null());
        Ok(())
    }

    #[rstest]
    #[case::list(Scalar::null(DType::List(
        Arc::new(DType::Primitive(PType::I32, Nullability::Nullable)),
        Nullability::Nullable
    )))]
    #[case::fixed_size_list(Scalar::null(DType::FixedSizeList(
        Arc::new(DType::Primitive(PType::I32, Nullability::Nullable)),
        2,
        Nullability::Nullable
    )))]
    #[case::union(Scalar::null(DType::Union(
        UnionVariants::new(
            ["a"].into(),
            vec![DType::Primitive(PType::I32, Nullability::Nullable)],
        )
        .unwrap(),
        Nullability::Nullable,
    )))]
    fn unsupported_vortex_scalars_return_errors(#[case] scalar: Scalar) {
        let err = scalar.try_to_df().unwrap_err();

        assert!(err.to_string().contains("unsupported scalar type"), "{err}");
    }
}
