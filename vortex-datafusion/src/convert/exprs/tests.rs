// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use arrow_array::Array;
use arrow_array::FixedSizeBinaryArray;
use arrow_array::RecordBatch;
use arrow_array::StructArray;
use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::TimeUnit as ArrowTimeUnit;
use arrow_schema::extension::Uuid as ArrowUuid;
use datafusion::arrow::array::AsArray;
use datafusion::arrow::datatypes::Int32Type;
use datafusion_common::ScalarValue;
use datafusion_common::arrow::buffer::NullBuffer;
use datafusion_common::arrow::datatypes::i256 as arrow_i256;
use datafusion_common::config::ConfigOptions;
use datafusion_common::metadata::FieldMetadata;
use datafusion_expr::Operator as DFOperator;
use datafusion_expr::ScalarUDF;
use datafusion_functions::core::coalesce::CoalesceFunc;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_plan::expressions as df_expr;
use insta::assert_snapshot;
use rstest::rstest;
use vortex::array::IntoArray;
use vortex::array::arrays::ConstantArray;
use vortex::extension::uuid::Uuid;
use vortex::scalar::Scalar;
use vortex::scalar_fn::fns::literal::Literal;
use vortex_array::assert_arrays_eq;

use super::*;
use crate::common_tests::TestSessionContext;
use crate::convert::TryToDataFusion;

#[rstest::fixture]
fn test_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("score", DataType::Float64, true),
        Field::new("active", DataType::Boolean, false),
        Field::new(
            "created_at",
            DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
            true,
        ),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
    ])
}

fn octet_length_expr(input: Arc<dyn PhysicalExpr>, schema: &Schema) -> Arc<dyn PhysicalExpr> {
    Arc::new(
        ScalarFunctionExpr::try_new(
            Arc::new(ScalarUDF::from(OctetLengthFunc::new())),
            vec![input],
            schema,
            Arc::new(ConfigOptions::new()),
        )
        .unwrap(),
    )
}

fn array_length_expr(args: Vec<Arc<dyn PhysicalExpr>>, schema: &Schema) -> Arc<dyn PhysicalExpr> {
    Arc::new(
        ScalarFunctionExpr::try_new(
            Arc::new(ScalarUDF::from(ArrayLength::new())),
            args,
            schema,
            Arc::new(ConfigOptions::new()),
        )
        .unwrap(),
    )
}

struct FailingConvertor;

impl ExpressionConvertor for FailingConvertor {
    fn try_convert(
        &self,
        _expr: &Arc<dyn PhysicalExpr>,
        _schema: &Schema,
    ) -> DFResult<Option<Expression>> {
        Err(exec_datafusion_err!("Expression conversion must not run"))
    }
}

#[test]
fn test_duplicate_aliases_fall_back_before_conversion() -> DFResult<()> {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);
    let projection = ProjectionExprs::from(vec![
        ProjectionExpr {
            expr: Arc::new(df_expr::BinaryExpr::new(
                Arc::new(df_expr::Column::new("b", 1)),
                DFOperator::Plus,
                Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(1)))),
            )),
            alias: "duplicate".into(),
        },
        ProjectionExpr {
            expr: Arc::new(df_expr::Column::new("a", 0)),
            alias: "duplicate".into(),
        },
        ProjectionExpr {
            expr: Arc::new(df_expr::Column::new("b", 1)),
            alias: "duplicate".into(),
        },
    ]);
    let output_schema = projection.project_schema(&schema)?;
    let processed =
        FailingConvertor.split_projection(projection.clone(), &schema, &output_schema)?;
    assert_eq!(
        processed.scan_projection,
        pack(
            [("a", get_item("a", root())), ("b", get_item("b", root()))],
            Nullability::NonNullable,
        )
    );
    assert_eq!(processed.scan_reference_schema, schema);
    assert_eq!(processed.leftover_projection, projection);
    Ok(())
}

#[rstest]
#[case::past_end(1)]
#[case::max(usize::MAX)]
fn test_raw_projection_out_of_bounds(#[case] index: usize) -> DFResult<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let projection = vec![ProjectionExpr {
        expr: Arc::new(df_expr::Column::new("a", index)),
        alias: "a".into(),
    }]
    .into();
    let error = raw_projection(projection, &schema)
        .err()
        .ok_or_else(|| exec_datafusion_err!("Expected an out-of-bounds error"))?;
    assert!(
        error
            .to_string()
            .contains(&format!("Projection column index {index} is out of bounds")),
        "{error}"
    );
    Ok(())
}

#[rstest]
fn test_predicate_rejects_cast_over_modulo(test_schema: Schema) {
    let modulo = Arc::new(df_expr::BinaryExpr::new(
        Arc::new(df_expr::Column::new("id", 0)),
        DFOperator::Modulo,
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(2)))),
    ));
    let expr: Arc<dyn PhysicalExpr> =
        Arc::new(df_expr::CastExpr::new(modulo, DataType::Int64, None));
    assert!(
        !DefaultExpressionConvertor::default()
            .try_convert(&expr, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
#[case::empty(false)]
#[case::column(true)]
fn test_predicate_rejects_unsupported_in_list(
    test_schema: Schema,
    #[case] nonempty: bool,
    #[values(false, true)] negated: bool,
) -> DFResult<()> {
    let column: Arc<dyn PhysicalExpr> = Arc::new(df_expr::Column::new("id", 0));
    let list = if nonempty {
        vec![Arc::clone(&column)]
    } else {
        vec![]
    };
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::InListExpr::try_new(
        column,
        list,
        negated,
        &test_schema,
    )?);
    assert!(
        !DefaultExpressionConvertor::default()
            .try_convert(&expr, &test_schema)?
            .is_some()
    );
    Ok(())
}

#[rstest]
#[case::values(vec![Some(1), Some(3)])]
#[case::with_null(vec![Some(1), None])]
#[case::null_only(vec![None])]
#[case::singleton(vec![Some(1)])]
#[case::duplicates(vec![Some(1), Some(1), None, None])]
fn test_native_in_list(
    #[case] list: Vec<Option<i32>>,
    #[values(false, true)] negated: bool,
) -> anyhow::Result<()> {
    let batch = arrow_array::record_batch!(("a", Int32, vec![Some(1), Some(2), Some(3), None]))?;
    let expr = df_expr::InListExpr::try_new(
        Arc::new(df_expr::Column::new("a", 0)),
        list.into_iter()
            .map(|value| Arc::new(df_expr::Literal::new(ScalarValue::Int32(value))) as _)
            .collect(),
        negated,
        &batch.schema(),
    )?;
    expr.evaluate(&batch)?;
    assert_native_matches(Arc::new(expr), batch)
}

#[rstest]
#[case::boolean(ScalarValue::Boolean(Some(true)), ScalarValue::Boolean(Some(false)))]
#[case::unsigned(ScalarValue::UInt64(Some(u64::MAX)), ScalarValue::UInt64(Some(0)))]
#[case::utf8(ScalarValue::Utf8(Some("a".into())), ScalarValue::Utf8(Some("b".into())))]
#[case::utf8_view(ScalarValue::Utf8View(Some("a".into())), ScalarValue::Utf8View(Some("b".into())))]
#[case::large_utf8(ScalarValue::LargeUtf8(Some("a".into())), ScalarValue::LargeUtf8(Some("b".into())))]
#[case::binary(ScalarValue::Binary(Some(vec![0])), ScalarValue::Binary(Some(vec![1])))]
#[case::binary_view(ScalarValue::BinaryView(Some(vec![0])), ScalarValue::BinaryView(Some(vec![1])))]
#[case::large_binary(ScalarValue::LargeBinary(Some(vec![0])), ScalarValue::LargeBinary(Some(vec![1])))]
#[case::decimal32(
    ScalarValue::Decimal32(Some(1234), 5, 2),
    ScalarValue::Decimal32(Some(5678), 5, 2)
)]
#[case::decimal64(
    ScalarValue::Decimal64(Some(1234), 10, 2),
    ScalarValue::Decimal64(Some(5678), 10, 2)
)]
#[case::decimal128(
    ScalarValue::Decimal128(Some(1234), 20, 2),
    ScalarValue::Decimal128(Some(5678), 20, 2)
)]
#[case::decimal256(
    ScalarValue::Decimal256(Some(arrow_i256::from_i128(1234)), 50, 2),
    ScalarValue::Decimal256(Some(arrow_i256::from_i128(5678)), 50, 2)
)]
#[case::date(ScalarValue::Date32(Some(1)), ScalarValue::Date32(Some(2)))]
#[case::time(
    ScalarValue::Time64Microsecond(Some(1)),
    ScalarValue::Time64Microsecond(Some(2))
)]
#[case::timestamp(ScalarValue::TimestampNanosecond(Some(1), Some("UTC".into())), ScalarValue::TimestampNanosecond(Some(2), Some("UTC".into())))]
#[case::dictionary(ScalarValue::Dictionary(Box::new(DataType::Int8), Box::new(ScalarValue::Utf8(Some("a".into())))), ScalarValue::Dictionary(Box::new(DataType::Int8), Box::new(ScalarValue::Utf8(Some("b".into())))))]
fn test_native_in_list_data_types(
    #[case] member: ScalarValue,
    #[case] absent: ScalarValue,
    #[values(false, true)] negated: bool,
) -> anyhow::Result<()> {
    let null = ScalarValue::try_new_null(&member.data_type())?;
    let batch = RecordBatch::try_from_iter([(
        "a",
        ScalarValue::iter_to_array([member.clone(), absent, null.clone()])?,
    )])?;
    let expr = df_expr::InListExpr::try_new(
        Arc::new(df_expr::Column::new("a", 0)),
        vec![
            Arc::new(df_expr::Literal::new(member)),
            Arc::new(df_expr::Literal::new(null)),
        ],
        negated,
        &batch.schema(),
    )?;
    expr.evaluate(&batch)?;
    assert_native_matches(Arc::new(expr), batch)
}

#[rstest]
fn test_native_in_list_untyped_null(
    #[values(false, true)] negated: bool,
    #[values(false, true)] mixed: bool,
    #[values(false, true)] literal_input: bool,
) -> anyhow::Result<()> {
    let batch = arrow_array::record_batch!(("a", Int32, vec![Some(1), Some(2), None]))?;
    let mut list: Vec<Arc<dyn PhysicalExpr>> =
        vec![Arc::new(df_expr::Literal::new(ScalarValue::Null))];
    if mixed {
        list.push(Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(1)))));
    }
    let value: Arc<dyn PhysicalExpr> = if literal_input {
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(None)))
    } else {
        Arc::new(df_expr::Column::new("a", 0))
    };
    let expr = df_expr::InListExpr::try_new(value, list, negated, &batch.schema())?;
    expr.evaluate(&batch)?;
    assert_native_matches(Arc::new(expr), batch)
}

#[rstest]
fn test_native_in_list_large(#[values(false, true)] negated: bool) -> anyhow::Result<()> {
    let batch =
        arrow_array::record_batch!(("a", Int32, vec![Some(0), Some(1023), Some(1024), None]))?;
    let expr = df_expr::InListExpr::try_new(
        Arc::new(df_expr::Column::new("a", 0)),
        (0..1024)
            .map(|i| Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(i)))) as _)
            .collect(),
        negated,
        &batch.schema(),
    )?;
    expr.evaluate(&batch)?;
    assert_native_matches(Arc::new(expr), batch)
}

#[rstest]
fn test_native_in_list_float(
    #[values(false, true)] negated: bool,
    #[values(DataType::Float32, DataType::Float64)] data_type: DataType,
) -> anyhow::Result<()> {
    let values = [
        Some(-0.0),
        Some(0.0),
        Some(f64::NAN),
        Some(f64::from_bits(f64::NAN.to_bits() + (1 << 29))),
        Some(f64::INFINITY),
        None,
    ];
    let scalar = |value| ScalarValue::Float64(value).cast_to(&data_type);
    let batch = RecordBatch::try_from_iter([(
        "a",
        ScalarValue::iter_to_array(
            values
                .into_iter()
                .map(scalar)
                .collect::<DFResult<Vec<_>>>()?,
        )?,
    )])?;
    let expr = df_expr::InListExpr::try_new(
        Arc::new(df_expr::Column::new("a", 0)),
        vec![
            Arc::new(df_expr::Literal::new(scalar(Some(-0.0))?)),
            Arc::new(df_expr::Literal::new(scalar(Some(f64::NAN))?)),
        ],
        negated,
        &batch.schema(),
    )?;
    expr.evaluate(&batch)?;
    assert_native_matches(Arc::new(expr), batch)
}

#[rstest]
#[case::unsupported(DFOperator::Modulo)]
#[case::fallible(DFOperator::Divide)]
fn test_in_list_unsupported_input(
    #[case] operator: DFOperator,
    #[values(false, true)] negated: bool,
) -> anyhow::Result<()> {
    let batch = arrow_array::record_batch!(("a", Int32, vec![0, 1]))?;
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::InListExpr::try_new(
        Arc::new(df_expr::BinaryExpr::new(
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(12)))),
            operator,
            Arc::new(df_expr::Column::new("a", 0)),
        )),
        vec![Arc::new(df_expr::Literal::new(ScalarValue::Null))],
        negated,
        &batch.schema(),
    )?);
    assert!(expr.evaluate(&batch).is_err());
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &batch.schema())?
            .is_none()
    );
    Ok(())
}

#[test]
fn test_in_list_unsupported_type() -> DFResult<()> {
    let value: Arc<dyn PhysicalExpr> =
        Arc::new(df_expr::Literal::new(ScalarValue::DurationSecond(Some(1))));
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::InListExpr::try_new(
        Arc::clone(&value),
        vec![value],
        false,
        &Schema::empty(),
    )?);
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &Schema::empty())?
            .is_none()
    );
    Ok(())
}

#[test]
fn test_in_list_malformed_literal() -> DFResult<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Decimal128(10, 2), false)]);
    let column: Arc<dyn PhysicalExpr> = Arc::new(df_expr::Column::new("a", 0));
    let expr = Arc::new(df_expr::InListExpr::try_new(
        Arc::clone(&column),
        vec![Arc::new(df_expr::Literal::new(ScalarValue::Decimal128(
            Some(1),
            10,
            2,
        )))],
        false,
        &schema,
    )?);
    let expr = expr.with_new_children(vec![
        column,
        Arc::new(df_expr::Literal::new(ScalarValue::Decimal128(
            Some(1),
            0,
            0,
        ))),
    ])?;
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &schema)
            .is_err()
    );
    Ok(())
}

#[rstest]
#[case::eq(DFOperator::Eq, Operator::Eq)]
#[case::not_eq(DFOperator::NotEq, Operator::NotEq)]
#[case::lt(DFOperator::Lt, Operator::Lt)]
#[case::lte(DFOperator::LtEq, Operator::Lte)]
#[case::gt(DFOperator::Gt, Operator::Gt)]
#[case::gte(DFOperator::GtEq, Operator::Gte)]
#[case::and(DFOperator::And, Operator::And)]
#[case::or(DFOperator::Or, Operator::Or)]
#[case::plus(DFOperator::Plus, Operator::Add)]
#[case::plus(DFOperator::Minus, Operator::Sub)]
#[case::plus(DFOperator::Multiply, Operator::Mul)]
#[case::plus(DFOperator::Divide, Operator::Div)]
fn test_operator_conversion_supported(
    #[case] df_op: DFOperator,
    #[case] expected_vortex_op: Operator,
) {
    assert_eq!(try_operator_from_df(&df_op), Some(expected_vortex_op));
}

#[rstest]
#[case::modulo(DFOperator::Modulo)]
#[case::bitwise_and(DFOperator::BitwiseAnd)]
#[case::regex_match(DFOperator::RegexMatch)]
#[case::like_match(DFOperator::LikeMatch)]
fn test_operator_conversion_unsupported(#[case] df_op: DFOperator) {
    assert_eq!(try_operator_from_df(&df_op), None);
}

#[test]
fn test_expr_from_df_column() {
    let col_expr = df_expr::Column::new("test_column", 0);
    let result = DefaultExpressionConvertor::default()
        .try_convert(
            &(Arc::new(col_expr) as Arc<dyn PhysicalExpr>),
            &Schema::new(vec![Field::new("test_column", DataType::Int32, false)]),
        )
        .unwrap()
        .unwrap();

    assert_snapshot!(result.display_tree().to_string(), @r"
    vortex.get_item(test_column)
    └── input: vortex.root()
    ");
}

#[test]
fn test_expr_from_df_literal() {
    let literal_expr = df_expr::Literal::new(ScalarValue::Int32(Some(42)));
    let result = DefaultExpressionConvertor::default()
        .try_convert(
            &(Arc::new(literal_expr) as Arc<dyn PhysicalExpr>),
            &Schema::empty(),
        )
        .unwrap()
        .unwrap();

    assert_snapshot!(result.display_tree().to_string(), @"vortex.literal(42i32)");
}

fn convert_literal(expr: Arc<dyn PhysicalExpr>) -> anyhow::Result<Scalar> {
    let converted = DefaultExpressionConvertor::default()
        .try_convert(&expr, &Schema::empty())?
        .ok_or_else(|| anyhow::anyhow!("Expected native conversion for {expr}"))?;
    converted
        .as_opt::<Literal>()
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("Expected a literal expression"))
}

#[rstest]
#[case::null(ScalarValue::Null)]
#[case::boolean(ScalarValue::Boolean(Some(true)))]
#[case::false_value(ScalarValue::Boolean(Some(false)))]
#[case::u32(ScalarValue::UInt32(Some(42)))]
#[case::i32(ScalarValue::Int32(Some(-42)))]
#[case::i64(ScalarValue::Int64(Some(-123)))]
#[case::f64(ScalarValue::Float64(Some(2.5)))]
#[case::utf8(ScalarValue::Utf8(Some("test string".into())))]
#[case::binary(ScalarValue::Binary(Some(vec![1, 2, 3])))]
#[case::decimal32(ScalarValue::Decimal32(Some(1234), 5, 2))]
#[case::decimal64(ScalarValue::Decimal64(Some(12345), 10, 2))]
#[case::decimal128(ScalarValue::Decimal128(Some(12345), 20, 2))]
#[case::decimal256(ScalarValue::Decimal256(Some(arrow_i256::from_i128(12345)), 50, 10))]
#[case::date32(ScalarValue::Date32(Some(18628)))]
#[case::date64(ScalarValue::Date64(Some(1609459200000)))]
#[case::time32_second(ScalarValue::Time32Second(Some(3661)))]
#[case::time32_millisecond(ScalarValue::Time32Millisecond(Some(3661000)))]
#[case::time64_microsecond(ScalarValue::Time64Microsecond(Some(3661000000)))]
#[case::time64_nanosecond(ScalarValue::Time64Nanosecond(Some(3661000000000)))]
#[case::timestamp_second(ScalarValue::TimestampSecond(Some(1609459200), None))]
#[case::timestamp_millisecond(ScalarValue::TimestampMillisecond(Some(1609459200000), None))]
#[case::timestamp_microsecond(ScalarValue::TimestampMicrosecond(Some(1609459200000000), None))]
#[case::timestamp_nanosecond(ScalarValue::TimestampNanosecond(Some(1609459200000000000), None))]
#[case::timestamp_timezone(ScalarValue::TimestampNanosecond(Some(1609459200000000000), Some("UTC".into())))]
fn test_literal_round_trip(
    #[case] value: ScalarValue,
    #[values(false, true)] null: bool,
) -> anyhow::Result<()> {
    let value = if null {
        ScalarValue::try_from(&value.data_type())?
    } else {
        value
    };
    let converted = convert_literal(Arc::new(df_expr::Literal::new(value.clone())))?;
    assert_eq!(converted.is_null(), value.is_null());
    assert_eq!(converted.try_to_df()?, value);
    Ok(())
}

#[rstest]
#[case::utf8_view(ScalarValue::Utf8View(Some("test string".into())), Scalar::from("test string"))]
#[case::large_utf8(ScalarValue::LargeUtf8(Some("test string".into())), Scalar::from("test string"))]
#[case::binary_view(ScalarValue::BinaryView(Some(vec![1, 2, 3])), Scalar::binary(vec![1, 2, 3], Nullability::NonNullable))]
#[case::large_binary(ScalarValue::LargeBinary(Some(vec![1, 2, 3])), Scalar::binary(vec![1, 2, 3], Nullability::NonNullable))]
#[case::dictionary(ScalarValue::Dictionary(Box::new(DataType::Int8), Box::new(ScalarValue::Utf8(Some("test string".into())))), Scalar::from("test string"))]
fn test_literal_storage_variants(
    #[case] value: ScalarValue,
    #[case] expected: Scalar,
    #[values(false, true)] null: bool,
) -> anyhow::Result<()> {
    let (value, expected) = if null {
        (
            ScalarValue::try_from(&value.data_type())?,
            Scalar::null(expected.dtype().as_nullable()),
        )
    } else {
        (value, expected)
    };
    let converted = convert_literal(Arc::new(df_expr::Literal::new(value)))?;
    assert!(
        converted.eq_ignore_nullability(&expected),
        "{converted} != {expected}"
    );
    Ok(())
}

#[rstest]
fn test_struct_literal_preserves_extension_child(
    #[values(false, true)] null: bool,
) -> anyhow::Result<()> {
    let mut id_field = Field::new("id", DataType::FixedSizeBinary(16), false);
    id_field.try_with_extension_type(ArrowUuid)?;
    let fields = vec![Arc::new(id_field)].into();
    let array = if null {
        StructArray::new_null(fields, 1)
    } else {
        let ids = FixedSizeBinaryArray::try_from_iter([*b"0123456789abcdef"].into_iter())?;
        StructArray::try_new(fields, vec![Arc::new(ids)], None)?
    };
    let converted = convert_literal(Arc::new(df_expr::Literal::new(ScalarValue::Struct(
        Arc::new(array),
    ))))?;
    assert_eq!(converted.is_null(), null);
    let id_dtype = converted
        .dtype()
        .as_struct_fields()
        .field_by_index(0)
        .ok_or_else(|| anyhow::anyhow!("Expected the id field"))?;
    assert!(id_dtype.as_extension().is::<Uuid>());
    assert!(!id_dtype.is_nullable());
    Ok(())
}

#[rstest]
fn test_literal_preserves_extension_metadata(
    #[values(false, true)] null: bool,
) -> anyhow::Result<()> {
    let mut field = Field::new("id", DataType::FixedSizeBinary(16), null);
    field.try_with_extension_type(ArrowUuid)?;
    let value = ScalarValue::FixedSizeBinary(16, (!null).then(|| b"0123456789abcdef".to_vec()));
    let converted = convert_literal(Arc::new(df_expr::Literal::new_with_metadata(
        value,
        Some(FieldMetadata::new_from_field(&field)),
    )))?;
    assert_eq!(converted.is_null(), null);
    assert!(converted.dtype().as_extension().is::<Uuid>());
    Ok(())
}

#[test]
fn test_expr_from_df_binary() {
    let left = Arc::new(df_expr::Column::new("left", 0)) as Arc<dyn PhysicalExpr>;
    let right =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let binary_expr = df_expr::BinaryExpr::new(left, DFOperator::Eq, right);

    let result = DefaultExpressionConvertor::default()
        .try_convert(
            &(Arc::new(binary_expr) as Arc<dyn PhysicalExpr>),
            &Schema::new(vec![Field::new("left", DataType::Int32, false)]),
        )
        .unwrap()
        .unwrap();

    assert_snapshot!(result.display_tree().to_string(), @r"
    vortex.binary(=)
    ├── lhs: vortex.get_item(left)
    │   └── input: vortex.root()
    └── rhs: vortex.literal(42i32)
    ");
}

#[rstest]
#[case::like_normal(false, false)]
#[case::like_negated(true, false)]
#[case::like_case_insensitive(false, true)]
#[case::like_negated_case_insensitive(true, true)]
fn test_expr_from_df_like(#[case] negated: bool, #[case] case_insensitive: bool) {
    let expr = Arc::new(df_expr::Column::new("text_col", 0)) as Arc<dyn PhysicalExpr>;
    let pattern = Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
        "test%".to_string(),
    )))) as Arc<dyn PhysicalExpr>;
    let like_expr = df_expr::LikeExpr::new(negated, case_insensitive, expr, pattern);

    let result = DefaultExpressionConvertor::default()
        .try_convert(
            &(Arc::new(like_expr) as Arc<dyn PhysicalExpr>),
            &Schema::new(vec![Field::new("text_col", DataType::Utf8, true)]),
        )
        .unwrap()
        .unwrap();
    let like_opts = result.as_::<Like>();
    assert_eq!(
        like_opts,
        &LikeOptions {
            negated,
            case_insensitive
        }
    );
}

#[rstest]
fn test_expr_from_df_octet_length(test_schema: Schema) {
    let expr = Arc::new(df_expr::Column::new("name", 1)) as Arc<dyn PhysicalExpr>;
    let octet_length = octet_length_expr(expr, &test_schema);

    let result = DefaultExpressionConvertor::default()
        .try_convert(&octet_length, &test_schema)
        .unwrap()
        .unwrap();

    assert_snapshot!(result.display_tree().to_string(), @r"
    vortex.cast(i32?)
    └── input: vortex.byte_length()
        └── input: vortex.get_item(name)
            └── input: vortex.root()
    ");
}

#[rstest]
fn test_expr_from_df_array_length(test_schema: Schema) {
    let expr = Arc::new(df_expr::Column::new("tags", 5)) as Arc<dyn PhysicalExpr>;
    let array_length = array_length_expr(vec![expr], &test_schema);

    let result = DefaultExpressionConvertor::default()
        .try_convert(&array_length, &test_schema)
        .unwrap()
        .unwrap();

    assert_snapshot!(result.display_tree().to_string(), @r"
    vortex.cast(u64?)
    └── input: vortex.list.length()
        └── input: vortex.get_item(tags)
            └── input: vortex.root()
    ");
}

#[rstest]
// Supported types
#[case::null(DataType::Null, true)]
#[case::boolean(DataType::Boolean, true)]
#[case::int8(DataType::Int8, true)]
#[case::int16(DataType::Int16, true)]
#[case::int32(DataType::Int32, true)]
#[case::int64(DataType::Int64, true)]
#[case::uint8(DataType::UInt8, true)]
#[case::uint16(DataType::UInt16, true)]
#[case::uint32(DataType::UInt32, true)]
#[case::uint64(DataType::UInt64, true)]
#[case::float32(DataType::Float32, true)]
#[case::float64(DataType::Float64, true)]
#[case::utf8(DataType::Utf8, true)]
#[case::utf8_view(DataType::Utf8View, true)]
#[case::binary(DataType::Binary, true)]
#[case::binary_view(DataType::BinaryView, true)]
#[case::date32(DataType::Date32, true)]
#[case::date64(DataType::Date64, true)]
#[case::timestamp_ms(DataType::Timestamp(ArrowTimeUnit::Millisecond, None), true)]
#[case::timestamp_us(
    DataType::Timestamp(ArrowTimeUnit::Microsecond, Some(Arc::from("UTC"))),
    true
)]
#[case::time32_s(DataType::Time32(ArrowTimeUnit::Second), true)]
#[case::time64_ns(DataType::Time64(ArrowTimeUnit::Nanosecond), true)]
// Unsupported types
#[case::list(
    DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
    false
)]
#[case::struct_type(DataType::Struct(vec![Field::new("field", DataType::Int32, true)].into()
), false)]
// Dictionary types - should be supported if value type is supported
#[case::dict_utf8(
    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
    true
)]
#[case::dict_int32(
    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Int32)),
    true
)]
#[case::dict_unsupported(
    DataType::Dictionary(
        Box::new(DataType::UInt32),
        Box::new(DataType::List(Arc::new(Field::new("item", DataType::Int32, true))))
    ),
    false
)]
fn test_supported_data_types(#[case] data_type: DataType, #[case] expected: bool) {
    assert_eq!(supported_data_types(&data_type), expected);
}

#[rstest]
fn test_can_be_pushed_down_column_supported(test_schema: Schema) {
    let col_expr = Arc::new(df_expr::Column::new("id", 0)) as Arc<dyn PhysicalExpr>;

    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&col_expr, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_nested_column_conversion(test_schema: Schema) {
    let col_expr = Arc::new(df_expr::Column::new("tags", 5)) as Arc<dyn PhysicalExpr>;

    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&col_expr, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_can_be_pushed_down_column_not_found(test_schema: Schema) {
    let col_expr = Arc::new(df_expr::Column::new("nonexistent", 99)) as Arc<dyn PhysicalExpr>;

    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&col_expr, &test_schema)
            .is_err()
    );
}

#[rstest]
fn test_can_be_pushed_down_literal_supported(test_schema: Schema) {
    let lit_expr =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;

    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&lit_expr, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
#[case::duration(ScalarValue::DurationSecond(Some(42)))]
#[case::null_duration(ScalarValue::DurationSecond(None))]
#[case::interval(ScalarValue::IntervalYearMonth(Some(1)))]
#[case::fixed_size_binary(ScalarValue::FixedSizeBinary(5, Some(vec![1, 2, 3, 4, 5])))]
#[case::null_fixed_size_binary(ScalarValue::FixedSizeBinary(5, None))]
fn test_can_be_pushed_down_literal_unsupported(
    test_schema: Schema,
    #[case] value: ScalarValue,
) -> DFResult<()> {
    let lit_expr = Arc::new(df_expr::Literal::new(value)) as Arc<dyn PhysicalExpr>;
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&lit_expr, &test_schema)?
            .is_none()
    );
    Ok(())
}

#[rstest]
fn test_can_be_pushed_down_binary_supported(test_schema: Schema) {
    let left = Arc::new(df_expr::Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
    let right =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let binary_expr =
        Arc::new(df_expr::BinaryExpr::new(left, DFOperator::Eq, right)) as Arc<dyn PhysicalExpr>;

    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&binary_expr, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_can_be_pushed_down_binary_unsupported_operator(test_schema: Schema) {
    let left = Arc::new(df_expr::Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
    let right =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let binary_expr = Arc::new(df_expr::BinaryExpr::new(
        left,
        DFOperator::AtQuestion,
        right,
    )) as Arc<dyn PhysicalExpr>;

    assert!(
        !DefaultExpressionConvertor::default()
            .try_convert(&binary_expr, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_can_be_pushed_down_binary_unsupported_operand(test_schema: Schema) {
    let left = Arc::new(df_expr::Column::new("tags", 5)) as Arc<dyn PhysicalExpr>;
    let right =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(42)))) as Arc<dyn PhysicalExpr>;
    let binary_expr =
        Arc::new(df_expr::BinaryExpr::new(left, DFOperator::Eq, right)) as Arc<dyn PhysicalExpr>;

    assert!(
        !DefaultExpressionConvertor::default()
            .try_convert(&binary_expr, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_can_be_pushed_down_like_supported(test_schema: Schema) {
    let expr = Arc::new(df_expr::Column::new("name", 1)) as Arc<dyn PhysicalExpr>;
    let pattern = Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
        "test%".to_string(),
    )))) as Arc<dyn PhysicalExpr>;
    let like_expr =
        Arc::new(df_expr::LikeExpr::new(false, false, expr, pattern)) as Arc<dyn PhysicalExpr>;

    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&like_expr, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_can_be_pushed_down_like_unsupported_operand(test_schema: Schema) {
    let expr = Arc::new(df_expr::Column::new("tags", 5)) as Arc<dyn PhysicalExpr>;
    let pattern = Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
        "test%".to_string(),
    )))) as Arc<dyn PhysicalExpr>;
    let like_expr =
        Arc::new(df_expr::LikeExpr::new(false, false, expr, pattern)) as Arc<dyn PhysicalExpr>;

    assert!(
        !DefaultExpressionConvertor::default()
            .try_convert(&like_expr, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_can_be_pushed_down_octet_length_supported(test_schema: Schema) {
    let expr = Arc::new(df_expr::Column::new("name", 1)) as Arc<dyn PhysicalExpr>;
    let octet_length = octet_length_expr(expr, &test_schema);

    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&octet_length, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_can_be_pushed_down_octet_length_unsupported_operand(test_schema: Schema) {
    let expr = Arc::new(df_expr::Column::new("tags", 5)) as Arc<dyn PhysicalExpr>;
    let octet_length = Arc::new(ScalarFunctionExpr::new(
        "octet_length",
        Arc::new(ScalarUDF::from(OctetLengthFunc::new())),
        vec![expr],
        Arc::new(Field::new("octet_length", DataType::Int32, true)),
        Arc::new(ConfigOptions::new()),
    )) as Arc<dyn PhysicalExpr>;

    assert!(
        !DefaultExpressionConvertor::default()
            .try_convert(&octet_length, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_can_be_pushed_down_array_length_supported(test_schema: Schema) {
    let expr = Arc::new(df_expr::Column::new("tags", 5)) as Arc<dyn PhysicalExpr>;
    let array_length = array_length_expr(vec![expr], &test_schema);

    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&array_length, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_can_be_pushed_down_array_length_unsupported_operand(test_schema: Schema) {
    // `array_length` over a non-list column cannot be pushed down.
    let expr = Arc::new(df_expr::Column::new("name", 1)) as Arc<dyn PhysicalExpr>;
    let array_length = Arc::new(ScalarFunctionExpr::new(
        "array_length",
        Arc::new(ScalarUDF::from(ArrayLength::new())),
        vec![expr],
        Arc::new(Field::new("array_length", DataType::UInt64, true)),
        Arc::new(ConfigOptions::new()),
    )) as Arc<dyn PhysicalExpr>;

    assert!(
        !DefaultExpressionConvertor::default()
            .try_convert(&array_length, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
fn test_can_be_pushed_down_array_length_dimension_one_supported(test_schema: Schema) {
    // `array_length(arr, 1)` is the first-dimension length, equivalent to `list_length`.
    let list = Arc::new(df_expr::Column::new("tags", 5)) as Arc<dyn PhysicalExpr>;
    let dimension =
        Arc::new(df_expr::Literal::new(ScalarValue::Int64(Some(1)))) as Arc<dyn PhysicalExpr>;
    let array_length = array_length_expr(vec![list, dimension], &test_schema);

    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&array_length, &test_schema)
            .unwrap()
            .is_some()
    );
}

#[rstest]
#[case::higher(Arc::new(df_expr::Literal::new(ScalarValue::Int64(Some(2)))))]
#[case::zero(Arc::new(df_expr::Literal::new(ScalarValue::Int64(Some(0)))))]
#[case::negative(Arc::new(df_expr::Literal::new(ScalarValue::Int64(Some(-1)))))]
#[case::null(Arc::new(df_expr::Literal::new(ScalarValue::Int64(None))))]
#[case::dynamic(Arc::new(df_expr::CastExpr::new(
    Arc::new(df_expr::Column::new("id", 0)),
    DataType::Int64,
    None,
)))]
fn test_array_length_unsupported_dimension(
    test_schema: Schema,
    #[case] dimension: Arc<dyn PhysicalExpr>,
) -> DFResult<()> {
    let list = Arc::new(df_expr::Column::new("tags", 5)) as Arc<dyn PhysicalExpr>;
    let array_length = array_length_expr(vec![list, dimension], &test_schema);
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&array_length, &test_schema)?
            .is_none()
    );
    Ok(())
}

#[rstest]
#[case::octet_zero(OctetLengthFunc::new().into(), 0)]
#[case::octet_two(OctetLengthFunc::new().into(), 2)]
#[case::array_zero(ArrayLength::new().into(), 0)]
#[case::array_three(ArrayLength::new().into(), 3)]
fn test_length_function_invalid_arity(#[case] function: ScalarUDF, #[case] arity: usize) {
    let input = Arc::new(df_expr::Literal::new(ScalarValue::Null)) as Arc<dyn PhysicalExpr>;
    let name = function.name().to_owned();
    let expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        &name,
        Arc::new(function),
        vec![input; arity],
        Arc::new(Field::new("", DataType::Int64, true)),
        Arc::new(ConfigOptions::new()),
    ));
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &Schema::empty())
            .is_err()
    );
}

// https://github.com/vortex-data/vortex/issues/6211
#[tokio::test]
async fn test_cast_int_to_string() -> anyhow::Result<()> {
    let ctx = TestSessionContext::default();

    ctx.session
        .sql(r#"copy (select 1 as id) to 'example.vortex'"#)
        .await?
        .show()
        .await?;

    ctx.session
        .sql(r#"select cast(id as string) as sid from 'example.vortex' where id > 0"#)
        .await?
        .show()
        .await?;

    ctx.session
        .sql(r#"select id from 'example.vortex' where cast (id as string) == '1'"#)
        .await?
        .show()
        .await?;

    // This fails as it pushes string cast to the scan
    ctx.session
        .sql(r#"select cast(id as string) from 'example.vortex'"#)
        .await?
        .collect()
        .await?;

    Ok(())
}

/// A cast whose target is a UUID-tagged `FixedSizeBinary(16)` must resolve
/// through the dtype extension registry (UUID is registered on the default
/// session) instead of the static, non-plugin-aware `DType::from_arrow`,
/// which does not support `FixedSizeBinary` and previously panicked here.
#[test]
fn test_cast_to_uuid_resolves_via_registry() -> anyhow::Result<()> {
    use arrow_schema::extension::Uuid;

    let mut uuid_field = Field::new("id", DataType::FixedSizeBinary(16), true);
    uuid_field.try_with_extension_type(Uuid)?;

    let child = Arc::new(df_expr::Column::new("id", 0)) as Arc<dyn PhysicalExpr>;
    let schema = Schema::new(vec![uuid_field.clone()]);
    let cast: Arc<dyn PhysicalExpr> = Arc::new(df_expr::CastExpr::new_with_target_field(
        child,
        Arc::new(uuid_field),
        None,
    ));

    // Must convert without panicking — the static path would `unimplemented!()`.
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&cast, &schema)?
            .is_some()
    );
    Ok(())
}

/// Test that applying a CASE expression to an Arrow RecordBatch using DataFusion
/// matches the result of applying the converted Vortex expression.
#[test]
fn test_case_when_datafusion_vortex_equivalence() {
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::array::RecordBatch;
    use datafusion_physical_expr::expressions::CaseExpr;
    use vortex::VortexSessionDefault;
    use vortex::array::ArrayRef;
    use vortex::array::Canonical;
    use vortex::array::VortexSessionExecute as _;
    use vortex::session::VortexSession;

    // Create test data
    let values = Arc::new(Int32Array::from(vec![1, 5, 10, 15, 20]));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int32,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![values]).unwrap();

    // Build a DataFusion CASE expression:
    // CASE WHEN value > 10 THEN 100 WHEN value > 5 THEN 50 ELSE 0 END
    let col_value = Arc::new(df_expr::Column::new("value", 0)) as Arc<dyn PhysicalExpr>;
    let lit_10 =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(10)))) as Arc<dyn PhysicalExpr>;
    let lit_5 =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(5)))) as Arc<dyn PhysicalExpr>;
    let lit_100 =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(100)))) as Arc<dyn PhysicalExpr>;
    let lit_50 =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(50)))) as Arc<dyn PhysicalExpr>;
    let lit_0 =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(0)))) as Arc<dyn PhysicalExpr>;

    // WHEN value > 10 THEN 100
    let when1 = Arc::new(df_expr::BinaryExpr::new(
        Arc::clone(&col_value),
        DFOperator::Gt,
        lit_10,
    )) as Arc<dyn PhysicalExpr>;
    // WHEN value > 5 THEN 50
    let when2 = Arc::new(df_expr::BinaryExpr::new(col_value, DFOperator::Gt, lit_5))
        as Arc<dyn PhysicalExpr>;

    let case_expr =
        CaseExpr::try_new(None, vec![(when1, lit_100), (when2, lit_50)], Some(lit_0)).unwrap();

    // Apply DataFusion expression
    let df_result = case_expr.evaluate(&batch).unwrap();
    let df_array = df_result.into_array(batch.num_rows()).unwrap();

    // Convert to Vortex expression
    let expr_convertor = DefaultExpressionConvertor::default();
    let vortex_expr = expr_convertor
        .try_convert(
            &(Arc::new(case_expr) as Arc<dyn PhysicalExpr>),
            &batch.schema(),
        )
        .unwrap()
        .unwrap();

    // Convert batch to Vortex array
    let session = VortexSession::default();
    let vortex_array: ArrayRef = session
        .arrow()
        .from_arrow_record_batch(batch.clone(), &batch.schema())
        .unwrap();

    // Apply Vortex expression
    let mut ctx = session.create_execution_ctx();
    let vortex_result = vortex_array
        .apply(&vortex_expr)
        .unwrap()
        .execute::<Canonical>(&mut ctx)
        .unwrap();

    // Convert back to Arrow for comparison
    let vortex_as_arrow = vortex_result.into_primitive().as_slice::<i32>().to_vec();

    // Convert DataFusion result to Vec for comparison
    let df_as_arrow: Vec<i32> = df_array.as_primitive::<Int32Type>().values().to_vec();

    // Compare results
    // Expected: [0, 0, 50, 100, 100] for values [1, 5, 10, 15, 20]
    // value=1: not > 10, not > 5 -> ELSE 0
    // value=5: not > 10, not > 5 -> ELSE 0
    // value=10: not > 10, > 5 -> 50
    // value=15: > 10 -> 100
    // value=20: > 10 -> 100
    assert_eq!(df_as_arrow, vec![0, 0, 50, 100, 100]);
    assert_eq!(vortex_as_arrow, df_as_arrow);
}

fn assert_native_matches(expr: Arc<dyn PhysicalExpr>, batch: RecordBatch) -> anyhow::Result<()> {
    let session = VortexSession::default();
    let converted = DefaultExpressionConvertor::new(session.clone())
        .try_convert(&expr, &batch.schema())?
        .ok_or_else(|| anyhow::anyhow!("Expected native conversion for {expr}"))?;
    for constant in [false, true] {
        let batches = if constant {
            (0..batch.num_rows()).map(|i| batch.slice(i, 1)).collect()
        } else {
            vec![batch.clone()]
        };
        for batch in batches {
            let mut ctx = session.create_execution_ctx();
            let mut input = session
                .arrow()
                .from_arrow_record_batch(batch.clone(), &batch.schema())?;
            if constant {
                input = ConstantArray::new(input.execute_scalar(0, &mut ctx)?, 1).into_array();
            }
            let actual = input
                .apply(&converted)?
                .execute::<vortex::array::ArrayRef>(&mut ctx);
            let expected = expr
                .evaluate(&batch)
                .and_then(|value| value.into_array(batch.num_rows()));
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => {
                    let expected = session
                        .arrow()
                        .from_arrow_array(expected, expr.return_field(&batch.schema())?.as_ref())?;
                    assert_arrays_eq!(actual, expected, &mut ctx);
                }
                (Err(_), Err(_)) => {}
                (actual, expected) => anyhow::bail!(
                    "Evaluation mismatch for {expr}: Vortex {actual:?}, DataFusion {expected:?}"
                ),
            }
        }
    }
    Ok(())
}

#[rstest]
#[case::ordinary(vec![Some(i32::MIN), Some(i32::MAX), Some(-1), Some(0), None], vec![2, 2, 2, 2, 0])]
#[case::zero(vec![Some(1)], vec![0])]
#[case::overflow(vec![Some(i32::MIN)], vec![-1])]
fn test_native_integer_division(
    #[case] values: Vec<Option<i32>>,
    #[case] denominators: Vec<i32>,
) -> anyhow::Result<()> {
    let batch = arrow_array::record_batch!(("a", Int32, values), ("b", Int32, denominators))?;
    assert_native_matches(
        Arc::new(df_expr::BinaryExpr::new(
            Arc::new(df_expr::Column::new("a", 0)),
            DFOperator::Divide,
            Arc::new(df_expr::Column::new("b", 1)),
        )),
        batch,
    )
}

#[rstest]
#[case::add(DFOperator::Plus)]
#[case::sub(DFOperator::Minus)]
#[case::mul(DFOperator::Multiply)]
#[case::div(DFOperator::Divide)]
fn test_native_float_arithmetic(#[case] operator: DFOperator) -> anyhow::Result<()> {
    let batch = arrow_array::record_batch!(
        (
            "a",
            Float64,
            vec![
                Some(f64::NAN),
                Some(f64::INFINITY),
                Some(-0.0),
                Some(f64::MAX),
                None
            ]
        ),
        ("b", Float64, vec![0.0, f64::NEG_INFINITY, 0.0, 2.0, 0.0])
    )?;
    assert_native_matches(
        Arc::new(df_expr::BinaryExpr::new(
            Arc::new(df_expr::Column::new("a", 0)),
            operator,
            Arc::new(df_expr::Column::new("b", 1)),
        )),
        batch,
    )
}

#[rstest]
#[case::identity(DataType::Int32)]
#[case::widen(DataType::Int64)]
fn test_native_integer_cast(#[case] target: DataType) -> anyhow::Result<()> {
    let batch = arrow_array::record_batch!((
        "a",
        Int32,
        vec![Some(i32::MIN), Some(i32::MAX), Some(0), None]
    ))?;
    assert_native_matches(
        Arc::new(df_expr::CastExpr::new(
            Arc::new(df_expr::Column::new("a", 0)),
            target,
            None,
        )),
        batch,
    )
}

#[test]
fn test_native_float_cast() -> anyhow::Result<()> {
    let batch = arrow_array::record_batch!((
        "a",
        Float32,
        vec![
            Some(f32::NAN),
            Some(f32::INFINITY),
            Some(-0.0),
            Some(f32::MAX),
            None
        ]
    ))?;
    assert_native_matches(
        Arc::new(df_expr::CastExpr::new(
            Arc::new(df_expr::Column::new("a", 0)),
            DataType::Float64,
            None,
        )),
        batch,
    )
}

#[rstest]
#[case::narrow(DataType::Int8)]
#[case::signedness(DataType::UInt32)]
#[case::string(DataType::Utf8)]
#[case::decimal(DataType::Decimal128(10, 2))]
fn test_cast_falls_back(#[case] target: DataType) -> DFResult<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::CastExpr::new(
        Arc::new(df_expr::Column::new("a", 0)),
        target,
        None,
    ));
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &schema)?
            .is_none()
    );
    Ok(())
}

#[test]
fn test_cast_options_fall_back() -> DFResult<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::CastExpr::new(
        Arc::new(df_expr::Column::new("a", 0)),
        DataType::Int64,
        Some(datafusion_common::arrow::compute::CastOptions {
            safe: true,
            format_options: DEFAULT_FORMAT_OPTIONS,
        }),
    ));
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &schema)?
            .is_none()
    );
    Ok(())
}

#[rstest]
#[case::add(DFOperator::Plus)]
#[case::sub(DFOperator::Minus)]
#[case::mul(DFOperator::Multiply)]
#[case::div(DFOperator::Divide)]
fn test_integer_arithmetic_pushdown_ignores_overflow_mode(
    #[case] op: DFOperator,
    #[values(false, true)] checked: bool,
) -> DFResult<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let expr: Arc<dyn PhysicalExpr> = Arc::new(
        df_expr::BinaryExpr::new(
            Arc::new(df_expr::Column::new("a", 0)),
            op,
            Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(1)))),
        )
        .with_fail_on_overflow(checked),
    );
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &schema)?
            .is_some()
    );
    Ok(())
}

#[rstest]
#[case::add(DFOperator::Plus)]
#[case::sub(DFOperator::Minus)]
#[case::mul(DFOperator::Multiply)]
#[case::div(DFOperator::Divide)]
fn test_native_integer_arithmetic(
    #[case] op: DFOperator,
    #[values(false, true)] checked: bool,
) -> anyhow::Result<()> {
    let batch = arrow_array::record_batch!(
        ("a", Int32, vec![Some(-12), Some(0), Some(12), None]),
        ("b", Int32, vec![-2, 2, 2, 0])
    )?;
    assert_native_matches(
        Arc::new(
            df_expr::BinaryExpr::new(
                Arc::new(df_expr::Column::new("a", 0)),
                op,
                Arc::new(df_expr::Column::new("b", 1)),
            )
            .with_fail_on_overflow(checked),
        ),
        batch,
    )
}

#[rstest]
#[case::add(DFOperator::Plus)]
#[case::sub(DFOperator::Minus)]
#[case::mul(DFOperator::Multiply)]
#[case::div(DFOperator::Divide)]
fn test_native_decimal_arithmetic(#[case] op: DFOperator) -> anyhow::Result<()> {
    let batch = RecordBatch::try_from_iter([
        (
            "a",
            Arc::new(
                arrow_array::Decimal128Array::from(vec![Some(-1200), Some(0), Some(1200), None])
                    .with_precision_and_scale(12, 2)?,
            ) as arrow_array::ArrayRef,
        ),
        (
            "b",
            Arc::new(
                arrow_array::Decimal128Array::from(vec![-200, 200, 200, 0])
                    .with_precision_and_scale(12, 2)?,
            ) as arrow_array::ArrayRef,
        ),
    ])?;
    assert_native_matches(
        Arc::new(df_expr::BinaryExpr::new(
            Arc::new(df_expr::Column::new("a", 0)),
            op,
            Arc::new(df_expr::Column::new("b", 1)),
        )),
        batch,
    )
}

#[test]
fn test_fallible_case_branch_is_residual() -> DFResult<()> {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let a: Arc<dyn PhysicalExpr> = Arc::new(df_expr::Column::new("a", 0));
    let zero: Arc<dyn PhysicalExpr> = Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(0))));
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::CaseExpr::try_new(
        None,
        vec![(
            Arc::new(df_expr::BinaryExpr::new(
                Arc::clone(&a),
                DFOperator::NotEq,
                Arc::clone(&zero),
            )),
            Arc::new(df_expr::BinaryExpr::new(
                Arc::clone(&zero),
                DFOperator::Divide,
                a,
            )),
        )],
        Some(zero),
    )?);
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &schema)?
            .is_none()
    );
    Ok(())
}

#[rstest]
fn test_nested_functions_reject_unknown_children(
    #[values(false, true)] list: bool,
) -> DFResult<()> {
    let input_type = if list {
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
    } else {
        DataType::Struct(vec![Field::new("leaf", DataType::Int32, true)].into())
    };
    let field = Arc::new(Field::new("a", input_type, true));
    let schema = Schema::new(vec![Arc::clone(&field)]);
    let unknown: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        "coalesce",
        Arc::new(ScalarUDF::from(CoalesceFunc::new())),
        vec![Arc::new(df_expr::Column::new("a", 0))],
        field,
        Arc::new(ConfigOptions::new()),
    ));
    let expr = if list {
        array_length_expr(vec![unknown], &schema)
    } else {
        Arc::new(ScalarFunctionExpr::try_new(
            Arc::new(ScalarUDF::from(GetFieldFunc::new())),
            vec![
                unknown,
                Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
                    "leaf".into(),
                )))),
            ],
            &schema,
            Arc::new(ConfigOptions::new()),
        )?) as Arc<dyn PhysicalExpr>
    };
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &schema)?
            .is_none()
    );
    Ok(())
}

#[rstest]
#[case::no_args(vec![])]
#[case::no_path(vec![Arc::new(df_expr::Column::new("a", 0)) as Arc<dyn PhysicalExpr>])]
#[case::null_path(vec![
    Arc::new(df_expr::Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
    Arc::new(df_expr::Literal::new(ScalarValue::Utf8(None))),
])]
#[case::column_path(vec![
    Arc::new(df_expr::Column::new("a", 0)) as Arc<dyn PhysicalExpr>,
    Arc::new(df_expr::Column::new("a", 0)),
])]
fn test_malformed_get_field_returns_error(
    #[case] args: Vec<Arc<dyn PhysicalExpr>>,
) -> DFResult<()> {
    let schema = Schema::new(vec![Field::new(
        "a",
        DataType::Struct(vec![Field::new("leaf", DataType::Int32, true)].into()),
        true,
    )]);
    let expr: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::new(
        "get_field",
        Arc::new(ScalarUDF::from(GetFieldFunc::new())),
        args,
        Arc::new(Field::new("", DataType::Int32, true)),
        Arc::new(ConfigOptions::new()),
    ));
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &schema)
            .is_err()
    );
    Ok(())
}

#[test]
fn test_mismatched_column_identity_returns_error() {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, true),
    ]);
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::Column::new("a", 1));
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &schema)
            .is_err()
    );
}

#[rstest]
fn test_native_nested_list_length(
    #[values(false, true)] nullable_parent: bool,
) -> anyhow::Result<()> {
    let mut lists =
        arrow_array::builder::ListBuilder::new(arrow_array::builder::Int32Builder::new());
    lists.values().append_value(1);
    lists.append(true);
    lists.append(true);
    lists.append(false);
    let lists = Arc::new(lists.finish());
    let field = Arc::new(Field::new("items", lists.data_type().clone(), true));
    let payload = Arc::new(StructArray::new(
        vec![field].into(),
        vec![lists],
        nullable_parent.then(|| NullBuffer::from(vec![true, false, true])),
    ));
    let schema = Arc::new(Schema::new(vec![Field::new(
        "payload",
        payload.data_type().clone(),
        nullable_parent,
    )]));
    let batch = RecordBatch::try_new(schema, vec![payload])?;
    let get_field: Arc<dyn PhysicalExpr> = Arc::new(ScalarFunctionExpr::try_new(
        Arc::new(ScalarUDF::from(GetFieldFunc::new())),
        vec![
            Arc::new(df_expr::Column::new("payload", 0)),
            Arc::new(df_expr::Literal::new(ScalarValue::Utf8(Some(
                "items".into(),
            )))),
        ],
        &batch.schema(),
        Arc::new(ConfigOptions::new()),
    )?);
    let length = array_length_expr(vec![get_field], &batch.schema());
    if nullable_parent {
        assert!(
            DefaultExpressionConvertor::default()
                .try_convert(&length, &batch.schema())?
                .is_none()
        );
        Ok(())
    } else {
        assert_native_matches(length, batch)
    }
}

#[test]
fn test_native_case_null_conditions() -> anyhow::Result<()> {
    let batch = arrow_array::record_batch!(("a", Int32, vec![Some(1), None, Some(3)]))?;
    let a: Arc<dyn PhysicalExpr> = Arc::new(df_expr::Column::new("a", 0));
    let expr = Arc::new(df_expr::CaseExpr::try_new(
        None,
        vec![(
            Arc::new(df_expr::BinaryExpr::new(
                Arc::clone(&a),
                DFOperator::Gt,
                Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(2)))),
            )) as Arc<dyn PhysicalExpr>,
            a,
        )],
        None,
    )?);
    assert_native_matches(expr, batch)
}

#[test]
fn test_simple_case_falls_back() -> DFResult<()> {
    let value =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::CaseExpr::try_new(
        Some(Arc::clone(&value)),
        vec![(Arc::clone(&value), value)],
        None,
    )?);
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &Schema::empty())?
            .is_none()
    );
    Ok(())
}

#[test]
fn test_case_non_boolean_condition_returns_error() -> DFResult<()> {
    let value =
        Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(1)))) as Arc<dyn PhysicalExpr>;
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::CaseExpr::try_new(
        None,
        vec![(Arc::clone(&value), value)],
        None,
    )?);
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &Schema::empty())
            .is_err()
    );
    Ok(())
}

#[test]
fn test_malformed_literal_returns_error() {
    let value = ScalarValue::Decimal128(Some(1), 0, 0);
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::Literal::new(value));
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &Schema::empty())
            .is_err()
    );
}

#[rstest]
fn test_literal_invalid_row_count(#[values(0, 2)] len: usize) {
    let array = StructArray::new_empty_fields(len, None);
    let expr: Arc<dyn PhysicalExpr> =
        Arc::new(df_expr::Literal::new(ScalarValue::Struct(Arc::new(array))));
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &Schema::empty())
            .is_err()
    );
}

#[rstest]
#[case::and(DFOperator::And, DFOperator::NotEq)]
#[case::or(DFOperator::Or, DFOperator::Eq)]
fn test_fallible_boolean_rhs_is_residual(
    #[case] operator: DFOperator,
    #[case] comparison: DFOperator,
) -> anyhow::Result<()> {
    let batch = arrow_array::record_batch!(("a", Int32, vec![0, 0, 0, 0, 2]))?;
    let a: Arc<dyn PhysicalExpr> = Arc::new(df_expr::Column::new("a", 0));
    let zero: Arc<dyn PhysicalExpr> = Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(0))));
    let expr: Arc<dyn PhysicalExpr> = Arc::new(df_expr::BinaryExpr::new(
        Arc::new(df_expr::BinaryExpr::new(
            Arc::clone(&a),
            comparison,
            Arc::clone(&zero),
        )),
        operator,
        Arc::new(df_expr::BinaryExpr::new(
            Arc::new(df_expr::BinaryExpr::new(
                Arc::new(df_expr::Literal::new(ScalarValue::Int32(Some(12)))),
                DFOperator::Divide,
                a,
            )),
            DFOperator::Gt,
            zero,
        )),
    ));
    // DataFusion selects the one row that needs RHS evaluation at this selectivity.
    expr.evaluate(&batch)?;
    assert!(
        DefaultExpressionConvertor::default()
            .try_convert(&expr, &batch.schema())?
            .is_none()
    );
    Ok(())
}
