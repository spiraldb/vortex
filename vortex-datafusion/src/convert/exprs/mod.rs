// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;

use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use datafusion_common::Result as DFResult;
use datafusion_common::ScalarValue;
use datafusion_common::exec_datafusion_err;
use datafusion_common::format::DEFAULT_FORMAT_OPTIONS;
use datafusion_expr::Operator as DFOperator;
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_functions::string::octet_length::OctetLengthFunc;
use datafusion_functions_nested::length::ArrayLength;
use datafusion_physical_expr::DynamicFilterTracking;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::projection::ProjectionExpr;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_plan::expressions as df_expr;
use itertools::Itertools;
use vortex::VortexSessionDefault;
use vortex::array::VortexSessionExecute;
use vortex::dtype::DType;
use vortex::dtype::Nullability;
use vortex::expr::Expression;
use vortex::expr::analysis::label_infallible;
use vortex::expr::and_collect;
use vortex::expr::byte_length;
use vortex::expr::cast;
use vortex::expr::get_item;
use vortex::expr::is_not_null;
use vortex::expr::is_null;
use vortex::expr::list_length;
use vortex::expr::lit;
use vortex::expr::nested_case_when;
use vortex::expr::or_collect;
use vortex::expr::pack;
use vortex::expr::root;
use vortex::scalar_fn::ScalarFnVTableExt;
use vortex::scalar_fn::fns::binary::Binary;
use vortex::scalar_fn::fns::like::Like;
use vortex::scalar_fn::fns::like::LikeOptions;
use vortex::scalar_fn::fns::operators::Operator;
use vortex::session::VortexSession;
use vortex_arrow::ArrowSessionExt;

#[cfg(test)]
mod tests;

/// Result of splitting a projection into Vortex expressions and leftover DataFusion projections.
pub struct ProcessedProjection {
    /// Projection evaluated by the Vortex scan.
    pub scan_projection: Expression,
    /// Arrow reference types and metadata for the scan output.
    pub scan_reference_schema: Schema,
    /// Projection evaluated by DataFusion after the Vortex scan.
    pub leftover_projection: ProjectionExprs,
}

/// Trait for converting DataFusion expressions to Vortex ones.
///
/// Custom convertors implement a single schema-aware decision. Conversion should preserve
/// DataFusion values, nulls, and evaluation errors; see [`DefaultExpressionConvertor`] for
/// the temporary arithmetic exception. Unsupported expressions remain in DataFusion,
/// including when a file's schema adapter introduces them.
///
/// # Implementing a custom convertor
///
///     use std::sync::Arc;
///
///     use arrow_schema::Schema;
///     use datafusion_common::Result as DFResult;
///     use datafusion_physical_expr::PhysicalExpr;
///     use vortex::expr::Expression;
///     use vortex_datafusion::convert::DefaultExpressionConvertor;
///     use vortex_datafusion::convert::ExpressionConvertor;
///
///     struct CustomExpressionConvertor(DefaultExpressionConvertor);
///
///     impl ExpressionConvertor for CustomExpressionConvertor {
///         fn try_convert(
///             &self,
///             expr: &Arc<dyn PhysicalExpr>,
///             schema: &Schema,
///         ) -> DFResult<Option<Expression>> {
///             self.0.try_convert(expr, schema)
///         }
///     }
pub trait ExpressionConvertor: Send + Sync {
    /// Convert an expression for native evaluation against this schema.
    ///
    /// Returns None for valid but unsupported expressions. Malformed expressions and
    /// conversion failures return errors. Callers must retain unsupported exact predicates
    /// for DataFusion evaluation.
    fn try_convert(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> DFResult<Option<Expression>>;

    /// Split a projection into native and DataFusion evaluation.
    ///
    /// If any expression is unsupported, evaluate the complete projection in DataFusion
    /// over deduplicated raw inputs. This avoids mixing input names with output aliases.
    fn split_projection(
        &self,
        source_projection: ProjectionExprs,
        input_schema: &Schema,
        output_schema: &Schema,
    ) -> DFResult<ProcessedProjection> {
        // Duplicate output names cannot identify native pack fields unambiguously.
        if !source_projection
            .iter()
            .map(|projection| &projection.alias)
            .all_unique()
        {
            return self.no_pushdown_projection(source_projection, input_schema);
        }
        let mut scan_projection = Vec::with_capacity(source_projection.as_ref().len());
        for projection in source_projection.as_ref() {
            let Some(expr) = self.try_convert(&projection.expr, input_schema)? else {
                return self.no_pushdown_projection(source_projection, input_schema);
            };
            scan_projection.push((projection.alias.clone(), expr));
        }
        Ok(ProcessedProjection {
            scan_projection: pack(scan_projection, Nullability::NonNullable),
            scan_reference_schema: output_schema.clone(),
            leftover_projection: source_projection
                .iter()
                .enumerate()
                .map(|(index, projection)| ProjectionExpr {
                    expr: Arc::new(df_expr::Column::new(&projection.alias, index)),
                    alias: projection.alias.clone(),
                })
                .collect::<Vec<_>>()
                .into(),
        })
    }

    /// Read the required raw columns and apply the complete projection in DataFusion.
    fn no_pushdown_projection(
        &self,
        source_projection: ProjectionExprs,
        input_schema: &Schema,
    ) -> DFResult<ProcessedProjection> {
        raw_projection(source_projection, input_schema)
    }
}

/// Read raw columns in file-index order without involving custom expression conversion.
pub(crate) fn raw_projection(
    source_projection: ProjectionExprs,
    input_schema: &Schema,
) -> DFResult<ProcessedProjection> {
    let column_indices = source_projection.column_indices();
    let mut scan_columns = Vec::with_capacity(column_indices.len());
    let mut fields = Vec::with_capacity(column_indices.len());
    for index in column_indices {
        let field = input_schema.fields().get(index).ok_or_else(|| {
            exec_datafusion_err!("Projection column index {index} is out of bounds")
        })?;
        scan_columns.push((
            field.name().clone(),
            get_item(field.name().as_str(), root()),
        ));
        fields.push(Arc::clone(field));
    }
    Ok(ProcessedProjection {
        scan_projection: pack(scan_columns, Nullability::NonNullable),
        scan_reference_schema: Schema::new_with_metadata(fields, input_schema.metadata().clone()),
        leftover_projection: source_projection,
    })
}

/// The default schema-aware DataFusion expression convertor.
///
/// Supported arithmetic is pushed down using Vortex semantics, including its checked
/// integer arithmetic. Matching DataFusion's overflow behavior is deferred to a future patch.
/// Other expressions require compatible SQL semantics or remain in DataFusion.
pub struct DefaultExpressionConvertor {
    session: VortexSession,
}

impl Default for DefaultExpressionConvertor {
    fn default() -> Self {
        Self::new(VortexSession::default())
    }
}

impl DefaultExpressionConvertor {
    /// Create a convertor that resolves Arrow extension types using the session registry.
    pub fn new(session: VortexSession) -> Self {
        Self { session }
    }

    fn convert_expr(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        schema: &Schema,
        input_dtype: &DType,
    ) -> DFResult<Option<Expression>> {
        let converted = if let Some(binary) = expr.downcast_ref::<df_expr::BinaryExpr>() {
            let Some(operator) = try_operator_from_df(binary.op()) else {
                return Ok(None);
            };
            let boolean_operator = matches!(operator, Operator::And | Operator::Or);
            let left_type = binary.left().data_type(schema)?;
            let right_type = binary.right().data_type(schema)?;
            if boolean_operator {
                if left_type != DataType::Boolean || right_type != DataType::Boolean {
                    return Err(exec_datafusion_err!(
                        "Boolean operator requires Boolean operands: {expr}"
                    ));
                }
            } else if !supported_data_types(&left_type) || !supported_data_types(&right_type) {
                return Ok(None);
            }
            let (Some(left), Some(right)) = (
                self.convert_expr(binary.left(), schema, input_dtype)?,
                self.convert_expr(binary.right(), schema, input_dtype)?,
            ) else {
                return Ok(None);
            };
            if boolean_operator
                && (label_infallible(&left).get(&left) != Some(&true)
                    || label_infallible(&right).get(&right) != Some(&true))
            {
                // DataFusion may evaluate the RHS only on rows selected by the LHS.
                return Ok(None);
            }
            let (Ok(left_dtype), Ok(right_dtype)) = (
                left.return_dtype(input_dtype),
                right.return_dtype(input_dtype),
            ) else {
                return Ok(None);
            };
            if !left_dtype.eq_ignore_nullability(&right_dtype) {
                return Ok(None);
            }
            Binary.new_expr(operator, [left, right])
        } else if let Some(column) = expr.downcast_ref::<df_expr::Column>() {
            get_item(column.name(), root())
        } else if let Some(literal) = expr.downcast_ref::<df_expr::Literal>() {
            let field = literal.return_field(schema)?;
            let array = literal.value().to_array()?;
            if array.len() != 1 {
                return Err(exec_datafusion_err!(
                    "Literal must contain exactly one value, found {}",
                    array.len()
                ));
            }
            let array = match self.session.arrow().from_arrow_array(array, &field) {
                Ok(array) => array,
                Err(error) => {
                    if self.session.arrow().from_arrow_field(&field).is_err() {
                        return Ok(None);
                    }
                    return Err(exec_datafusion_err!(
                        "Failed to convert literal {expr}: {error}"
                    ));
                }
            };
            lit(array
                .execute_scalar(0, &mut self.session.create_execution_ctx())
                .map_err(|e| exec_datafusion_err!("Failed to evaluate literal {expr}: {e}"))?)
        } else if let Some(cast_expr) = expr.downcast_ref::<df_expr::CastExpr>() {
            if !supported_cast(cast_expr, schema)? {
                return Ok(None);
            }
            let Some(child) = self.convert_expr(cast_expr.expr(), schema, input_dtype)? else {
                return Ok(None);
            };
            let Ok(target) = self
                .session
                .arrow()
                .from_arrow_field(cast_expr.target_field())
            else {
                return Ok(None);
            };
            let Ok(child_dtype) = child.return_dtype(input_dtype) else {
                return Ok(None);
            };
            // Matching Arrow storage types do not imply matching extension semantics.
            if (child_dtype.is_extension() || target.is_extension())
                && !child_dtype.eq_ignore_nullability(&target)
            {
                return Ok(None);
            }
            cast(child, target)
        } else if let Some(is_null_expr) = expr.downcast_ref::<df_expr::IsNullExpr>() {
            let Some(child) = self.convert_expr(is_null_expr.arg(), schema, input_dtype)? else {
                return Ok(None);
            };
            is_null(child)
        } else if let Some(is_not_null_expr) = expr.downcast_ref::<df_expr::IsNotNullExpr>() {
            let Some(child) = self.convert_expr(is_not_null_expr.arg(), schema, input_dtype)?
            else {
                return Ok(None);
            };
            is_not_null(child)
        } else if let Some(like) = expr.downcast_ref::<df_expr::LikeExpr>() {
            if !like.expr().data_type(schema)?.is_string()
                || !like.pattern().data_type(schema)?.is_string()
            {
                return Ok(None);
            }
            let (Some(child), Some(pattern)) = (
                self.convert_expr(like.expr(), schema, input_dtype)?,
                self.convert_expr(like.pattern(), schema, input_dtype)?,
            ) else {
                return Ok(None);
            };
            Like.new_expr(
                LikeOptions {
                    negated: like.negated(),
                    case_insensitive: like.case_insensitive(),
                },
                [child, pattern],
            )
        } else if let Some(in_list) = expr.downcast_ref::<df_expr::InListExpr>() {
            return self.convert_in_list(in_list, schema, input_dtype);
        } else if let Some(scalar_fn) = expr.downcast_ref::<ScalarFunctionExpr>() {
            return self.convert_scalar_function(scalar_fn, schema, input_dtype);
        } else if let Some(case_expr) = expr.downcast_ref::<df_expr::CaseExpr>() {
            if case_expr.expr().is_some() {
                return Ok(None);
            }
            let mut pairs = Vec::with_capacity(case_expr.when_then_expr().len());
            for (when, then) in case_expr.when_then_expr() {
                if when.data_type(schema)? != DataType::Boolean {
                    return Err(exec_datafusion_err!("CASE WHEN must be Boolean"));
                }
                let (Some(when), Some(then)) = (
                    self.convert_expr(when, schema, input_dtype)?,
                    self.convert_expr(then, schema, input_dtype)?,
                ) else {
                    return Ok(None);
                };
                pairs.push((when, then));
            }
            let otherwise = match case_expr.else_expr() {
                Some(expr) => {
                    let Some(expr) = self.convert_expr(expr, schema, input_dtype)? else {
                        return Ok(None);
                    };
                    Some(expr)
                }
                None => None,
            };
            let case = nested_case_when(pairs, otherwise);
            // Vortex may evaluate branch values on rows excluded by the condition.
            if label_infallible(&case).get(&case) != Some(&true) {
                return Ok(None);
            }
            case
        } else {
            return Ok(None);
        };
        Ok(Some(converted))
    }

    fn convert_in_list(
        &self,
        in_list: &df_expr::InListExpr,
        schema: &Schema,
        input_dtype: &DType,
    ) -> DFResult<Option<Expression>> {
        if in_list.is_empty()
            || !in_list
                .list()
                .iter()
                .all(|expr| expr.is::<df_expr::Literal>())
            || !supported_data_types(&in_list.expr().data_type(schema)?)
        {
            return Ok(None);
        }
        let Some(value) = self.convert_expr(in_list.expr(), schema, input_dtype)? else {
            return Ok(None);
        };
        // Boolean rewrites may skip evaluating the input, particularly for all-null lists.
        if label_infallible(&value).get(&value) != Some(&true) {
            return Ok(None);
        }
        let Ok(value_dtype) = value.return_dtype(input_dtype) else {
            return Ok(None);
        };
        let operator = if in_list.negated() {
            Operator::NotEq
        } else {
            Operator::Eq
        };
        let mut comparisons = Vec::with_capacity(in_list.len());
        for element in in_list.list() {
            let Some(element) = self.convert_expr(element, schema, input_dtype)? else {
                return Ok(None);
            };
            let Ok(element_dtype) = element.return_dtype(input_dtype) else {
                return Ok(None);
            };
            if element_dtype == DType::Null {
                comparisons.push(lit(None::<bool>));
            } else if value_dtype.eq_ignore_nullability(&element_dtype) {
                comparisons.push(Binary.new_expr(operator, [value.clone(), element]));
            } else {
                return Ok(None);
            }
        }
        // Kleene AND/OR preserve SQL IN/NOT IN nulls; list_contains does not.
        Ok(if in_list.negated() {
            and_collect(comparisons)
        } else {
            or_collect(comparisons)
        })
    }

    fn convert_scalar_function(
        &self,
        scalar_fn: &ScalarFunctionExpr,
        schema: &Schema,
        input_dtype: &DType,
    ) -> DFResult<Option<Expression>> {
        if ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(scalar_fn).is_some() {
            let [source, paths @ ..] = scalar_fn.args() else {
                return Err(exec_datafusion_err!(
                    "get_field requires a source and field path"
                ));
            };
            if paths.is_empty() {
                return Err(exec_datafusion_err!("get_field requires a field path"));
            }
            let mut source_type = source.data_type(schema)?;
            let mut nullable = source.nullable(schema)?;
            let mut nullable_struct = false;
            let mut names = Vec::with_capacity(paths.len());
            for path in paths {
                let name = path
                    .downcast_ref::<df_expr::Literal>()
                    .and_then(|literal| literal.value().try_as_str().flatten())
                    .ok_or_else(|| {
                        exec_datafusion_err!("get_field path must be a non-null string literal")
                    })?;
                let DataType::Struct(fields) = &source_type else {
                    return Ok(None);
                };
                nullable_struct |= nullable;
                let field = fields
                    .iter()
                    .find(|field| field.name() == name)
                    .ok_or_else(|| {
                        exec_datafusion_err!("get_field references missing field {name}")
                    })?;
                nullable = field.is_nullable();
                source_type = field.data_type().clone();
                names.push(name);
            }
            // DataFusion extracts the child without applying parent struct validity;
            // Vortex get_item masks the child when its parent is null.
            if nullable_struct {
                return Ok(None);
            }
            let Some(mut result) = self.convert_expr(source, schema, input_dtype)? else {
                return Ok(None);
            };
            for name in names {
                result = get_item(name, result);
            }
            return Ok(Some(result));
        }

        let (input, length): (_, fn(Expression) -> Expression) =
            if ScalarFunctionExpr::try_downcast_func::<OctetLengthFunc>(scalar_fn).is_some() {
                let [input] = scalar_fn.args() else {
                    return Err(exec_datafusion_err!(
                        "octet_length requires exactly one argument"
                    ));
                };
                let data_type = input.data_type(schema)?;
                let data_type = match &data_type {
                    DataType::Dictionary(_, value) => value.as_ref(),
                    data_type => data_type,
                };
                if !data_type.is_binary() && !data_type.is_string() {
                    return Ok(None);
                }
                (input, byte_length)
            } else if ScalarFunctionExpr::try_downcast_func::<ArrayLength>(scalar_fn).is_some() {
                let Some(input) = array_length_input(scalar_fn)? else {
                    return Ok(None);
                };
                if !matches!(
                    input.data_type(schema)?,
                    DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
                ) {
                    return Ok(None);
                }
                (input, list_length)
            } else {
                return Ok(None);
            };
        let Some(input) = self.convert_expr(input, schema, input_dtype)? else {
            return Ok(None);
        };
        let Ok(return_dtype) = self.session.arrow().from_arrow_field(&Field::new(
            "",
            scalar_fn.return_type().clone(),
            scalar_fn.nullable(),
        )) else {
            return Ok(None);
        };
        Ok(Some(cast(length(input), return_dtype)))
    }
}

impl ExpressionConvertor for DefaultExpressionConvertor {
    fn try_convert(
        &self,
        expr: &Arc<dyn PhysicalExpr>,
        schema: &Schema,
    ) -> DFResult<Option<Expression>> {
        if DynamicFilterTracking::classify(expr).contains_dynamic_filter() {
            return Ok(None);
        }
        for column in collect_columns(expr) {
            let field = schema.fields().get(column.index()).ok_or_else(|| {
                exec_datafusion_err!(
                    "Column {}@{} is out of bounds",
                    column.name(),
                    column.index()
                )
            })?;
            if field.name() != column.name() {
                return Err(exec_datafusion_err!(
                    "Column {}@{} refers to field {}",
                    column.name(),
                    column.index(),
                    field.name()
                ));
            }
        }
        let Ok(input_dtype) = self.session.arrow().from_arrow_schema(schema) else {
            return Ok(None);
        };
        let Some(converted) = self.convert_expr(expr, schema, &input_dtype)? else {
            return Ok(None);
        };
        let Ok(expected_dtype) = self
            .session
            .arrow()
            .from_arrow_field(expr.return_field(schema)?.as_ref())
        else {
            return Ok(None);
        };
        let Ok(actual_dtype) = converted.return_dtype(&input_dtype) else {
            return Ok(None);
        };
        if !actual_dtype.eq_ignore_nullability(&expected_dtype) {
            return Ok(None);
        }
        Ok(Some(converted))
    }
}

fn supported_cast(cast: &df_expr::CastExpr, schema: &Schema) -> DFResult<bool> {
    use DataType::*;
    let options = cast.cast_options();
    if options.safe
        || options.format_options != DEFAULT_FORMAT_OPTIONS
        || (cast.expr().nullable(schema)? && !cast.target_field().is_nullable())
    {
        return Ok(false);
    }
    let source = cast.expr().data_type(schema)?;
    let target = cast.cast_type();
    Ok(source == *target
        || matches!(
            (&source, target),
            (Int8, Int16 | Int32 | Int64)
                | (Int16, Int32 | Int64)
                | (Int32, Int64)
                | (UInt8, UInt16 | UInt32 | UInt64)
                | (UInt16, UInt32 | UInt64)
                | (UInt32, UInt64)
                | (Float32, Float64)
        ))
}

fn try_operator_from_df(value: &DFOperator) -> Option<Operator> {
    match value {
        DFOperator::Eq => Some(Operator::Eq),
        DFOperator::NotEq => Some(Operator::NotEq),
        DFOperator::Lt => Some(Operator::Lt),
        DFOperator::LtEq => Some(Operator::Lte),
        DFOperator::Gt => Some(Operator::Gt),
        DFOperator::GtEq => Some(Operator::Gte),
        DFOperator::And => Some(Operator::And),
        DFOperator::Or => Some(Operator::Or),
        DFOperator::Plus => Some(Operator::Add),
        DFOperator::Minus => Some(Operator::Sub),
        DFOperator::Multiply => Some(Operator::Mul),
        DFOperator::Divide => Some(Operator::Div),
        DFOperator::IsDistinctFrom
        | DFOperator::IsNotDistinctFrom
        | DFOperator::RegexMatch
        | DFOperator::RegexIMatch
        | DFOperator::RegexNotMatch
        | DFOperator::RegexNotIMatch
        | DFOperator::LikeMatch
        | DFOperator::ILikeMatch
        | DFOperator::NotLikeMatch
        | DFOperator::NotILikeMatch
        | DFOperator::BitwiseAnd
        | DFOperator::BitwiseOr
        | DFOperator::BitwiseXor
        | DFOperator::BitwiseShiftRight
        | DFOperator::BitwiseShiftLeft
        | DFOperator::StringConcat
        | DFOperator::AtArrow
        | DFOperator::ArrowAt
        | DFOperator::Modulo
        | DFOperator::Arrow
        | DFOperator::LongArrow
        | DFOperator::HashArrow
        | DFOperator::HashLongArrow
        | DFOperator::AtAt
        | DFOperator::IntegerDivide
        | DFOperator::HashMinus
        | DFOperator::AtQuestion
        | DFOperator::Question
        | DFOperator::QuestionAnd
        | DFOperator::QuestionPipe
        | DFOperator::Colon => None,
    }
}

fn supported_data_types(dt: &DataType) -> bool {
    use DataType::*;

    // For dictionary types, check if the value type is supported.
    if let Dictionary(_, value_type) = dt {
        return supported_data_types(value_type.as_ref());
    }

    let is_supported = dt.is_null()
        || dt.is_numeric()
        || dt.is_binary()
        || dt.is_string()
        || matches!(
            dt,
            Boolean | Date32 | Date64 | Timestamp(_, _) | Time32(_) | Time64(_)
        );

    if !is_supported {
        tracing::debug!("DataFusion data type {dt:?} is not supported");
    }

    is_supported
}

/// Returns the list argument of an `array_length` call if the call is a form we can rewrite to
/// `list_length`: either the single-argument form `array_length(arr)`, or the two-argument form
/// with an explicit first dimension `array_length(arr, 1)`, which is equivalent. Higher
/// dimensions recurse into nested lists and are not supported.
/// Calls with other arities return errors.
fn array_length_input(scalar_fn: &ScalarFunctionExpr) -> DFResult<Option<&Arc<dyn PhysicalExpr>>> {
    match scalar_fn.args() {
        [input] => Ok(Some(input)),
        [input, dimension]
            if dimension
                .downcast_ref::<df_expr::Literal>()
                .is_some_and(|literal| matches!(literal.value(), ScalarValue::Int64(Some(1)))) =>
        {
            Ok(Some(input))
        }
        [_, _] => Ok(None),
        _ => Err(exec_datafusion_err!(
            "array_length requires one or two arguments"
        )),
    }
}
