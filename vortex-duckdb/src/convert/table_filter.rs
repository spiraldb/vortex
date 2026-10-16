// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;

use itertools::Itertools;
use vortex::buffer::Buffer;
use vortex::dtype::DType;
use vortex::dtype::Nullability;
use vortex::dtype::PType;
use vortex::error::VortexExpect;
use vortex::error::VortexResult;
use vortex::error::vortex_bail;
use vortex::error::vortex_err;
use vortex::expr::Expression;
use vortex::expr::and_collect;
use vortex::expr::cast;
use vortex::expr::get_item;
use vortex::expr::in_list;
use vortex::expr::is_not_null;
use vortex::expr::is_null;
use vortex::expr::lit;
use vortex::expr::or_collect;
use vortex::scalar::Scalar;
use vortex::scalar_fn::ScalarFnVTableExt;
use vortex::scalar_fn::fns::binary::Binary;
use vortex::scalar_fn::fns::operators::CompareOperator;
use vortex::scan::selection::Selection;
use vortex::scan::strict_sorted_buffer::StrictSortedBuffer;

use super::dtype::FromLogicalType;
use super::expr::try_from_bound_expression_with_col_sub;
use super::join_filter::adaptive_filter;
use super::join_filter::bloom_filter;
use crate::cpp::DUCKDB_VX_EXPR_TYPE;
use crate::duckdb::ExtractedValue;
use crate::duckdb::TableFilterClass;
use crate::duckdb::TableFilterRef;
use crate::duckdb::ValueRef;

pub fn try_from_table_filter(
    value: &TableFilterRef,
    col: &Expression,
    scope_dtype: &DType,
) -> VortexResult<Option<Expression>> {
    Ok(Some(match value.as_class() {
        TableFilterClass::ConstantComparison(const_) => {
            let scalar: Scalar = const_.value.try_into()?;

            let source = col.return_dtype(scope_dtype)?;
            let scalar = if exact_double_source(&source, scalar.dtype()) {
                normalize_double_zero(scalar)
            } else {
                scalar
            };
            let input = exact_double_filter_input(col, scalar.dtype(), scope_dtype)?;
            Binary.new_expr(const_.operator.try_into()?, [input, lit(scalar)])
        }
        TableFilterClass::ConjunctionAnd(conj_and) => {
            let Some(children) = conj_and
                .children()
                .map(|child| try_from_table_filter(child, col, scope_dtype))
                .try_collect::<_, Option<Vec<_>>, _>()?
            else {
                return Ok(None);
            };

            and_collect(children).unwrap_or_else(|| lit(true))
        }
        // This is a disjunction.
        TableFilterClass::ConjunctionOr(disjuction_or) => {
            let Some(children) = disjuction_or
                .children()
                .map(|child| try_from_table_filter(child, col, scope_dtype))
                .try_collect::<_, Option<Vec<_>>, _>()?
            else {
                return Ok(None);
            };

            or_collect(children).unwrap_or_else(|| lit(false))
        }
        TableFilterClass::IsNull => is_null(col.clone()),
        TableFilterClass::IsNotNull => is_not_null(col.clone()),
        TableFilterClass::StructExtract(name, child_filter) => {
            return try_from_table_filter(child_filter, &get_item(name, col.clone()), scope_dtype);
        }
        TableFilterClass::Optional(optional) => {
            // An unsupported optional filter passes every row. Returning None here would
            // discard supported siblings in a conjunction, including join min/max bounds.
            let expression = try_from_table_filter(optional.child, col, scope_dtype)
                .ok()
                .flatten()
                .unwrap_or_else(|| lit(true));
            match optional.policy {
                Some(policy) => adaptive_filter(expression, policy),
                None => expression,
            }
        }
        TableFilterClass::InFilter(values) => {
            let mut scalars: Vec<_> = values.iter().map(Scalar::try_from).try_collect()?;
            assert!(
                !scalars.is_empty(),
                "IN filter must have at least one value"
            );
            let dtype = scalars[0].dtype().clone();
            if exact_double_source(&col.return_dtype(scope_dtype)?, &dtype) {
                scalars = scalars.into_iter().map(normalize_double_zero).collect();
            }
            let input = exact_double_filter_input(col, &dtype, scope_dtype)?;
            // The IN list itself is never null. Null values are elements, and in_list handles
            // them.
            let list_scalar = Scalar::list(Arc::new(dtype), scalars, Nullability::NonNullable);
            in_list(input, lit(list_scalar))
        }
        TableFilterClass::Dynamic(dynamic) => {
            let op = match dynamic.operator {
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_EQUAL => CompareOperator::Eq,
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_NOTEQUAL => CompareOperator::NotEq,
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_LESSTHAN => CompareOperator::Lt,
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_GREATERTHAN => CompareOperator::Gt,
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_LESSTHANOREQUALTO => {
                    CompareOperator::Lte
                }
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_GREATERTHANOREQUALTO => {
                    CompareOperator::Gte
                }
                _ => vortex_bail!(
                    "unsupported dynamic filter operator: {:?}",
                    dynamic.operator
                ),
            };
            let data = dynamic.data;

            vortex::expr::dynamic(
                op,
                move || {
                    let value = data.latest()?;
                    let scalar = Scalar::try_from(&*value)
                        .vortex_expect("failed to convert dynamic filter value to scalar");
                    scalar.into_value()
                },
                col.return_dtype(scope_dtype)?,
                true, // If there is no value, we say that all rows pass the dynamic filter.
                col.clone(),
            )
        }
        TableFilterClass::ExpressionRef(expr) => {
            match try_from_bound_expression_with_col_sub(expr, col)? {
                Some(expression) => expression,
                None => return Ok(None),
            }
        }
        TableFilterClass::Bloom(Some(snapshot)) => {
            let dtype = DType::from_logical_type(&snapshot.key_type(), Nullability::Nullable)?;
            bloom_filter(
                snapshot,
                exact_double_filter_input(col, &dtype, scope_dtype)?,
            )?
        }
        TableFilterClass::Bloom(None) => lit(true),
    }))
}

fn exact_double_source(source: &DType, target: &DType) -> bool {
    matches!(
        source,
        DType::Primitive(
            PType::I8 | PType::I16 | PType::I32 | PType::U8 | PType::U16 | PType::U32,
            _
        )
    ) && matches!(target, DType::Primitive(PType::F64, _))
}

fn normalize_double_zero(value: Scalar) -> Scalar {
    // Casting integer zero produces +0.0, which must also match DuckDB's -0.0 keys.
    if value.as_primitive().typed_value::<f64>() == Some(0.0) {
        Scalar::primitive(0f64, value.dtype().nullability())
    } else {
        value
    }
}

fn exact_double_filter_input(
    col: &Expression,
    target: &DType,
    scope: &DType,
) -> VortexResult<Expression> {
    let source = col.return_dtype(scope)?;
    if exact_double_source(&source, target) {
        return Ok(cast(
            col.clone(),
            target.with_nullability(source.nullability()),
        ));
    }
    Ok(col.clone())
}

fn nonnegative_number_from_value(value: &ValueRef) -> VortexResult<u64> {
    match value.extract() {
        ExtractedValue::BigInt(i) => {
            u64::try_from(i).map_err(|_| vortex_err!("negative value: {i}"))
        }
        ExtractedValue::Integer(i) => {
            u64::try_from(i).map_err(|_| vortex_err!("negative value: {i}"))
        }
        ExtractedValue::UBigInt(u) => Ok(u),
        ExtractedValue::UInteger(u) => Ok(u64::from(u)),
        _ => vortex_bail!("unexpected value type"),
    }
}

fn intersect_sorted(left: &[u64], right: &[u64]) -> Vec<u64> {
    let mut result = Vec::new();
    let (mut i, mut j) = (0, 0);
    while i < left.len() && j < right.len() {
        match left[i].cmp(&right[j]) {
            std::cmp::Ordering::Equal => {
                result.push(left[i]);
                i += 1;
                j += 1;
            }
            std::cmp::Ordering::Less => i += 1,
            std::cmp::Ordering::Greater => j += 1,
        }
    }
    result
}

/// For constant comparison on IN filters over file_index or file_row_number
/// virtual column, create a selection and a range covering the same range as
/// expressions do.
pub fn try_from_virtual_column_filter(
    filter: &TableFilterRef,
) -> VortexResult<(Selection, Option<Range<u64>>)> {
    match filter.as_class() {
        TableFilterClass::InFilter(values) => {
            let indices = values
                .iter()
                .map(nonnegative_number_from_value)
                .collect::<VortexResult<Vec<u64>>>()?;
            Ok((
                Selection::IncludeByIndex(StrictSortedBuffer::try_new(Buffer::from_iter(indices))?),
                None,
            ))
        }
        TableFilterClass::ConstantComparison(const_) => {
            let n = nonnegative_number_from_value(const_.value)?;
            let range = match const_.operator {
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_EQUAL => Some(n..n + 1),
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_GREATERTHANOREQUALTO => {
                    Some(n..u64::MAX)
                }
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_GREATERTHAN => {
                    Some(n.saturating_add(1)..u64::MAX)
                }
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_LESSTHANOREQUALTO => {
                    Some(0..n.saturating_add(1))
                }
                DUCKDB_VX_EXPR_TYPE::DUCKDB_VX_EXPR_TYPE_COMPARE_LESSTHAN => Some(0..n),
                _ => None,
            };
            Ok((Selection::All, range))
        }
        TableFilterClass::ConjunctionAnd(conj) => {
            let mut start = 0u64;
            let mut end = u64::MAX;
            let mut indices: Option<Vec<u64>> = None;
            for child in conj.children() {
                let (sel, range) = try_from_virtual_column_filter(child)?;
                if let Selection::IncludeByIndex(buf) = sel {
                    indices = Some(match indices {
                        None => buf.as_slice().to_vec(),
                        Some(existing) => intersect_sorted(&existing, buf.as_slice()),
                    });
                }
                if let Some(r) = range {
                    start = start.max(r.start);
                    end = end.min(r.end);
                }
            }
            let range = (start < end).then_some(start..end);
            let sel = indices
                .map(|v| {
                    StrictSortedBuffer::try_new(Buffer::from_iter(v)).map(Selection::IncludeByIndex)
                })
                .transpose()?
                .unwrap_or(Selection::All);
            Ok((sel, range))
        }
        TableFilterClass::Optional(optional) => {
            try_from_virtual_column_filter(optional.child).or_else(|_| Ok((Selection::All, None)))
        }
        _ => Ok((Selection::All, None)),
    }
}
