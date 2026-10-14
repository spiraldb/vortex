// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Expression constructors for this crate's scalar functions.
//!
//! These complement the constructors in `vortex_array::expr`.

use vortex_array::aggregate_fn::NumericalAggregateOpts;
use vortex_array::dtype::FieldNames;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::Expression;
use vortex_array::scalar_fn::EmptyOptions;
use vortex_array::scalar_fn::ScalarFnVTableExt;
use vortex_error::VortexExpect;
use vortex_error::vortex_panic;

use crate::byte_length::ByteLength;
use crate::case_when::CaseWhen;
use crate::case_when::CaseWhenOptions;
use crate::ext_storage::ExtStorage;
use crate::list_length::ListLength;
use crate::list_sum::ListSum;
use crate::merge::DuplicateHandling;
use crate::merge::Merge;
use crate::replace_time_zone::ReplaceTimeZone;
use crate::replace_time_zone::ReplaceTimeZoneOptions;
use crate::select::FieldSelection;
use crate::select::Select;

/// Creates a CASE WHEN expression with one WHEN/THEN pair and an ELSE value.
pub fn case_when(
    condition: Expression,
    then_value: Expression,
    else_value: Expression,
) -> Expression {
    let options = CaseWhenOptions {
        num_when_then_pairs: 1,
        has_else: true,
    };
    CaseWhen.new_expr(options, [condition, then_value, else_value])
}

/// Creates a bound CASE WHEN expression with one WHEN/THEN pair and an ELSE value.
pub fn bound_case_when(
    condition: BoundExpression,
    then_value: BoundExpression,
    else_value: BoundExpression,
) -> BoundExpression {
    let options = CaseWhenOptions {
        num_when_then_pairs: 1,
        has_else: true,
    };
    CaseWhen
        .try_new_bound_expr(options, [condition, then_value, else_value])
        .vortex_expect("case expressions must have boolean conditions and matching branch dtypes")
}

/// Creates a CASE WHEN expression with one WHEN/THEN pair and no ELSE value.
pub fn case_when_no_else(condition: Expression, then_value: Expression) -> Expression {
    let options = CaseWhenOptions {
        num_when_then_pairs: 1,
        has_else: false,
    };
    CaseWhen.new_expr(options, [condition, then_value])
}

/// Creates a bound CASE WHEN expression with one WHEN/THEN pair and no ELSE value.
pub fn bound_case_when_no_else(
    condition: BoundExpression,
    then_value: BoundExpression,
) -> BoundExpression {
    let options = CaseWhenOptions {
        num_when_then_pairs: 1,
        has_else: false,
    };
    CaseWhen
        .try_new_bound_expr(options, [condition, then_value])
        .vortex_expect("case expressions must have boolean conditions")
}

/// Creates an n-ary CASE WHEN expression from WHEN/THEN pairs and an optional ELSE value.
pub fn nested_case_when(
    when_then_pairs: Vec<(Expression, Expression)>,
    else_value: Option<Expression>,
) -> Expression {
    assert!(
        !when_then_pairs.is_empty(),
        "nested_case_when requires at least one when/then pair"
    );

    let has_else = else_value.is_some();
    let mut children = Vec::with_capacity(when_then_pairs.len() * 2 + usize::from(has_else));
    for (condition, then_value) in &when_then_pairs {
        children.push(condition.clone());
        children.push(then_value.clone());
    }
    if let Some(else_expr) = else_value {
        children.push(else_expr);
    }

    let Ok(num_when_then_pairs) = u32::try_from(when_then_pairs.len()) else {
        vortex_panic!("nested_case_when has too many when/then pairs");
    };
    let options = CaseWhenOptions {
        num_when_then_pairs,
        has_else,
    };
    CaseWhen.new_expr(options, children)
}

/// Creates a bound n-ary CASE WHEN expression from WHEN/THEN pairs and an optional ELSE value.
pub fn bound_nested_case_when(
    when_then_pairs: Vec<(BoundExpression, BoundExpression)>,
    else_value: Option<BoundExpression>,
) -> BoundExpression {
    assert!(
        !when_then_pairs.is_empty(),
        "nested_case_when requires at least one when/then pair"
    );

    let Ok(num_when_then_pairs) = u32::try_from(when_then_pairs.len()) else {
        vortex_panic!("nested_case_when has too many when/then pairs");
    };
    let has_else = else_value.is_some();
    let mut children = Vec::with_capacity(when_then_pairs.len() * 2 + usize::from(has_else));
    for (condition, then_value) in when_then_pairs {
        children.push(condition);
        children.push(then_value);
    }
    if let Some(else_expr) = else_value {
        children.push(else_expr);
    }

    let options = CaseWhenOptions {
        num_when_then_pairs,
        has_else,
    };
    CaseWhen
        .try_new_bound_expr(options, children)
        .vortex_expect("case expressions must have boolean conditions and matching branch dtypes")
}

/// Creates an expression that selects (includes) specific fields from an array.
///
/// Projects only the specified fields from the child expression, which must be of DType struct.
/// ```rust
/// # use vortex_scalar_fns::expr::select;
/// # use vortex_array::expr::root;
/// let expr = select(["name", "age"], root());
/// ```
pub fn select(field_names: impl Into<FieldNames>, child: Expression) -> Expression {
    Select
        .try_new_expr(FieldSelection::Include(field_names.into()), [child])
        .vortex_expect("Failed to create Select expression")
}

/// Creates a bound expression that selects specific fields from a struct expression.
pub fn bound_select(field_names: impl Into<FieldNames>, child: BoundExpression) -> BoundExpression {
    Select
        .try_new_bound_expr(FieldSelection::Include(field_names.into()), [child])
        .vortex_expect("select expressions require fields from a struct child")
}

/// Creates an expression that excludes specific fields from an array.
///
/// Projects all fields except the specified ones from the input struct expression.
///
/// ```rust
/// # use vortex_scalar_fns::expr::select_exclude;
/// # use vortex_array::expr::root;
/// let expr = select_exclude(["internal_id", "metadata"], root());
/// ```
pub fn select_exclude(fields: impl Into<FieldNames>, child: Expression) -> Expression {
    Select
        .try_new_expr(FieldSelection::Exclude(fields.into()), [child])
        .vortex_expect("Failed to create Select expression")
}

/// Creates a bound expression that excludes specific fields from a struct expression.
pub fn bound_select_exclude(
    fields: impl Into<FieldNames>,
    child: BoundExpression,
) -> BoundExpression {
    Select
        .try_new_bound_expr(FieldSelection::Exclude(fields.into()), [child])
        .vortex_expect("select expressions require fields from a struct child")
}

/// Creates an expression that merges struct expressions into a single struct.
///
/// Combines fields from all input expressions. If field names are duplicated,
/// later expressions win. Fields are not recursively merged.
///
/// ```rust
/// # use vortex_array::dtype::Nullability;
/// # use vortex_scalar_fns::expr::merge;
/// # use vortex_array::expr::{get_item, root};
/// let expr = merge([get_item("a", root()), get_item("b", root())]);
/// ```
pub fn merge(elements: impl IntoIterator<Item = impl Into<Expression>>) -> Expression {
    use itertools::Itertools as _;
    let values = elements.into_iter().map(|value| value.into()).collect_vec();
    Merge.new_expr(DuplicateHandling::default(), values)
}

/// Creates a bound expression that merges struct expressions.
pub fn bound_merge(elements: impl IntoIterator<Item = BoundExpression>) -> BoundExpression {
    bound_merge_opts(elements, DuplicateHandling::default())
}

/// Creates a merge expression with explicit duplicate handling.
pub fn merge_opts(
    elements: impl IntoIterator<Item = impl Into<Expression>>,
    duplicate_handling: DuplicateHandling,
) -> Expression {
    use itertools::Itertools as _;
    let values = elements.into_iter().map(|value| value.into()).collect_vec();
    Merge.new_expr(duplicate_handling, values)
}

/// Creates a bound merge expression with explicit duplicate handling.
pub fn bound_merge_opts(
    elements: impl IntoIterator<Item = BoundExpression>,
    duplicate_handling: DuplicateHandling,
) -> BoundExpression {
    Merge
        .try_new_bound_expr(duplicate_handling, elements)
        .vortex_expect("merge expressions require non-nullable struct children")
}

/// Creates an expression that computes the byte length of each element.
/// This is akin to ANSI SQL OCTET_LENGTH(), or DuckDB's strlen().
///
/// ```rust
/// # use vortex_scalar_fns::expr::byte_length;
/// # use vortex_array::expr::root;
/// let expr = byte_length(root());
/// ```
pub fn byte_length(input: Expression) -> Expression {
    ByteLength.new_expr(EmptyOptions, [input])
}

/// Creates a bound expression that computes each element's byte length.
pub fn bound_byte_length(input: BoundExpression) -> BoundExpression {
    ByteLength
        .try_new_bound_expr(EmptyOptions, [input])
        .vortex_expect("byte-length expressions require a variable-length binary child")
}

/// Creates an expression that extracts the storage values from an extension array.
///
/// ```rust
/// # use vortex_scalar_fns::expr::ext_storage;
/// # use vortex_array::expr::root;
/// let expr = ext_storage(root());
/// ```
pub fn ext_storage(input: Expression) -> Expression {
    ExtStorage.new_expr(EmptyOptions, [input])
}

/// Creates a bound expression that extracts an extension array's storage values.
pub fn bound_ext_storage(input: BoundExpression) -> BoundExpression {
    ExtStorage
        .try_new_bound_expr(EmptyOptions, [input])
        .vortex_expect("extension-storage expressions require an extension child")
}

/// Creates an expression that computes the number of elements in each list
/// for `List` and `FixedSizeList` inputs. This is akin to ANSI SQL `CARDINALITY()`,
/// or DuckDB's `len()`/`array_length()`.
///
/// ```rust
/// # use vortex_scalar_fns::expr::list_length;
/// # use vortex_array::expr::root;
/// let expr = list_length(root());
/// ```
pub fn list_length(input: Expression) -> Expression {
    ListLength.new_expr(EmptyOptions, [input])
}

/// Creates a bound expression that computes the number of elements in each list.
pub fn bound_list_length(input: BoundExpression) -> BoundExpression {
    ListLength
        .try_new_bound_expr(EmptyOptions, [input])
        .vortex_expect("list-length expressions require a list child")
}

/// Creates an expression that sums the elements of each list for `List` and
/// `FixedSizeList` inputs, akin to DuckDB's `list_sum()`.
///
/// Follows SQL `SUM` semantics per list: null lists, empty lists, and lists whose elements are
/// all null yield null; null elements are skipped; integer and decimal overflow yields a null
/// value. The result dtype follows `sum`'s widening rules and is always nullable. NaN float
/// elements are skipped by default; see [`list_sum_opts`] for the NaN-including variant.
///
/// ```rust
/// # use vortex_scalar_fns::expr::list_sum;
/// # use vortex_array::expr::root;
/// let expr = list_sum(root());
/// ```
pub fn list_sum(input: Expression) -> Expression {
    ListSum.new_expr(NumericalAggregateOpts::default(), [input])
}

/// Creates a bound expression that sums the elements of each list.
pub fn bound_list_sum(input: BoundExpression) -> BoundExpression {
    ListSum
        .try_new_bound_expr(NumericalAggregateOpts::default(), [input])
        .vortex_expect("list-sum expressions require a numeric list child")
}

/// Creates a [`list_sum`] expression with explicit [`NumericalAggregateOpts`], controlling
/// whether NaN float elements are skipped (the default) or poison the list's sum to NaN.
pub fn list_sum_opts(input: Expression, options: NumericalAggregateOpts) -> Expression {
    ListSum.new_expr(options, [input])
}

/// Creates a bound list-sum expression with explicit aggregate options.
pub fn bound_list_sum_opts(
    input: BoundExpression,
    options: NumericalAggregateOpts,
) -> BoundExpression {
    ListSum
        .try_new_bound_expr(options, [input])
        .vortex_expect("list-sum expressions require a numeric list child")
}

/// Reinterpret timestamp wall times in `options.time_zone`, preserving the time unit.
///
/// `ambiguous` is a UTF-8 expression containing `raise`, `earliest`, `latest`, or `null`.
/// Removing the timezone preserves wall time in a timezone-naive timestamp.
pub fn replace_time_zone(
    input: Expression,
    ambiguous: Expression,
    options: ReplaceTimeZoneOptions,
) -> Expression {
    ReplaceTimeZone.new_expr(options, [input, ambiguous])
}

/// Create a bound timezone replacement expression.
pub fn bound_replace_time_zone(
    input: BoundExpression,
    ambiguous: BoundExpression,
    options: ReplaceTimeZoneOptions,
) -> BoundExpression {
    ReplaceTimeZone
        .try_new_bound_expr(options, [input, ambiguous])
        .vortex_expect("timezone replacement requires a timestamp and a UTF-8 ambiguity policy")
}

/// Constructors for expressions whose children have already been bound and type-checked.
///
/// These mirror the constructors in [`crate::expr`] and panic when the supplied children do not
/// form a well-typed expression.
pub mod bound {
    pub use super::bound_byte_length as byte_length;
    pub use super::bound_case_when as case_when;
    pub use super::bound_case_when_no_else as case_when_no_else;
    pub use super::bound_ext_storage as ext_storage;
    pub use super::bound_list_length as list_length;
    pub use super::bound_list_sum as list_sum;
    pub use super::bound_list_sum_opts as list_sum_opts;
    pub use super::bound_merge as merge;
    pub use super::bound_merge_opts as merge_opts;
    pub use super::bound_nested_case_when as nested_case_when;
    pub use super::bound_replace_time_zone as replace_time_zone;
    pub use super::bound_select as select;
    pub use super::bound_select_exclude as select_exclude;
}
