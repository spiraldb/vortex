use arrow_schema::SchemaRef;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion_common::{Result as DFResult, ToDFSchema};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{and, lit, Expr};

/// Convert a set of expressions into a single AND expression.
///
/// # Returns
///
/// If conversion is successful, the result will be a
/// [binary expression node][datafusion_expr::Expr::BinaryExpr] containing the conjunction.
pub(crate) fn make_conjunction(exprs: impl AsRef<[Expr]>) -> DFResult<Expr> {
    Ok(exprs
        .as_ref()
        .iter()
        .fold(lit(true), |conj, elem| and(conj, elem.clone())))
}

/// Simplify an expression using DataFusion's builtin analysis passes.
///
/// This encapsulates common optimizations like constant folding and eliminating redundant
/// expressions, e.g. `value AND true`.
pub(crate) fn simplify_expr(expr: &Expr, schema: SchemaRef) -> DFResult<Expr> {
    let schema = schema.to_dfschema_ref()?;

    let props = ExecutionProps::new();
    let context = SimplifyContext::new(&props).with_schema(schema);
    let simplifier = ExprSimplifier::new(context);

    simplifier.simplify(expr.clone())
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};
    use datafusion_expr::{col, lit};

    use super::*;

    #[test]
    fn test_conjunction_simplify() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("int_col", DataType::Int32, false),
            Field::new("bool_col", DataType::Boolean, false),
        ]));

        let exprs = vec![col("int_col").gt_eq(lit(4)), col("bool_col").is_true()];

        assert_eq!(
            simplify_expr(&make_conjunction(&exprs).unwrap(), schema).unwrap(),
            and(col("int_col").gt_eq(lit(4)), col("bool_col").is_true())
        );
    }
}
