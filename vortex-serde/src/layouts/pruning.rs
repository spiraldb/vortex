use std::collections::hash_map::Entry;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use vortex::stats::Stat;
use vortex_dtype::field::Field;
use vortex_dtype::Nullability;
use vortex_expr::{BinaryExpr, Column, Literal, Operator, VortexExpr};
use vortex_scalar::Scalar;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnStat {
    column: Field,
    stat: Stat,
}

impl ColumnStat {
    pub fn new(column: Field, stat: Stat) -> Self {
        Self { column, stat }
    }

    pub fn column(&self) -> &Field {
        &self.column
    }

    pub fn stat(&self) -> Stat {
        self.stat
    }
}

#[allow(dead_code)]
pub struct PruningPredicate {
    expr: Arc<dyn VortexExpr>,
    stat_references: HashMap<Field, ColumnStat>,
}

impl PruningPredicate {
    pub fn new(original_expr: &Arc<dyn VortexExpr>) -> Self {
        let (expr, stat_references) = convert_to_pruning_expression(original_expr);
        Self {
            expr,
            stat_references,
        }
    }

    pub fn references(&self) -> &HashMap<Field, ColumnStat> {
        &self.stat_references
    }

    pub fn necessary_statistics(&self) -> HashMap<Field, Vec<Stat>> {
        let mut to_fetch: HashMap<Field, Vec<Stat>> = HashMap::new();
        for column_stat in self.stat_references.values() {
            match to_fetch.entry(column_stat.column().clone()) {
                Entry::Occupied(o) => o.into_mut().push(column_stat.stat()),
                Entry::Vacant(v) => {
                    v.insert(vec![column_stat.stat()]);
                }
            }
        }
        to_fetch
    }
}

fn convert_to_pruning_expression(
    expr: &Arc<dyn VortexExpr>,
) -> (Arc<dyn VortexExpr>, HashMap<Field, ColumnStat>) {
    // Anything that can't be translated has to be represented as
    // boolean true expression, i.e. the value might be in that chunk
    let fallback = Arc::new(Literal::new(Scalar::bool(true, Nullability::NonNullable)));
    // TODO(robert): Add support for boolean column expressions,
    //  i.e. if column is of bool dtype it's valid to filter on it directly as a predicate
    if expr.as_any().downcast_ref::<Column>().is_some() {
        return (fallback, HashMap::new());
    }

    if let Some(bexp) = expr.as_any().downcast_ref::<BinaryExpr>() {
        if bexp.op() == Operator::Or || bexp.op() == Operator::And {
            let (rewritten_left, mut refs_lhs) = convert_to_pruning_expression(bexp.lhs());
            let (rewritten_right, refs_rhs) = convert_to_pruning_expression(bexp.rhs());
            refs_lhs.extend(refs_rhs);
            return (
                Arc::new(BinaryExpr::new(rewritten_left, bexp.op(), rewritten_right)),
                refs_lhs,
            );
        }

        if let Some(col) = bexp.lhs().as_any().downcast_ref::<Column>() {
            return PruningPredicateBuilder::try_new(col.field().clone(), bexp.op(), bexp.rhs())
                .and_then(column_comparison_to_stat_expr)
                .unwrap_or_else(|| (fallback, HashMap::new()));
        };

        if let Some(col) = bexp.rhs().as_any().downcast_ref::<Column>() {
            return PruningPredicateBuilder::try_new(
                col.field().clone(),
                bexp.op().swap(),
                bexp.lhs(),
            )
            .and_then(column_comparison_to_stat_expr)
            .unwrap_or_else(|| (fallback, HashMap::new()));
        };
    }

    (fallback, HashMap::new())
}

struct PruningPredicateBuilder<'a> {
    column: Field,
    operator: Operator,
    other_exp: &'a Arc<dyn VortexExpr>,
    references: HashMap<Field, ColumnStat>,
}

impl<'a> PruningPredicateBuilder<'a> {
    pub fn try_new(
        column: Field,
        operator: Operator,
        other_exp: &'a Arc<dyn VortexExpr>,
    ) -> Option<Self> {
        // TODO(robert): Simplify expression to guarantee that each column is not compared to itself
        //  For majority of cases self column references are likely not prunable
        if other_exp.references().contains(&column) {
            return None;
        }

        Some(Self {
            column,
            operator,
            other_exp,
            references: HashMap::new(),
        })
    }

    pub fn operator(&self) -> Operator {
        self.operator
    }

    pub fn column_stat(&mut self, stat: Stat) -> Field {
        let (new_field, stat_ref) = column_stat_reference(&self.column, stat);
        self.references.insert(new_field.clone(), stat_ref);
        new_field
    }

    pub fn rewrite_other_exp(&mut self, stat: Stat) -> Arc<dyn VortexExpr> {
        replace_column_with_stat(self.other_exp, stat, &mut self.references)
            .unwrap_or_else(|| self.other_exp.clone())
    }

    pub fn into_references(self) -> HashMap<Field, ColumnStat> {
        self.references
    }
}

fn column_comparison_to_stat_expr(
    mut builder: PruningPredicateBuilder,
) -> Option<(Arc<dyn VortexExpr>, HashMap<Field, ColumnStat>)> {
    let expr: Option<Arc<dyn VortexExpr>> = match builder.operator() {
        Operator::Eq => {
            let min_col = Arc::new(Column::new(builder.column_stat(Stat::Min)));
            let max_col = Arc::new(Column::new(builder.column_stat(Stat::Max)));
            let replaced_max = builder.rewrite_other_exp(Stat::Max);
            let replaced_min = builder.rewrite_other_exp(Stat::Min);

            Some(Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(min_col, Operator::Lte, replaced_max)),
                Operator::And,
                Arc::new(BinaryExpr::new(replaced_min, Operator::Lte, max_col)),
            )))
        }
        Operator::NotEq => {
            let min_col = Arc::new(Column::new(builder.column_stat(Stat::Min)));
            let max_col = Arc::new(Column::new(builder.column_stat(Stat::Max)));
            let replaced_max = builder.rewrite_other_exp(Stat::Max);
            let replaced_min = builder.rewrite_other_exp(Stat::Min);

            // In case of other_exp is literal both sides of AND will be the same expression
            Some(Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        min_col.clone(),
                        Operator::NotEq,
                        replaced_min.clone(),
                    )),
                    Operator::Or,
                    Arc::new(BinaryExpr::new(
                        replaced_min,
                        Operator::NotEq,
                        max_col.clone(),
                    )),
                )),
                Operator::And,
                Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        min_col,
                        Operator::NotEq,
                        replaced_max.clone(),
                    )),
                    Operator::Or,
                    Arc::new(BinaryExpr::new(replaced_max, Operator::NotEq, max_col)),
                )),
            )))
        }
        op @ Operator::Gt | op @ Operator::Gte => {
            let max_col = Arc::new(Column::new(builder.column_stat(Stat::Max)));
            let replaced_min = builder.rewrite_other_exp(Stat::Min);

            Some(Arc::new(BinaryExpr::new(max_col, op, replaced_min)))
        }
        op @ Operator::Lt | op @ Operator::Lte => {
            let min_col = Arc::new(Column::new(builder.column_stat(Stat::Min)));
            let replaced_max = builder.rewrite_other_exp(Stat::Max);

            Some(Arc::new(BinaryExpr::new(min_col, op, replaced_max)))
        }
        _ => None,
    };
    expr.map(|e| (e, builder.into_references()))
}

fn replace_column_with_stat(
    expr: &Arc<dyn VortexExpr>,
    stat: Stat,
    references: &mut HashMap<Field, ColumnStat>,
) -> Option<Arc<dyn VortexExpr>> {
    if let Some(col) = expr.as_any().downcast_ref::<Column>() {
        let (column, stat_ref) = column_stat_reference(col.field(), stat);
        references.insert(column.clone(), stat_ref);
        return Some(Arc::new(Column::new(column)));
    }

    if let Some(bexp) = expr.as_any().downcast_ref::<BinaryExpr>() {
        let rewritten_lhs = replace_column_with_stat(bexp.lhs(), stat, references);
        let rewritten_rhs = replace_column_with_stat(bexp.rhs(), stat, references);
        if rewritten_lhs.is_none() && rewritten_rhs.is_none() {
            return None;
        }

        let lhs = rewritten_lhs.unwrap_or_else(|| bexp.lhs().clone());
        let rhs = rewritten_rhs.unwrap_or_else(|| bexp.rhs().clone());

        return Some(Arc::new(BinaryExpr::new(lhs, bexp.op(), rhs)));
    }

    None
}

fn column_stat_reference(field: &Field, stat: Stat) -> (Field, ColumnStat) {
    let new_field = stat_column_name(field, stat);
    (new_field, ColumnStat::new(field.clone(), stat))
}

fn stat_column_name(field: &Field, stat: Stat) -> Field {
    match field {
        Field::Name(n) => Field::Name(format!("{n}_{stat}")),
        Field::Index(i) => Field::Name(format!("{i}_{stat}")),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use ahash::HashMap;
    use vortex::stats::Stat;
    use vortex_dtype::field::Field;
    use vortex_expr::{BinaryExpr, Column, Literal, Operator, VortexExpr};

    use crate::layouts::pruning::{convert_to_pruning_expression, stat_column_name, ColumnStat};

    #[test]
    pub fn pruning_equals() {
        let column = Field::from("a");
        let literal_eq = Arc::new(Literal::new(42.into()));
        let eq_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column.clone())),
            Operator::Eq,
            literal_eq.clone(),
        )) as _;
        let (converted, refs) = convert_to_pruning_expression(&eq_expr);
        assert_eq!(
            refs,
            HashMap::from_iter([
                (
                    stat_column_name(&column, Stat::Min),
                    ColumnStat::new(column.clone(), Stat::Min)
                ),
                (
                    stat_column_name(&column, Stat::Max),
                    ColumnStat::new(column.clone(), Stat::Max)
                )
            ])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
                Operator::Lte,
                literal_eq.clone(),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                literal_eq,
                Operator::Lte,
                Arc::new(Column::new(stat_column_name(&column, Stat::Max))),
            )),
        ));
        assert_eq!(*converted, *expected_expr.as_any());
    }

    #[test]
    pub fn pruning_equals_column() {
        let column = Field::from("a");
        let other_col = Field::from("b");
        let eq_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column.clone())),
            Operator::Eq,
            Arc::new(Column::new(other_col.clone())),
        )) as _;

        let (converted, refs) = convert_to_pruning_expression(&eq_expr);
        assert_eq!(
            refs,
            HashMap::from_iter([
                (
                    stat_column_name(&column, Stat::Min),
                    ColumnStat::new(column.clone(), Stat::Min)
                ),
                (
                    stat_column_name(&column, Stat::Max),
                    ColumnStat::new(column.clone(), Stat::Max)
                ),
                (
                    stat_column_name(&other_col, Stat::Min),
                    ColumnStat::new(other_col.clone(), Stat::Min)
                ),
                (
                    stat_column_name(&other_col, Stat::Max),
                    ColumnStat::new(other_col.clone(), Stat::Max)
                )
            ])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
                Operator::Lte,
                Arc::new(Column::new(stat_column_name(&other_col, Stat::Max))),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new(stat_column_name(&other_col, Stat::Min))),
                Operator::Lte,
                Arc::new(Column::new(stat_column_name(&column, Stat::Max))),
            )),
        ));
        assert_eq!(*converted, *expected_expr.as_any());
    }

    #[test]
    pub fn pruning_not_equals_column() {
        let column = Field::from("a");
        let other_col = Field::from("b");
        let not_eq_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column.clone())),
            Operator::NotEq,
            Arc::new(Column::new(other_col.clone())),
        )) as _;

        let (converted, refs) = convert_to_pruning_expression(&not_eq_expr);
        assert_eq!(
            refs,
            HashMap::from_iter([
                (
                    stat_column_name(&column, Stat::Min),
                    ColumnStat::new(column.clone(), Stat::Min)
                ),
                (
                    stat_column_name(&column, Stat::Max),
                    ColumnStat::new(column.clone(), Stat::Max)
                ),
                (
                    stat_column_name(&other_col, Stat::Min),
                    ColumnStat::new(other_col.clone(), Stat::Min)
                ),
                (
                    stat_column_name(&other_col, Stat::Max),
                    ColumnStat::new(other_col.clone(), Stat::Max)
                )
            ])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
                    Operator::NotEq,
                    Arc::new(Column::new(stat_column_name(&other_col, Stat::Min))),
                )),
                Operator::Or,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(stat_column_name(&other_col, Stat::Min))),
                    Operator::NotEq,
                    Arc::new(Column::new(stat_column_name(&column, Stat::Max))),
                )),
            )),
            Operator::And,
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
                    Operator::NotEq,
                    Arc::new(Column::new(stat_column_name(&other_col, Stat::Max))),
                )),
                Operator::Or,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(stat_column_name(&other_col, Stat::Max))),
                    Operator::NotEq,
                    Arc::new(Column::new(stat_column_name(&column, Stat::Max))),
                )),
            )),
        ));
        assert_eq!(*converted, *expected_expr.as_any());
    }

    #[test]
    pub fn pruning_gt_column() {
        let column = Field::from("a");
        let other_col = Field::from("b");
        let other_expr = Arc::new(Column::new(other_col.clone()));
        let not_eq_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column.clone())),
            Operator::Gt,
            other_expr.clone(),
        )) as _;

        let (converted, refs) = convert_to_pruning_expression(&not_eq_expr);
        assert_eq!(
            refs,
            HashMap::from_iter([
                (
                    stat_column_name(&column, Stat::Max),
                    ColumnStat::new(column.clone(), Stat::Max)
                ),
                (
                    stat_column_name(&other_col, Stat::Min),
                    ColumnStat::new(other_col.clone(), Stat::Min)
                )
            ])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(stat_column_name(&column, Stat::Max))),
            Operator::Gt,
            Arc::new(Column::new(stat_column_name(&other_col, Stat::Min))),
        ));
        assert_eq!(*converted, *expected_expr.as_any());
    }

    #[test]
    pub fn pruning_gt_value() {
        let column = Field::from("a");
        let other_col = Arc::new(Literal::new(42.into()));
        let not_eq_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column.clone())),
            Operator::Gt,
            other_col.clone(),
        )) as _;

        let (converted, refs) = convert_to_pruning_expression(&not_eq_expr);
        assert_eq!(
            refs,
            HashMap::from_iter([(
                stat_column_name(&column, Stat::Max),
                ColumnStat::new(column.clone(), Stat::Max)
            )])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(stat_column_name(&column, Stat::Max))),
            Operator::Gt,
            other_col.clone(),
        ));
        assert_eq!(*converted, *expected_expr.as_any());
    }

    #[test]
    pub fn pruning_lt_column() {
        let column = Field::from("a");
        let other_col = Field::from("b");
        let other_expr = Arc::new(Column::new(other_col.clone()));
        let not_eq_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column.clone())),
            Operator::Lt,
            other_expr.clone(),
        )) as _;

        let (converted, refs) = convert_to_pruning_expression(&not_eq_expr);
        assert_eq!(
            refs,
            HashMap::from_iter([
                (
                    stat_column_name(&column, Stat::Min),
                    ColumnStat::new(column.clone(), Stat::Min)
                ),
                (
                    stat_column_name(&other_col, Stat::Max),
                    ColumnStat::new(other_col.clone(), Stat::Max)
                )
            ])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
            Operator::Lt,
            Arc::new(Column::new(stat_column_name(&other_col, Stat::Max))),
        ));
        assert_eq!(*converted, *expected_expr.as_any());
    }

    #[test]
    pub fn pruning_lt_value() {
        let column = Field::from("a");
        let other_col = Arc::new(Literal::new(42.into()));
        let not_eq_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(column.clone())),
            Operator::Lt,
            other_col.clone(),
        )) as _;

        let (converted, refs) = convert_to_pruning_expression(&not_eq_expr);
        assert_eq!(
            refs,
            HashMap::from_iter([(
                stat_column_name(&column, Stat::Min),
                ColumnStat::new(column.clone(), Stat::Min)
            )])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
            Operator::Lt,
            other_col.clone(),
        ));
        assert_eq!(*converted, *expected_expr.as_any());
    }
}
