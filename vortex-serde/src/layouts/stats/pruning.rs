use std::sync::Arc;

use ahash::{HashMap, HashMapExt, HashSet};
use vortex::stats::Stat;
use vortex_dtype::field::Field;
use vortex_dtype::Nullability;
use vortex_expr::{BinaryExpr, Column, Literal, Not, Operator, VortexExpr};
use vortex_scalar::Scalar;

#[derive(Debug, Clone)]
pub struct PruningPredicate {
    expr: Arc<dyn VortexExpr>,
    required_stats: HashMap<Field, HashSet<Stat>>,
}

impl PruningPredicate {
    pub fn try_new(original_expr: &Arc<dyn VortexExpr>) -> Option<Self> {
        let (expr, required_stats) = convert_to_pruning_expression(original_expr);
        // TODO(robert): Could be constant false but right now we don't generate such expressions, need to have way to reduce constant expressions
        if expr.is_constant() {
            None
        } else {
            Some(Self {
                expr,
                required_stats,
            })
        }
    }

    pub fn expr(&self) -> &Arc<dyn VortexExpr> {
        &self.expr
    }

    pub fn required_stats(&self) -> &HashMap<Field, HashSet<Stat>> {
        &self.required_stats
    }
}

// Anything that can't be translated has to be represented as
// boolean true expression, i.e. the value might be in that chunk
fn convert_to_pruning_expression(
    expr: &Arc<dyn VortexExpr>,
) -> (Arc<dyn VortexExpr>, HashMap<Field, HashSet<Stat>>) {
    if let Some(nexp) = expr.as_any().downcast_ref::<Not>() {
        if nexp.child().as_any().downcast_ref::<Column>().is_some() {
            let mut references = HashMap::new();
            let min_expr = replace_column_with_stat(expr, Stat::Min, &mut references);
            let max_expr = replace_column_with_stat(expr, Stat::Max, &mut references);
            return min_expr
                .zip(max_expr)
                .map(|(min_exp, max_exp)| {
                    (
                        Arc::new(BinaryExpr::new(min_exp, Operator::And, max_exp))
                            as Arc<dyn VortexExpr>,
                        references,
                    )
                })
                .unwrap_or_else(|| {
                    (
                        Arc::new(Literal::new(Scalar::bool(false, Nullability::NonNullable))),
                        HashMap::new(),
                    )
                });
        }
    }

    if expr.as_any().downcast_ref::<Column>().is_some() {
        let mut references = HashMap::new();
        let min_expr = replace_column_with_stat(expr, Stat::Min, &mut references);
        let max_expr = replace_column_with_stat(expr, Stat::Max, &mut references);
        return min_expr
            .zip(max_expr)
            .map(|(min_exp, max_exp)| {
                (
                    Arc::new(Not::new(Arc::new(BinaryExpr::new(
                        min_exp,
                        Operator::Or,
                        max_exp,
                    )))) as Arc<dyn VortexExpr>,
                    references,
                )
            })
            .unwrap_or_else(|| {
                (
                    Arc::new(Literal::new(Scalar::bool(false, Nullability::NonNullable))),
                    HashMap::new(),
                )
            });
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
            return PruningPredicateRewriter::try_new(col.field().clone(), bexp.op(), bexp.rhs())
                .and_then(PruningPredicateRewriter::rewrite)
                .unwrap_or_else(|| {
                    (
                        Arc::new(Literal::new(Scalar::bool(false, Nullability::NonNullable))),
                        HashMap::new(),
                    )
                });
        };

        if let Some(col) = bexp.rhs().as_any().downcast_ref::<Column>() {
            return PruningPredicateRewriter::try_new(
                col.field().clone(),
                bexp.op().swap(),
                bexp.lhs(),
            )
            .and_then(PruningPredicateRewriter::rewrite)
            .unwrap_or_else(|| {
                (
                    Arc::new(Literal::new(Scalar::bool(false, Nullability::NonNullable))),
                    HashMap::new(),
                )
            });
        };
    }

    (
        Arc::new(Literal::new(Scalar::bool(false, Nullability::NonNullable))),
        HashMap::new(),
    )
}

struct PruningPredicateRewriter<'a> {
    column: Field,
    operator: Operator,
    other_exp: &'a Arc<dyn VortexExpr>,
    stats_to_fetch: HashMap<Field, HashSet<Stat>>,
}

type PruningPredicateStats = (Arc<dyn VortexExpr>, HashMap<Field, HashSet<Stat>>);

impl<'a> PruningPredicateRewriter<'a> {
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
            stats_to_fetch: HashMap::new(),
        })
    }

    fn add_stat_reference(&mut self, stat: Stat) -> Field {
        let new_field = stat_column_name(&self.column, stat);
        self.stats_to_fetch
            .entry(self.column.clone())
            .or_default()
            .insert(stat);
        new_field
    }

    fn rewrite_other_exp(&mut self, stat: Stat) -> Arc<dyn VortexExpr> {
        replace_column_with_stat(self.other_exp, stat, &mut self.stats_to_fetch)
            .unwrap_or_else(|| self.other_exp.clone())
    }

    fn rewrite(mut self) -> Option<PruningPredicateStats> {
        let expr: Option<Arc<dyn VortexExpr>> = match self.operator {
            Operator::Eq => {
                let min_col = Arc::new(Column::new(self.add_stat_reference(Stat::Min)));
                let max_col = Arc::new(Column::new(self.add_stat_reference(Stat::Max)));
                let replaced_max = self.rewrite_other_exp(Stat::Max);
                let replaced_min = self.rewrite_other_exp(Stat::Min);

                Some(Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(min_col, Operator::Gt, replaced_max)),
                    Operator::Or,
                    Arc::new(BinaryExpr::new(replaced_min, Operator::Gt, max_col)),
                )))
            }
            Operator::NotEq => {
                let min_col = Arc::new(Column::new(self.add_stat_reference(Stat::Min)));
                let max_col = Arc::new(Column::new(self.add_stat_reference(Stat::Max)));
                let replaced_max = self.rewrite_other_exp(Stat::Max);
                let replaced_min = self.rewrite_other_exp(Stat::Min);

                // In case of other_exp is literal both sides of AND will be the same expression
                Some(Arc::new(BinaryExpr::new(
                    Arc::new(BinaryExpr::new(
                        Arc::new(BinaryExpr::new(
                            min_col.clone(),
                            Operator::Eq,
                            replaced_min.clone(),
                        )),
                        Operator::And,
                        Arc::new(BinaryExpr::new(replaced_min, Operator::Eq, max_col.clone())),
                    )),
                    Operator::Or,
                    Arc::new(BinaryExpr::new(
                        Arc::new(BinaryExpr::new(min_col, Operator::Eq, replaced_max.clone())),
                        Operator::And,
                        Arc::new(BinaryExpr::new(replaced_max, Operator::Eq, max_col)),
                    )),
                )))
            }
            Operator::Gt | Operator::Gte => {
                let max_col = Arc::new(Column::new(self.add_stat_reference(Stat::Max)));
                let replaced_min = self.rewrite_other_exp(Stat::Min);

                Some(Arc::new(BinaryExpr::new(
                    max_col,
                    Operator::Lte,
                    replaced_min,
                )))
            }
            Operator::Lt | Operator::Lte => {
                let min_col = Arc::new(Column::new(self.add_stat_reference(Stat::Min)));
                let replaced_max = self.rewrite_other_exp(Stat::Max);

                Some(Arc::new(BinaryExpr::new(
                    min_col,
                    Operator::Gte,
                    replaced_max,
                )))
            }
            _ => None,
        };
        expr.map(|e| (e, self.stats_to_fetch))
    }
}

fn replace_column_with_stat(
    expr: &Arc<dyn VortexExpr>,
    stat: Stat,
    stats_to_fetch: &mut HashMap<Field, HashSet<Stat>>,
) -> Option<Arc<dyn VortexExpr>> {
    if let Some(col) = expr.as_any().downcast_ref::<Column>() {
        let new_field = stat_column_name(col.field(), stat);
        stats_to_fetch
            .entry(col.field().clone())
            .or_default()
            .insert(stat);
        return Some(Arc::new(Column::new(new_field)));
    }

    if let Some(not) = expr.as_any().downcast_ref::<Not>() {
        let rewritten = replace_column_with_stat(not.child(), stat, stats_to_fetch)?;
        return Some(Arc::new(Not::new(rewritten)));
    }

    if let Some(bexp) = expr.as_any().downcast_ref::<BinaryExpr>() {
        let rewritten_lhs = replace_column_with_stat(bexp.lhs(), stat, stats_to_fetch);
        let rewritten_rhs = replace_column_with_stat(bexp.rhs(), stat, stats_to_fetch);
        if rewritten_lhs.is_none() && rewritten_rhs.is_none() {
            return None;
        }

        let lhs = rewritten_lhs.unwrap_or_else(|| bexp.lhs().clone());
        let rhs = rewritten_rhs.unwrap_or_else(|| bexp.rhs().clone());

        return Some(Arc::new(BinaryExpr::new(lhs, bexp.op(), rhs)));
    }

    None
}

pub(crate) fn stat_column_name(field: &Field, stat: Stat) -> Field {
    match field {
        Field::Name(n) => Field::Name(format!("{n}_{stat}")),
        Field::Index(i) => Field::Name(format!("{i}_{stat}")),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;

    use ahash::HashMap;
    use vortex::stats::Stat;
    use vortex_dtype::field::Field;
    use vortex_expr::{BinaryExpr, Column, Literal, Operator, VortexExpr};

    use crate::layouts::stats::pruning::{convert_to_pruning_expression, stat_column_name};

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
            HashMap::from_iter([(column.clone(), HashSet::from_iter([Stat::Min, Stat::Max]))])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
                Operator::Gt,
                literal_eq.clone(),
            )),
            Operator::Or,
            Arc::new(BinaryExpr::new(
                literal_eq,
                Operator::Gt,
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
                (column.clone(), HashSet::from_iter([Stat::Min, Stat::Max])),
                (
                    other_col.clone(),
                    HashSet::from_iter([Stat::Max, Stat::Min])
                )
            ])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
                Operator::Gt,
                Arc::new(Column::new(stat_column_name(&other_col, Stat::Max))),
            )),
            Operator::Or,
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new(stat_column_name(&other_col, Stat::Min))),
                Operator::Gt,
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
                (column.clone(), HashSet::from_iter([Stat::Min, Stat::Max])),
                (
                    other_col.clone(),
                    HashSet::from_iter([Stat::Max, Stat::Min])
                )
            ])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
                    Operator::Eq,
                    Arc::new(Column::new(stat_column_name(&other_col, Stat::Min))),
                )),
                Operator::And,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(stat_column_name(&other_col, Stat::Min))),
                    Operator::Eq,
                    Arc::new(Column::new(stat_column_name(&column, Stat::Max))),
                )),
            )),
            Operator::Or,
            Arc::new(BinaryExpr::new(
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
                    Operator::Eq,
                    Arc::new(Column::new(stat_column_name(&other_col, Stat::Max))),
                )),
                Operator::And,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new(stat_column_name(&other_col, Stat::Max))),
                    Operator::Eq,
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
                (column.clone(), HashSet::from_iter([Stat::Max])),
                (other_col.clone(), HashSet::from_iter([Stat::Min]))
            ])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(stat_column_name(&column, Stat::Max))),
            Operator::Lte,
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
            HashMap::from_iter([(column.clone(), HashSet::from_iter([Stat::Max])),])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(stat_column_name(&column, Stat::Max))),
            Operator::Lte,
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
                (column.clone(), HashSet::from_iter([Stat::Min])),
                (other_col.clone(), HashSet::from_iter([Stat::Max]))
            ])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
            Operator::Gte,
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
            HashMap::from_iter([(column.clone(), HashSet::from_iter([Stat::Min]))])
        );
        let expected_expr: Arc<dyn VortexExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new(stat_column_name(&column, Stat::Min))),
            Operator::Gte,
            other_col.clone(),
        ));
        assert_eq!(*converted, *expected_expr.as_any());
    }
}
