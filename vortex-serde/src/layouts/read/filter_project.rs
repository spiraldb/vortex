use std::sync::Arc;

use vortex_dtype::field::Field;
use vortex_expr::{BinaryExpr, Column, Identity, Literal, Operator, VortexExpr};

pub fn filter_project(
    filter: &Arc<dyn VortexExpr>,
    projection: &[Field],
) -> Option<Arc<dyn VortexExpr>> {
    if filter.as_any().downcast_ref::<Literal>().is_some() {
        Some(filter.clone())
    } else if let Some(c) = filter.as_any().downcast_ref::<Column>() {
        projection.contains(c.field()).then(|| {
            if projection.len() == 1 {
                Arc::new(Identity)
            } else {
                Arc::new(Column::new(c.field().clone())) as Arc<dyn VortexExpr>
            }
        })
    } else if let Some(bexp) = filter.as_any().downcast_ref::<BinaryExpr>() {
        let lhs_proj = filter_project(bexp.lhs(), projection);
        let rhs_proj = filter_project(bexp.rhs(), projection);
        if bexp.op() == Operator::And {
            if let Some(lhsp) = lhs_proj {
                if let Some(rhsp) = rhs_proj {
                    Some(Arc::new(BinaryExpr::new(lhsp, bexp.op(), rhsp)))
                } else {
                    (!bexp
                        .rhs()
                        .references()
                        .intersection(&lhsp.references())
                        .any(|f| projection.contains(f)))
                    .then_some(lhsp)
                }
            } else if bexp
                .lhs()
                .references()
                .intersection(&bexp.rhs().references())
                .next()
                .is_none()
            {
                rhs_proj
            } else {
                None
            }
        } else {
            Some(Arc::new(BinaryExpr::new(lhs_proj?, bexp.op(), rhs_proj?)))
        }
    } else {
        None
    }
}
