use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use vortex::compute::{and, compare, or, Operator as ArrayOperator};
use vortex::Array;
use vortex_dtype::field::Field;
use vortex_error::VortexResult;

use crate::{unbox_any, Operator, VortexExpr};

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    lhs: Arc<dyn VortexExpr>,
    operator: Operator,
    rhs: Arc<dyn VortexExpr>,
}

impl BinaryExpr {
    pub fn new(lhs: Arc<dyn VortexExpr>, operator: Operator, rhs: Arc<dyn VortexExpr>) -> Self {
        Self { lhs, operator, rhs }
    }

    pub fn lhs(&self) -> &Arc<dyn VortexExpr> {
        &self.lhs
    }

    pub fn rhs(&self) -> &Arc<dyn VortexExpr> {
        &self.rhs
    }

    pub fn op(&self) -> Operator {
        self.operator
    }
}

impl VortexExpr for BinaryExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn evaluate(&self, batch: &Array) -> VortexResult<Array> {
        let lhs = self.lhs.evaluate(batch)?;
        let rhs = self.rhs.evaluate(batch)?;

        match self.operator {
            Operator::Eq => compare(lhs, rhs, ArrayOperator::Eq),
            Operator::NotEq => compare(lhs, rhs, ArrayOperator::NotEq),
            Operator::Lt => compare(lhs, rhs, ArrayOperator::Lt),
            Operator::Lte => compare(lhs, rhs, ArrayOperator::Lte),
            Operator::Gt => compare(lhs, rhs, ArrayOperator::Gt),
            Operator::Gte => compare(lhs, rhs, ArrayOperator::Gte),
            Operator::And => and(lhs, rhs),
            Operator::Or => or(lhs, rhs),
        }
    }

    fn collect_references<'a>(&'a self, references: &mut HashSet<&'a Field>) {
        self.lhs.collect_references(references);
        self.rhs.collect_references(references);
    }
}

impl PartialEq<dyn Any> for BinaryExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        unbox_any(other)
            .downcast_ref::<Self>()
            .map(|x| x.operator == self.operator && x.lhs.eq(&self.lhs) && x.rhs.eq(&self.rhs))
            .unwrap_or(false)
    }
}
