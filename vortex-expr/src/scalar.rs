use vortex_scalar::Scalar;

use crate::expressions::Expr;
use crate::literal::Literal;

pub struct ScalarDisplayWrapper<'a>(pub &'a Scalar);

impl Literal for Scalar {
    fn lit(&self) -> Expr {
        Expr::Literal(self.clone())
    }
}
