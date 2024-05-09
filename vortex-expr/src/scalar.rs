use vortex_scalar::Scalar;

use crate::expressions::{Value};
use crate::literal::Literal;

pub struct ScalarDisplayWrapper<'a>(pub &'a Scalar);

impl Literal for Scalar {
    fn lit(&self) -> Value {
        Value::Literal(self.clone())
    }
}
