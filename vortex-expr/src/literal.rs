use vortex_scalar::Scalar;

use crate::expressions::Value;

pub trait Literal {
    fn lit(&self) -> Value;
}

#[allow(dead_code)]
pub fn lit<T: Into<Scalar>>(n: T) -> Value {
    n.into().lit()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::expression_fns::field;

    #[test]
    fn test_lit() {
        let scalar: Scalar = 1.into();
        let rhs: Value = lit(scalar);
        let expr = field("id").eq(rhs);
        assert_eq!(format!("{}", expr), "(id = 1i32)");
    }
}
