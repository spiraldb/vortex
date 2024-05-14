use vortex_scalar::Scalar;

use crate::expressions::Expr;

pub trait Literal {
    fn lit(&self) -> Expr;
}

#[allow(dead_code)]
pub fn lit<T: Into<Scalar>>(n: T) -> Expr {
    n.into().lit()
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::expression_fns::field;

    #[test]
    fn test_lit() {
        let scalar: Scalar = 1.into();
        let rhs: Expr = lit(scalar);
        let expr = field("id").eq(rhs);
        assert_eq!(format!("{}", expr), "id = 1i32");
    }
}
