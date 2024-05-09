use core::fmt;
use std::fmt::{Display, Formatter};

use vortex_dtype::{match_each_native_ptype, DType};
use vortex_scalar::{BoolScalar, PrimitiveScalar};

use crate::expressions::{BinaryExpr, Expr};
use crate::operators::{Associativity, Operator};
use crate::scalar::ScalarDisplayWrapper;

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Expr::BinaryExpr(expr) => write!(f, "{expr}"),
            Expr::Field(d) => write!(f, "{d}"),
            Expr::Literal(v) => {
                let wrapped = ScalarDisplayWrapper(v);
                write!(f, "{wrapped}")
            }
            Expr::Not(expr) => write!(f, "NOT {expr}"),
            Expr::Minus(expr) => write!(f, "(- {expr})"),
            Expr::IsNull(expr) => write!(f, "{expr} IS NULL"),
        }
    }
}

enum Side {
    Left,
    Right,
}

impl Display for BinaryExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fn write_inner(
            f: &mut Formatter<'_>,
            outer: &Expr,
            outer_op: Operator,
            side: Side,
        ) -> fmt::Result {
            if let Expr::BinaryExpr(inner) = outer {
                let inner_op_precedence = inner.op.precedence();
                // if the inner operator has higher precedence than the outer expression,
                // wrap it in parentheses to prevent inversion of priority
                if inner_op_precedence > outer_op.precedence() ||
                    // if the inner and outer operators have the same precedence, we need to
                    // account for operator associativity when determining grouping
                    (inner_op_precedence == outer_op.precedence() &&
                        match side {
                            Side::Left => {
                                outer_op.associativity() == Associativity::Left
                            }
                            Side::Right => {
                                outer_op.associativity() == Associativity::Right
                            }
                        })
                {
                    write!(f, "({inner})")?;
                } else {
                    write!(f, "{inner}")?;
                }
            } else if let Expr::Literal(scalar) = outer {
                // use alternative formatting for scalars
                let wrapped = ScalarDisplayWrapper(scalar);
                write!(f, "{wrapped}")?;
            } else {
                write!(f, "{outer}")?;
            }
            Ok(())
        }

        write_inner(f, self.left.as_ref(), self.op, Side::Left)?;
        write!(f, " {} ", self.op)?;
        write_inner(f, self.right.as_ref(), self.op, Side::Right)
    }
}

/// Alternative display for scalars
impl Display for ScalarDisplayWrapper<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.0.dtype() {
            DType::Null => write!(f, "null"),
            DType::Bool(_) => match BoolScalar::try_from(self.0).expect("bool").value() {
                None => write!(f, "null"),
                Some(b) => write!(f, "{}", b),
            },
            DType::Primitive(ptype, _) => match_each_native_ptype!(ptype, |$T| {
                match PrimitiveScalar::try_from(self.0).expect("primitive").typed_value::<$T>() {
                    None => write!(f, "null"),
                    Some(v) => write!(f, "{}{}", v,  std::any::type_name::<$T>()),
                }
            }),
            DType::Utf8(_) => todo!(),
            DType::Binary(_) => todo!(),
            DType::Struct(..) => todo!(),
            DType::List(..) => todo!(),
            DType::Extension(..) => todo!(),
        }
    }
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let display = match &self {
            Operator::And => "AND",
            Operator::Or => "OR",
            Operator::EqualTo => "=",
            Operator::NotEqualTo => "!=",
            Operator::GreaterThan => ">",
            Operator::GreaterThanOrEqualTo => ">=",
            Operator::LessThan => "<",
            Operator::LessThanOrEqualTo => "<=",
            Operator::Plus => "+",
            Operator::Minus | Operator::UnaryMinus => "-",
            Operator::Multiplication => "*",
            Operator::Division => "/",
            Operator::Modulo => "%",
        };
        write!(f, "{display}")
    }
}

#[cfg(test)]
mod tests {
    use crate::expression_fns::{equals, field};
    use crate::literal::lit;

    #[test]
    fn test_formatting() {
        // Addition
        assert_eq!(format!("{}", lit(1u32) + lit(2u32)), "1u32 + 2u32");
        // Subtraction
        assert_eq!(format!("{}", lit(1u32) - lit(2u32)), "1u32 - 2u32");
        // Multiplication
        assert_eq!(format!("{}", lit(1u32) * lit(2u32)), "1u32 * 2u32");
        // Division
        assert_eq!(format!("{}", lit(1u32) / lit(2u32)), "1u32 / 2u32");
        // Modulus
        assert_eq!(format!("{}", lit(1u32) % lit(2u32)), "1u32 % 2u32");
        // Negate
        assert_eq!(format!("{}", -lit(1u32)), "(- 1u32)");

        // And
        let string = format!("{}", lit(true).and(lit(false)));
        assert_eq!(string, "true AND false");
        // Or
        let string = format!("{}", lit(true).or(lit(false)));
        assert_eq!(string, "true OR false");
        // Not
        let string = format!("{}", !lit(1u32));
        assert_eq!(string, "NOT 1u32");
    }

    #[test]
    fn test_format_respects_operator_associativity() {
        let left = field("id").eq(lit(1));
        let right = field("id2").eq(-lit(2));
        let s = format!("{}", equals(left, right));
        assert_eq!(s, "id = 1i32 = (id2 = (- 2i32))")
    }
}
