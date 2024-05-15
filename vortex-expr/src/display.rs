use core::fmt;
use std::fmt::{Display, Formatter};

use vortex_dtype::{match_each_native_ptype, DType};
use vortex_scalar::{BoolScalar, PrimitiveScalar};

use crate::expressions::{ConjunctionExpr, DNFExpr, FieldExpr, PredicateExpr, Value};
use crate::operators::Operator;
use crate::scalar::ScalarDisplayWrapper;

impl Display for DNFExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let iter = self.conjunctions.iter();
        let mut first = true;
        for conj in iter {
            if first {
                first = false;
            } else {
                write!(f, "\nOR \n")?;
            }

            write!(f, "{conj}")?;
        }
        Ok(())
    }
}

impl Display for ConjunctionExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let iter = self.predicates.iter();
        let mut first = true;
        for disj in iter {
            if first {
                first = false;
            } else {
                write!(f, " AND ")?;
            }

            write!(f, "{disj}")?;
        }
        Ok(())
    }
}

impl Display for PredicateExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
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

impl Display for FieldExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(&self.field_name, f)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Field(expr) => std::fmt::Display::fmt(expr, f),
            Value::Literal(scalar) => ScalarDisplayWrapper(scalar).fmt(f),
            Value::IsNull(field) => {
                write!(f, "{field} IS NULL")
            }
        }
    }
}

impl Display for Operator {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let display = match &self {
            Operator::EqualTo => "=",
            Operator::NotEqualTo => "!=",
            Operator::GreaterThan => ">",
            Operator::GreaterThanOrEqualTo => ">=",
            Operator::LessThan => "<",
            Operator::LessThanOrEqualTo => "<=",
        };
        write!(f, "{display}")
    }
}

#[cfg(test)]
mod tests {
    use crate::expressions::{ConjunctionExpr, DNFExpr};
    use crate::literal::lit;

    #[test]
    fn test_predicate_formatting() {
        // And
        assert_eq!(format!("{}", lit(1u32).lt(lit(2u32))), "(1u32 < 2u32)");
        // Or
        assert_eq!(format!("{}", lit(1u32).gte(lit(2u32))), "(1u32 >= 2u32)");
        // Not
        assert_eq!(format!("{}", !lit(1u32).lte(lit(2u32))), "(1u32 > 2u32)");
    }

    #[test]
    fn test_dnf_formatting() {
        let d1 = ConjunctionExpr {
            predicates: vec![
                lit(1u32).lt(lit(2u32)),
                lit(1u32).gte(lit(2u32)),
                !lit(1u32).lte(lit(2u32)),
            ],
        };
        let d2 = ConjunctionExpr {
            predicates: vec![
                lit(2u32).lt(lit(3u32)),
                lit(3u32).gte(lit(4u32)),
                !lit(5u32).lte(lit(6u32)),
            ],
        };

        let dnf = DNFExpr {
            conjunctions: vec![d1, d2],
        };

        let string = format!("{}", dnf);
        print!("{}", string);
        assert_eq!(
            string,
            "(1u32 < 2u32) AND (1u32 >= 2u32) AND (1u32 > 2u32)\nOR \n(2u32 < 3u32) AND (3u32 >= 4u32) AND (5u32 > 6u32)"
        );
    }
}
