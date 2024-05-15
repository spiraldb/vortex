use core::fmt;
use std::fmt::{Display, Formatter};

use crate::expressions::{ConjunctionExpr, DNFExpr, FieldExpr, PredicateExpr, Value};
use crate::operators::Operator;

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

impl Display for FieldExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(&self.field_name, f)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Field(expr) => std::fmt::Display::fmt(expr, f),
            Value::Literal(scalar) => scalar.fmt(f),
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
        assert_eq!(format!("{}", lit(1u32).lt(lit(2u32))), "(1 < 2)");
        // Or
        assert_eq!(format!("{}", lit(1u32).gte(lit(2u32))), "(1 >= 2)");
        // Not
        assert_eq!(format!("{}", !lit(1u32).lte(lit(2u32))), "(1 > 2)");
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
            "(1 < 2) AND (1 >= 2) AND (1 > 2)\nOR \n(2 < 3) AND (3 >= 4) AND (5 > 6)"
        );
    }
}
