use core::fmt;
use std::fmt::{Display, Formatter};

use crate::expressions::{Conjunction, Disjunction, Predicate, Value};
use crate::operators::Operator;

impl Display for Disjunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.conjunctions
            .iter()
            .map(|v| format!("{}", v))
            .intersperse("\nOR \n".to_string())
            .try_for_each(|s| write!(f, "{}", s))
    }
}

impl Display for Conjunction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        self.predicates
            .iter()
            .map(|v| format!("{}", v))
            .intersperse(" AND ".to_string())
            .try_for_each(|s| write!(f, "{}", s))
    }
}

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.left, self.op, self.right)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Field(expr) => std::fmt::Display::fmt(expr, f),
            Value::Literal(scalar) => scalar.fmt(f),
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
    use crate::expressions::{lit, Conjunction, Disjunction};

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
        let d1 = Conjunction {
            predicates: vec![
                lit(1u32).lt(lit(2u32)),
                lit(1u32).gte(lit(2u32)),
                !lit(1u32).lte(lit(2u32)),
            ],
        };
        let d2 = Conjunction {
            predicates: vec![
                lit(2u32).lt(lit(3u32)),
                lit(3u32).gte(lit(4u32)),
                !lit(5u32).lte(lit(6u32)),
            ],
        };

        let dnf = Disjunction {
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
