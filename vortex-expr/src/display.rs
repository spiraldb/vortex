use core::fmt;
use std::fmt::{Display, Formatter};

use crate::expressions::{Predicate, Value};

impl Display for Predicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({} {} {})", self.lhs, self.op, self.rhs)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Value::Field(field_path) => Display::fmt(field_path, f),
            Value::Literal(scalar) => Display::fmt(&scalar, f),
        }
    }
}

#[cfg(test)]
mod tests {
    use vortex_dtype::field::{Field, FieldPath};

    use crate::expressions::{lit, Conjunction, Disjunction};
    use crate::field_paths::FieldPathOperations;

    #[test]
    fn test_predicate_formatting() {
        let f1 = Field::from("field");
        assert_eq!(format!("{}", f1.clone().lt(lit(1u32))), "($field < 1)");
        assert_eq!(format!("{}", f1.clone().gte(lit(1u32))), "($field >= 1)");
        assert_eq!(format!("{}", !f1.clone().lte(lit(1u32))), "($field > 1)");
        assert_eq!(format!("{}", !lit(1u32).lte(f1)), "($field <= 1)");

        // nested field path
        let f2 = FieldPath::builder().join("field").join(0).build();
        assert_eq!(format!("{}", !f2.lte(lit(1u32))), "($field.[0] > 1)");
    }

    #[test]
    fn test_dnf_formatting() {
        let path = FieldPath::builder().join(2).join("col1").build();
        let d1 = Conjunction {
            predicates: vec![
                lit(1u32).lt(path.clone()),
                path.clone().gte(lit(1u32)),
                !lit(1u32).lte(path),
            ],
        };
        let path2 = FieldPath::builder().join("col1").join(2).build();
        let d2 = Conjunction {
            predicates: vec![
                lit(2u32).lt(path2),
                lit(3u32).gte(field(2)),
                !lit(5u32).lte(field("col2")),
            ],
        };

        let dnf = Disjunction {
            conjunctions: vec![d1, d2],
        };

        let string = format!("{}", dnf);
        print!("{}", string);
        assert_eq!(
            string,
            "([2].$col1 >= 1) AND ([2].$col1 >= 1) AND ([2].$col1 <= 1)\nOR \
            \n($col1.[2] >= 2) AND ([2] < 3) AND ($col2 <= 5)"
        );
    }
}
