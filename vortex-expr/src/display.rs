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
        let f1 = FieldPath::from_name("field");
        assert_eq!(format!("{}", f1.lt(lit(1u32))), "($field < 1)");
        assert_eq!(format!("{}", f1.gte(lit(1u32))), "($field >= 1)");
        assert_eq!(format!("{}", !f1.lte(lit(1u32))), "($field > 1)");
        assert_eq!(format!("{}", !lit(1u32).lte(f1)), "($field <= 1)");

        // nested field path
        let f2 = FieldPath::from_iter([Field::from("field"), Field::from(0)]);
        assert_eq!(format!("{}", !f2.lte(lit(1u32))), "($field.[0] > 1)");
    }

    #[test]
    fn test_dnf_formatting() {
        let path = FieldPath::from_iter([Field::from(2), Field::from("col1")]);
        let d1 = Conjunction::from_iter([
            lit(1u32).lt(path.clone()),
            path.clone().gte(lit(1u32)),
            !lit(1u32).lte(path),
        ]);
        let path2 = FieldPath::from_iter([Field::from("col1"), Field::from(2)]);
        let d2 = Conjunction::from_iter([
            lit(2u32).lt(path2),
            lit(3u32).gte(Field::from(2)),
            !lit(5u32).lte(Field::from("col2")),
        ]);

        let dnf = Disjunction::from_iter([d1, d2]);

        let string = format!("{}", dnf);
        print!("{}", string);
        assert_eq!(
            string,
            "([2].$col1 >= 1) AND ([2].$col1 >= 1) AND ([2].$col1 <= 1)\nOR \
            \n($col1.[2] >= 2) AND ([2] < 3) AND ($col2 <= 5)"
        );
    }
}
