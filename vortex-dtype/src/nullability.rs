use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum Nullability {
    #[default]
    NonNullable,
    Nullable,
}

impl Nullability {
    pub fn python_repr(&self) -> NullabilityPythonRepr {
        NullabilityPythonRepr { nullability: self }
    }
}

impl From<bool> for Nullability {
    fn from(value: bool) -> Self {
        if value {
            Self::Nullable
        } else {
            Self::NonNullable
        }
    }
}

impl From<Nullability> for bool {
    fn from(value: Nullability) -> Self {
        match value {
            Nullability::NonNullable => false,
            Nullability::Nullable => true,
        }
    }
}

impl Display for Nullability {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonNullable => write!(f, ""),
            Self::Nullable => write!(f, "?"),
        }
    }
}

pub struct NullabilityPythonRepr<'a> {
    nullability: &'a Nullability,
}

impl Display for NullabilityPythonRepr<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.nullability {
            Nullability::NonNullable => write!(f, "False"),
            Nullability::Nullable => write!(f, "True"),
        }
    }
}
