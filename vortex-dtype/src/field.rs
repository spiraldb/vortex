use core::fmt;
use std::fmt::{Display, Formatter};

use itertools::Itertools;
use vortex_error::vortex_panic;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Field {
    Name(String),
    Index(usize),
}

impl From<&str> for Field {
    fn from(value: &str) -> Self {
        Field::Name(value.into())
    }
}

impl From<String> for Field {
    fn from(value: String) -> Self {
        Field::Name(value)
    }
}

impl From<usize> for Field {
    fn from(value: usize) -> Self {
        Field::Index(value)
    }
}

impl Display for Field {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Field::Name(name) => write!(f, "${name}"),
            Field::Index(idx) => write!(f, "[{idx}]"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FieldPath(Vec<Field>);

impl FieldPath {
    pub fn root() -> Self {
        Self(vec![])
    }

    pub fn from_name<F: Into<Field>>(name: F) -> Self {
        Self(vec![name.into()])
    }

    pub fn path(&self) -> &[Field] {
        &self.0
    }

    pub fn to_name(&self) -> &str {
        assert_eq!(self.0.len(), 1);
        match &self.0[0] {
            Field::Name(name) => name.as_str(),
            _ => vortex_panic!("FieldPath is not a name: {}", self),
        }
    }
}

impl FromIterator<Field> for FieldPath {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        FieldPath(iter.into_iter().collect())
    }
}

impl From<Field> for FieldPath {
    fn from(value: Field) -> Self {
        FieldPath(vec![value])
    }
}

impl From<Vec<Field>> for FieldPath {
    fn from(value: Vec<Field>) -> Self {
        FieldPath(value)
    }
}

impl Display for FieldPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0.iter().format("."), f)
    }
}
