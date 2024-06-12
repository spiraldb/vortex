use core::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Field {
    Name(String),
    Index(i32),
}

impl From<&str> for Field {
    fn from(value: &str) -> Self {
        Field::Name(value.into())
    }
}

impl From<i32> for Field {
    fn from(value: i32) -> Self {
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
    pub fn from_name(name: &str) -> Self {
        Self(vec![Field::from(name)])
    }

    pub fn path(&self) -> &[Field] {
        &self.0
    }

    pub fn to_name(&self) -> &str {
        assert_eq!(self.0.len(), 1);
        match &self.0[0] {
            Field::Name(name) => name.as_str(),
            _ => panic!("FieldPath is not a name"),
        }
    }
}

impl FromIterator<Field> for FieldPath {
    fn from_iter<T: IntoIterator<Item = Field>>(iter: T) -> Self {
        FieldPath(iter.into_iter().collect())
    }
}

impl From<Vec<Field>> for FieldPath {
    fn from(value: Vec<Field>) -> Self {
        FieldPath(value)
    }
}

impl Display for FieldPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let formatted = self
            .0
            .iter()
            .map(|fid| format!("{fid}"))
            .collect::<Vec<_>>()
            .join(".");
        write!(f, "{}", formatted)
    }
}
