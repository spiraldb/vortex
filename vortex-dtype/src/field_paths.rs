use core::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FieldPath {
    pub field_names: Vec<FieldIdentifier>,
}

impl FieldPath {
    pub fn builder() -> FieldPathBuilder {
        FieldPathBuilder::default()
    }
}

#[derive(Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FieldIdentifier {
    Name(String),
    ListIndex(u64),
}

pub struct FieldPathBuilder {
    field_names: Vec<FieldIdentifier>,
}

impl FieldPathBuilder {
    pub fn new() -> Self {
        Self {
            field_names: Vec::new(),
        }
    }

    // Adds a field identifier to the path.
    pub fn join<T: Into<FieldIdentifier>>(mut self, identifier: T) -> Self {
        self.field_names.push(identifier.into());
        self
    }

    // Builds the FieldPath object.
    pub fn build(self) -> FieldPath {
        FieldPath {
            field_names: self.field_names,
        }
    }
}

impl Default for FieldPathBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub fn field(x: impl Into<FieldIdentifier>) -> FieldPath {
    x.into().into()
}

impl From<FieldIdentifier> for FieldPath {
    fn from(value: FieldIdentifier) -> Self {
        FieldPath {
            field_names: vec![value],
        }
    }
}

impl From<&str> for FieldIdentifier {
    fn from(value: &str) -> Self {
        FieldIdentifier::Name(value.to_string())
    }
}

impl From<u64> for FieldIdentifier {
    fn from(value: u64) -> Self {
        FieldIdentifier::ListIndex(value)
    }
}

impl Display for FieldIdentifier {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let formatted = match self {
            FieldIdentifier::Name(name) => format! {"${name}"},
            FieldIdentifier::ListIndex(idx) => format! {"[{idx}]"},
        };
        write!(f, "{}", formatted)
    }
}

impl Display for FieldPath {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let formatted = self
            .field_names
            .iter()
            .map(|fid| format!("{fid}"))
            .collect::<Vec<_>>()
            .join(".");
        write!(f, "{}", formatted)
    }
}
