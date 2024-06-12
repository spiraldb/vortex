use core::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FieldPath {
    field_names: Vec<FieldIdentifier>,
}

impl FieldPath {
    pub fn builder() -> FieldPathBuilder {
        FieldPathBuilder::default()
    }

    pub fn head(&self) -> Option<&FieldIdentifier> {
        self.field_names.first()
    }

    pub fn tail(&self) -> Option<Self> {
        if self.head().is_none() {
            None
        } else {
            let new_field_names = self.field_names[1..self.field_names.len()].to_vec();
            Some(Self::builder().join_all(new_field_names).build())
        }
    }

    pub fn parts(&self) -> &[FieldIdentifier] {
        &self.field_names
    }
}

#[derive(Clone, Debug, PartialEq)]
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

    pub fn join<T: Into<FieldIdentifier>>(mut self, identifier: T) -> Self {
        self.field_names.push(identifier.into());
        self
    }

    pub fn join_all(mut self, identifiers: Vec<impl Into<FieldIdentifier>>) -> Self {
        self.field_names
            .extend(identifiers.into_iter().map(|v| v.into()));
        self
    }

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
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FieldIdentifier::Name(name) => write!(f, "${name}"),
            FieldIdentifier::ListIndex(idx) => write!(f, "[{idx}]"),
        }
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
