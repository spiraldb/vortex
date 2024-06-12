use core::fmt;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FieldPath {
    parts: Vec<FieldIdentifier>,
}

impl FieldPath {
    pub fn builder() -> FieldPathBuilder {
        FieldPathBuilder::default()
    }

    pub fn head(&self) -> Option<&FieldIdentifier> {
        self.parts.first()
    }

    pub fn tail(&self) -> Option<Self> {
        if self.head().is_none() {
            None
        } else {
            let new_parts = self.parts[1..self.parts.len()].to_vec();
            Some(Self::builder().join_all(new_parts).build())
        }
    }

    pub fn parts(&self) -> &[FieldIdentifier] {
        &self.parts
    }
}

#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum FieldIdentifier {
    Name(String),
    ListIndex(u64),
}

pub struct FieldPathBuilder {
    parts: Vec<FieldIdentifier>,
}

impl FieldPathBuilder {
    pub fn new() -> Self {
        Self { parts: Vec::new() }
    }

    pub fn push<T: Into<FieldIdentifier>>(&mut self, identifier: T) {
        self.parts.push(identifier.into());
    }

    pub fn join<T: Into<FieldIdentifier>>(mut self, identifier: T) -> Self {
        self.push(identifier);
        self
    }

    pub fn join_all(mut self, identifiers: Vec<impl Into<FieldIdentifier>>) -> Self {
        self.parts.extend(identifiers.into_iter().map(|v| v.into()));
        self
    }

    pub fn build(self) -> FieldPath {
        FieldPath { parts: self.parts }
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
        FieldPath { parts: vec![value] }
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
            .parts
            .iter()
            .map(|fid| format!("{fid}"))
            .collect::<Vec<_>>()
            .join(".");
        write!(f, "{}", formatted)
    }
}
