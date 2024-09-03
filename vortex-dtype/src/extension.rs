use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
pub struct ExtID(Arc<str>);

impl ExtID {
    pub fn new(value: Arc<str>) -> Self {
        Self(value)
    }

    pub fn python_repr(&self) -> ExtIDPythonRepr {
        ExtIDPythonRepr { ext_id: self }
    }
}

impl Display for ExtID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for ExtID {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<&str> for ExtID {
    fn from(value: &str) -> Self {
        Self(value.into())
    }
}

pub struct ExtIDPythonRepr<'a> {
    ext_id: &'a ExtID,
}

impl Display for ExtIDPythonRepr<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.ext_id {
            ExtID(id) => write!(f, "\"{}\"", id.escape_default()),
        }
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExtMetadata(Arc<[u8]>);

impl ExtMetadata {
    pub fn new(value: Arc<[u8]>) -> Self {
        Self(value)
    }

    pub fn python_repr(&self) -> ExtMetadataPythonRepr {
        ExtMetadataPythonRepr { ext_metadata: self }
    }
}

impl AsRef<[u8]> for ExtMetadata {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl From<&[u8]> for ExtMetadata {
    fn from(value: &[u8]) -> Self {
        Self(value.into())
    }
}

pub struct ExtMetadataPythonRepr<'a> {
    ext_metadata: &'a ExtMetadata,
}

impl Display for ExtMetadataPythonRepr<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.ext_metadata {
            ExtMetadata(metadata) => write!(f, "\"{}\"", metadata.escape_ascii()),
        }
    }
}

#[derive(Debug, Clone, PartialOrd, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ExtDType {
    id: ExtID,
    metadata: Option<ExtMetadata>,
}

impl ExtDType {
    pub fn new(id: ExtID, metadata: Option<ExtMetadata>) -> Self {
        Self { id, metadata }
    }

    #[inline]
    pub fn id(&self) -> &ExtID {
        &self.id
    }

    #[inline]
    pub fn metadata(&self) -> Option<&ExtMetadata> {
        self.metadata.as_ref()
    }
}
