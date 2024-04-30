use std::fmt::{Display, Formatter};

use linkme::distributed_slice;
use vortex_error::{vortex_err, VortexError};

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize))]
pub struct CompositeID(&'static str);

impl CompositeID {
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }
}

impl<'a> TryFrom<&'a str> for CompositeID {
    type Error = VortexError;

    fn try_from(value: &'a str) -> Result<Self, Self::Error> {
        find_composite_dtype(value)
            .map(|cdt| CompositeID(cdt.id()))
            .ok_or_else(|| vortex_err!("CompositeID not found for the given id: {}", value))
    }
}

impl AsRef<str> for CompositeID {
    fn as_ref(&self) -> &str {
        self.0
    }
}

impl Display for CompositeID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub trait CompositeDType {
    fn id(&self) -> &'static str;
}

#[distributed_slice]
pub static VORTEX_COMPOSITE_DTYPES: [&'static dyn CompositeDType] = [..];

pub fn find_composite_dtype(id: &str) -> Option<&'static dyn CompositeDType> {
    VORTEX_COMPOSITE_DTYPES
        .iter()
        .find(|ext| ext.id() == id)
        .copied()
}
