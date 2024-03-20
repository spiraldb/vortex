use linkme::distributed_slice;
use std::fmt::{Display, Formatter};

mod array;
mod compress;
mod compute;
mod serde;
mod typed;

pub use array::*;
pub use typed::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompositeID(pub &'static str);

impl Display for CompositeID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[distributed_slice]
pub static COMPOSITE_EXTENSIONS: [&'static dyn CompositeExtension] = [..];

pub fn find_extension(id: CompositeID) -> Option<&'static dyn CompositeExtension> {
    COMPOSITE_EXTENSIONS
        .iter()
        .copied()
        .find(|ext| ext.id() == id)
}
