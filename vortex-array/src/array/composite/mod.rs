use std::fmt::{Display, Formatter};

use linkme::distributed_slice;

pub use array::*;
pub use typed::*;

mod array;
mod compress;
mod compute;
mod serde;
mod typed;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompositeID(pub &'static str);

impl Display for CompositeID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[distributed_slice]
pub static COMPOSITE_EXTENSIONS: [&'static dyn CompositeExtension] = [..];

pub fn find_extension(id: &str) -> Option<&'static dyn CompositeExtension> {
    COMPOSITE_EXTENSIONS
        .iter()
        .find(|ext| ext.id().0 == id)
        .copied()
}
