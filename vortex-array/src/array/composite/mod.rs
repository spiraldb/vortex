use linkme::distributed_slice;
use std::fmt::{Display, Formatter};

use crate::array::composite::typed::{CompositeArrayPlugin, CompositeMetadata};

// mod as_arrow;
// mod compress;
// mod compute;
pub mod datetime;
// mod serde;
pub mod typed;
pub mod untyped;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompositeID(pub &'static str);

impl Display for CompositeID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[distributed_slice]
pub static COMPOSITE_ARRAYS: [&'static dyn CompositeArrayPlugin] = [..];
