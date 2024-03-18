pub use dtype::*;
pub use error::ErrString;
pub use error::SchemaError;
pub use error::SchemaResult;
use std::fmt::{Display, Formatter};

mod dtype;
mod error;
mod generated;
mod serde;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompositeID(pub &'static str);

impl Display for CompositeID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
