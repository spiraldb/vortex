use std::fmt::{Display, Formatter};

pub use dtype::*;
pub use error::ErrString;
pub use error::SchemaError;
pub use error::SchemaResult;
pub use serde::Deserialize;
pub use serde::Serialize;

pub mod composite;
mod deserialize;
mod dtype;
mod error;
mod serde;

pub use deserialize::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd)]
pub struct CompositeID(pub &'static str);

impl Display for CompositeID {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub mod flatbuffers {
    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(clippy::all)]
    #[allow(non_camel_case_types)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/dtype.rs"));
    }
    pub use generated::vortex::dtype::*;
}
