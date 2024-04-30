use std::fmt::{Display, Formatter};

pub use dtype::*;
pub use half;
pub use ptype::*;
mod deserialize;
mod dtype;
mod ptype;
mod serde;
mod serialize;

pub use deserialize::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "serde", derive(::serde::Serialize, ::serde::Deserialize))]
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
