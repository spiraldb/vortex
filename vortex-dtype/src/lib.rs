#![cfg(target_endian = "little")]

pub use dtype::*;
pub use extension::*;
pub use half;
pub use nullability::*;
pub use ptype::*;

mod dtype;
mod extension;
pub mod field;
mod nullability;
mod ptype;
mod serde;

#[cfg(feature = "proto")]
pub mod proto {
    pub use vortex_proto::dtype;
}

#[cfg(feature = "flatbuffers")]
pub mod flatbuffers {
    pub use vortex_flatbuffers::dtype::*;

    pub use super::serde::flatbuffers::*;
}
