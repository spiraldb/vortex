#![cfg(target_endian = "little")]

pub use dtype::*;
pub use extension::*;
pub use half;
pub use nullability::*;
pub use ptype::*;
mod dtype;
mod extension;
mod nullability;
mod ptype;
mod serde;

#[cfg(feature = "prost")]
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/proto/vortex.dtype.rs"));
}

#[cfg(feature = "flatbuffers")]
pub mod flatbuffers {
    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(dead_code)]
    #[allow(clippy::all)]
    #[allow(non_camel_case_types)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/dtype.rs"));
    }
    pub use generated::vortex::dtype::*;
}
