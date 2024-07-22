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
    pub mod dtype {
        include!(concat!(env!("OUT_DIR"), "/proto/vortex.dtype.rs"));
    }
}

#[cfg(feature = "flatbuffers")]
pub mod flatbuffers {
    #[allow(clippy::all)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(unsafe_op_in_unsafe_fn)]
    #[allow(unused_imports)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/dtype.rs"));
    }

    pub use generated::vortex::dtype::*;
}
