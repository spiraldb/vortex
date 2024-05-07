#![cfg(target_endian = "little")]

pub use dtype::*;
pub use extension::*;
pub use half;
pub use ptype::*;
mod deserialize;
mod dtype;
mod extension;
mod ptype;
mod serde;
mod serialize;

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
