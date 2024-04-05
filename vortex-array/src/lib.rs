extern crate core;

pub mod array;
pub mod arrow;
pub mod scalar;

pub mod accessor;
mod array2;
pub mod compress;
pub mod compute;
pub mod datetime;
pub mod encode;
pub mod encoding;
pub mod formatter;
pub mod iterator;
pub mod ptype;
mod sampling;
pub mod serde;
pub mod stats;
pub mod validity;
pub mod view;
mod walk;

pub use walk::*;

pub mod flatbuffers {
    pub use generated::vortex::*;

    #[allow(unused_imports)]
    #[allow(dead_code)]
    #[allow(non_camel_case_types)]
    #[allow(clippy::all)]
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/array.rs"));
    }
}
