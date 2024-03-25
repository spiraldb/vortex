pub mod array;
pub mod arrow;
pub mod scalar;

pub mod accessor;
pub mod compress;
pub mod compute;
pub mod datetime;
pub mod encode;
pub mod error;
pub mod formatter;
pub mod iterator;
pub mod ptype;
mod sampling;
pub mod serde;
pub mod stats;

#[allow(unused_imports)]
#[allow(dead_code)]
#[allow(clippy::needless_lifetimes)]
#[allow(clippy::extra_unused_lifetimes)]
#[allow(non_camel_case_types)]
mod flatbuffers {
    mod generated {
        include!(concat!(env!("OUT_DIR"), "/flatbuffers/scalar.rs"));
    }
    pub use generated::vortex::*;
    pub use vortex_schema::flatbuffers::*;
}
