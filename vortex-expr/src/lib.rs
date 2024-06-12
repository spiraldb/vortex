#![feature(iter_intersperse)]

extern crate core;
extern crate core;

mod datafusion;
mod display;
mod expressions;
mod field_paths;
mod operators;
mod serde_proto;

pub use expressions::*;
pub use field_paths::*;
pub use operators::*;

#[cfg(feature = "proto")]
pub mod proto {
    pub mod expr {
        include!(concat!(env!("OUT_DIR"), "/proto/vortex.expr.rs"));
    }

    pub use vortex_dtype::proto::dtype;
    pub use vortex_scalar::proto::scalar;
}
