#![allow(clippy::all)]

#[cfg(feature = "dtype")]
#[allow(clippy::all)]
#[rustfmt::skip]
#[path = "./generated/vortex.dtype.rs"]
pub mod dtype;

#[cfg(feature = "scalar")]
#[allow(clippy::all)]
#[rustfmt::skip]
#[path = "./generated/vortex.scalar.rs"]
pub mod scalar;

#[cfg(feature = "expr")]
#[allow(clippy::all)]
#[rustfmt::skip]
#[path = "./generated/vortex.expr.rs"]
pub mod expr;
