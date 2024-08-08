

#[cfg(feature = "dtype")]
#[path = "./generated/vortex.dtype.rs"]
pub mod dtype;

#[cfg(feature = "scalar")]
#[path = "./generated/vortex.scalar.rs"]
pub mod scalar;

#[cfg(feature = "expr")]
#[path = "./generated/vortex.expr.rs"]
pub mod expr;
