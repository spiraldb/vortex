use std::any::Any;
use std::fmt::Debug;

pub use bool::*;
pub use nullable::*;
pub use primitive::*;
pub use struct_::*;
pub use utf8::*;

use crate::error::EncResult;
use crate::types::DType;

mod arrow;
mod bool;
mod equal;
mod nullable;
mod primitive;
mod struct_;
mod utf8;

pub trait Scalar: Debug + dyn_clone::DynClone + Send + Sync + 'static {
    /// convert itself to
    fn as_any(&self) -> &dyn Any;

    fn boxed(self) -> Box<dyn Scalar>;

    /// the logical type.
    fn dtype(&self) -> DType;

    fn cast(&self, dtype: &DType) -> EncResult<Box<dyn Scalar>>;
}

dyn_clone::clone_trait_object!(Scalar);
