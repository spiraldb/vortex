use std::any::Any;
use std::fmt::Debug;

use arrow2::scalar::Scalar as ArrowScalar;

pub use bool::*;
pub use nullable::*;
pub use primitive::*;
pub use struct_::*;
pub use utf8::*;

use crate::types::DType;

mod arrow;
mod bool;
mod equal;
mod nullable;
mod primitive;
mod struct_;
mod utf8;

pub trait Scalar: Debug + dyn_clone::DynClone + 'static {
    /// convert itself to
    fn as_any(&self) -> &dyn Any;

    fn boxed(self) -> Box<dyn Scalar>;

    /// the logical type.
    fn dtype(&self) -> &DType;
}

dyn_clone::clone_trait_object!(Scalar);
