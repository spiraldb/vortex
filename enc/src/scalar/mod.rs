use std::any::Any;
use std::fmt::{Debug, Display};

pub use binary::*;
pub use bool::*;
pub use list::*;
pub use localtime::*;
pub use null::*;
pub use nullable::*;
pub use primitive::*;
pub use struct_::*;
pub use utf8::*;

use crate::dtype::DType;
use crate::error::EncResult;

mod arrow;
mod binary;
mod bool;
mod equal;
mod list;
mod localtime;
mod null;
mod nullable;
mod ord;
mod primitive;
mod struct_;
mod utf8;

pub trait Scalar: Display + Debug + dyn_clone::DynClone + Send + Sync + 'static {
    /// convert itself to
    fn as_any(&self) -> &dyn Any;

    fn into_any(self: Box<Self>) -> Box<dyn Any>;

    fn boxed(self) -> Box<dyn Scalar>;

    /// the logical type.
    fn dtype(&self) -> &DType;

    fn cast(&self, dtype: &DType) -> EncResult<Box<dyn Scalar>>;

    fn nbytes(&self) -> usize;
}

dyn_clone::clone_trait_object!(Scalar);
