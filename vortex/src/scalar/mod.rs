use std::any::Any;
use std::fmt::{Debug, Display};

pub use binary::*;
pub use bool::*;
pub use list::*;
pub use localtime::*;
pub use null::*;
pub use nullable::*;
pub use primitive::*;
pub use serde::*;
pub use struct_::*;
pub use utf8::*;

use crate::dtype::DType;
use crate::error::VortexResult;
use crate::ptype::NativePType;

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
mod serde;
mod struct_;
mod utf8;

pub trait Scalar: Display + Debug + dyn_clone::DynClone + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;

    fn into_any(self: Box<Self>) -> Box<dyn Any>;

    fn as_nonnull(&self) -> Option<&dyn Scalar>;

    fn into_nonnull(self: Box<Self>) -> Option<Box<dyn Scalar>>;

    fn boxed(self) -> Box<dyn Scalar>;

    /// the logical type.
    fn dtype(&self) -> &DType;

    fn cast(&self, dtype: &DType) -> VortexResult<Box<dyn Scalar>>;

    fn nbytes(&self) -> usize;
}

dyn_clone::clone_trait_object!(Scalar);

/// Allows conversion from Enc scalars to a byte slice.
pub trait AsBytes {
    /// Converts this instance into a byte slice
    fn as_bytes(&self) -> &[u8];
}

impl<T: NativePType> From<Option<T>> for Box<dyn Scalar>
where
    Box<dyn Scalar>: From<T>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(value) => value.into(),
            None => Box::new(NullableScalar::None(DType::from(T::PTYPE))),
        }
    }
}

impl<T: NativePType> AsBytes for [T] {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        let raw_ptr = self.as_ptr() as *const u8;
        unsafe { std::slice::from_raw_parts(raw_ptr, std::mem::size_of_val(self)) }
    }
}

impl<T: NativePType> AsBytes for &[T] {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        let raw_ptr = (*self).as_ptr() as *const u8;
        unsafe { std::slice::from_raw_parts(raw_ptr, std::mem::size_of_val(*self)) }
    }
}

impl<T: NativePType> AsBytes for T {
    #[inline]
    fn as_bytes(&self) -> &[u8] {
        let raw_ptr = self as *const T as *const u8;
        unsafe { std::slice::from_raw_parts(raw_ptr, std::mem::size_of::<T>()) }
    }
}
