// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
