#![allow(dead_code)]

pub mod array;
pub mod compute;
mod context;
mod data;
pub mod encoding;
mod implementation;
mod metadata;
mod validity;
mod view;

use std::fmt::Debug;

use arrow_buffer::Buffer;
pub use context::*;
pub use data::*;
pub use implementation::*;
pub use metadata::*;
pub use view::*;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::compute::ArrayCompute;
use crate::validity::ArrayValidity;

#[derive(Debug, Clone)]
pub enum Array<'v> {
    Data(ArrayData),
    DataRef(&'v ArrayData),
    View(ArrayView<'v>),
}

pub trait ToArray {
    fn to_array(&self) -> Array;
}

pub trait IntoArray<'a> {
    fn into_array(self) -> Array<'a>;
}

pub trait ToArrayData {
    fn to_array_data(&self) -> ArrayData;
}

pub trait WithArray {
    fn with_array<R, F: Fn(&dyn ArrayTrait) -> R>(&self, f: F) -> R;
}

pub trait ArrayParts<'a> {
    fn buffer(&'a self, idx: usize) -> Option<&'a Buffer>;
    fn child(&'a self, idx: usize, dtype: &'a DType) -> Option<Array<'a>>;
}

pub trait TryFromArrayParts<'v, M: ArrayMetadata>: Sized + 'v {
    fn try_from_parts(parts: &'v dyn ArrayParts<'v>, metadata: M) -> VortexResult<Self>;
}

pub trait TryParseArrayMetadata: Sized + ArrayMetadata {
    fn try_parse_metadata(metadata: Option<&[u8]>) -> VortexResult<Self>;
}

/// Collects together the behaviour of an array.
pub trait ArrayTrait: ArrayCompute + ArrayValidity + ToArrayData {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        // TODO(ngates): remove this default impl to encourage explicit implementation
        self.len() == 0
    }
}

impl ToArrayData for Array<'_> {
    fn to_array_data(&self) -> ArrayData {
        match self {
            Array::Data(d) => d.encoding().with_data(d, |a| a.to_array_data()),
            Array::DataRef(d) => d.encoding().with_data(d, |a| a.to_array_data()),
            Array::View(v) => v.encoding().with_view(v, |a| a.to_array_data()),
        }
    }
}

impl WithArray for Array<'_> {
    fn with_array<R, F: Fn(&dyn ArrayTrait) -> R>(&self, f: F) -> R {
        match self {
            Array::Data(d) => d.encoding().with_data(d, f),
            Array::DataRef(d) => d.encoding().with_data(d, f),
            Array::View(v) => v.encoding().with_view(v, f),
        }
    }
}
