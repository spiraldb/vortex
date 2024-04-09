#![allow(dead_code)]

extern crate core;

pub mod array;
mod arrow;
mod batch;
pub mod compute;
mod context;
mod data;
pub mod encoding;
mod implementation;
mod metadata;
mod stats;
mod tree;
mod validity;
mod view;
mod visitor;

use std::fmt::{Debug, Display, Formatter};

use arrow_buffer::Buffer;
pub use batch::*;
pub use context::*;
pub use data::*;
pub use implementation::*;
pub use linkme;
pub use metadata::*;
pub use view::*;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::compute::ArrayCompute;
use crate::encoding::EncodingRef;
use crate::stats::{ArrayStatistics, Statistics};
use crate::validity::ArrayValidity;
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};

#[derive(Debug, Clone)]
pub enum Array<'v> {
    Data(ArrayData),
    DataRef(&'v ArrayData),
    View(ArrayView<'v>),
}

pub type OwnedArray = Array<'static>;

impl Array<'_> {
    pub fn encoding(&self) -> EncodingRef {
        match self {
            Array::Data(d) => d.encoding(),
            Array::DataRef(d) => d.encoding(),
            Array::View(v) => v.encoding(),
        }
    }

    pub fn dtype(&self) -> &DType {
        match self {
            Array::Data(d) => d.dtype(),
            Array::DataRef(d) => d.dtype(),
            Array::View(v) => v.dtype(),
        }
    }

    pub fn len(&self) -> usize {
        self.with_array(|a| a.len())
    }

    pub fn is_empty(&self) -> bool {
        self.with_array(|a| a.is_empty())
    }

    pub fn into_typed_data<D: ArrayDef>(self) -> Option<TypedArrayData<D>> {
        TypedArrayData::<D>::try_from(self.into_array_data()).ok()
    }

    pub fn to_static(&self) -> Array<'static> {
        Array::Data(self.to_array_data())
    }
}

impl<'a> Array<'a> {
    pub fn to_typed_array<D: ArrayDef>(&self) -> Option<D::Array<'a>> {
        // D::Array::try_from_parts(self, &D::Metadata::default()).ok()
        todo!()
    }
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

pub trait IntoArrayData {
    fn into_array_data(self) -> ArrayData;
}

pub trait WithArray {
    fn with_array<R, F: FnMut(&dyn ArrayTrait) -> R>(&self, f: F) -> R;
}

pub trait ArrayParts {
    fn dtype(&self) -> &DType;
    fn buffer(&self, idx: usize) -> Option<&Buffer>;
    fn child<'a>(&'a self, idx: usize, dtype: &'a DType) -> Option<Array>;
    fn nchildren(&self) -> usize;
    fn statistics(&self) -> &dyn Statistics;
}

pub trait TryFromArrayParts<'v, M: ArrayMetadata>: Sized + 'v {
    fn try_from_parts(parts: &'v dyn ArrayParts, metadata: &'v M) -> VortexResult<Self>;
}

/// Collects together the behaviour of an array.
pub trait ArrayTrait:
    ArrayCompute + ArrayValidity + AcceptArrayVisitor + ArrayStatistics + ToArrayData
{
    fn dtype(&self) -> &DType;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        // TODO(ngates): remove this default impl to encourage explicit implementation
        self.len() == 0
    }

    fn nbytes(&self) -> usize {
        let mut visitor = NBytesVisitor(0);
        self.accept(&mut visitor).unwrap();
        visitor.0
    }
}

struct NBytesVisitor(usize);
impl ArrayVisitor for NBytesVisitor {
    fn visit_column(&mut self, name: &str, array: &Array) -> VortexResult<()> {
        self.visit_child(name, array)
    }

    fn visit_child(&mut self, _name: &str, array: &Array) -> VortexResult<()> {
        self.0 += array.with_array(|a| a.nbytes());
        Ok(())
    }

    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()> {
        self.0 += buffer.len();
        Ok(())
    }
}

// TODO(ngates): I think we can remove IntoArrayData, make everything take self, and then
//  implement for reference?
impl ToArrayData for Array<'_> {
    fn to_array_data(&self) -> ArrayData {
        match self {
            Array::Data(d) => d.clone(),
            Array::DataRef(d) => (*d).clone(),
            Array::View(v) => v.encoding().with_view(v, |a| a.to_array_data()),
        }
    }
}

impl IntoArrayData for Array<'_> {
    fn into_array_data(self) -> ArrayData {
        match self {
            Array::Data(d) => d,
            Array::DataRef(d) => d.clone(),
            Array::View(v) => v.encoding().with_view(&v, |a| a.to_array_data()),
        }
    }
}

impl WithArray for Array<'_> {
    fn with_array<R, F: FnMut(&dyn ArrayTrait) -> R>(&self, f: F) -> R {
        match self {
            Array::Data(d) => d.encoding().with_data(d, f),
            Array::DataRef(d) => d.encoding().with_data(d, f),
            Array::View(v) => v.encoding().with_view(v, f),
        }
    }
}

impl Display for Array<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let prefix = match self {
            Array::Data(_) => "",
            Array::DataRef(_) => "&",
            Array::View(_) => "$",
        };
        write!(
            f,
            "{}{}({}, len={})",
            prefix,
            self.encoding().id(),
            self.dtype(),
            self.len()
        )
    }
}
