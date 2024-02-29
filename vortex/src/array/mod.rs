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
use std::fmt::{Debug, Display, Formatter};

use arrow::array::ArrayRef as ArrowArrayRef;
use linkme::distributed_slice;

use crate::array::bool::{BoolArray, BOOL_ENCODING};
use crate::array::chunked::{ChunkedArray, CHUNKED_ENCODING};
use crate::array::constant::{ConstantArray, CONSTANT_ENCODING};
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::{PrimitiveArray, PRIMITIVE_ENCODING};
use crate::array::sparse::{SparseArray, SPARSE_ENCODING};
use crate::array::struct_::{StructArray, STRUCT_ENCODING};
use crate::array::typed::{TypedArray, TYPED_ENCODING};
use crate::array::varbin::{VarBinArray, VARBIN_ENCODING};
use crate::array::varbinview::{VarBinViewArray, VARBINVIEW_ENCODING};
use crate::compress::EncodingCompression;
use crate::compute::ArrayCompute;
use crate::dtype::{DType, Nullability};
use crate::error::{VortexError, VortexResult};
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::scalar::Scalar;
use crate::serde::{ArraySerde, EncodingSerde};
use crate::stats::Stats;

pub mod bool;
pub mod chunked;
pub mod constant;
pub mod downcast;
pub mod primitive;
pub mod sparse;
pub mod struct_;
pub mod typed;
pub mod varbin;
pub mod varbinview;

pub type ArrowIterator = dyn Iterator<Item = ArrowArrayRef>;
pub type ArrayRef = Box<dyn Array>;

/// An Enc Array is the base object representing all arrays in enc.
///
/// Arrays have a dtype and an encoding. DTypes represent the logical type of the
/// values stored in a vortex array. Encodings represent the physical layout of the
/// array.
///
/// This differs from Apache Arrow where logical and physical are combined in
/// the data type, e.g. LargeString, RunEndEncoded.
pub trait Array: ArrayDisplay + Debug + Send + Sync + dyn_clone::DynClone + 'static {
    /// Converts itself to a reference of [`Any`], which enables downcasting to concrete types.
    fn as_any(&self) -> &dyn Any;
    /// Move an owned array to `ArrayRef`
    fn boxed(self) -> ArrayRef;
    /// Convert boxed array into `Box<dyn Any>`
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
    /// Get the length of the array
    fn len(&self) -> usize;
    /// Check whether the array is empty
    fn is_empty(&self) -> bool;
    /// Get the dtype of the array
    fn dtype(&self) -> &DType;
    /// Get statistics for the array
    fn stats(&self) -> Stats;
    /// Get scalar value at given index
    fn scalar_at(&self, index: usize) -> VortexResult<Box<dyn Scalar>>;
    /// Produce arrow batches from the encoding
    fn iter_arrow(&self) -> Box<ArrowIterator>;
    /// Limit array to start..stop range
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef>;
    /// Encoding kind of the array
    fn encoding(&self) -> &'static dyn Encoding;
    /// Approximate size in bytes of the array. Only takes into account variable size portion of the array
    fn nbytes(&self) -> usize;

    fn compute(&self) -> Option<&dyn ArrayCompute> {
        None
    }

    fn serde(&self) -> &dyn ArraySerde;
}

dyn_clone::clone_trait_object!(Array);

pub fn check_slice_bounds(array: &dyn Array, start: usize, stop: usize) -> VortexResult<()> {
    if start > array.len() {
        return Err(VortexError::OutOfBounds(start, 0, array.len()));
    }
    if stop > array.len() {
        return Err(VortexError::OutOfBounds(stop, 0, array.len()));
    }
    Ok(())
}

pub fn check_index_bounds(array: &dyn Array, index: usize) -> VortexResult<()> {
    if index >= array.len() {
        return Err(VortexError::OutOfBounds(index, 0, array.len()));
    }
    Ok(())
}

pub fn check_validity_buffer(validity: Option<&ArrayRef>) -> VortexResult<()> {
    if validity
        .map(|v| !matches!(v.dtype(), DType::Bool(Nullability::NonNullable)))
        .unwrap_or(false)
    {
        return Err(VortexError::MismatchedTypes(
            validity.unwrap().dtype().clone(),
            DType::Bool(Nullability::NonNullable),
        ));
    }
    Ok(())
}

impl<'a> AsRef<(dyn Array + 'a)> for dyn Array {
    fn as_ref(&self) -> &(dyn Array + 'a) {
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct EncodingId(&'static str);

impl EncodingId {
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    #[inline]
    pub fn name(&self) -> &str {
        self.0
    }
}

impl Display for EncodingId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.0, f)
    }
}

pub trait Encoding: Debug + Send + Sync + 'static {
    fn id(&self) -> &EncodingId;

    /// Implementation of the array compression trait
    fn compression(&self) -> Option<&dyn EncodingCompression>;

    /// Array serialization
    fn serde(&self) -> Option<&dyn EncodingSerde>;
}

pub type EncodingRef = &'static dyn Encoding;

#[distributed_slice]
pub static ENCODINGS: [EncodingRef] = [..];

#[derive(Debug, Clone)]
pub enum ArrayKind<'a> {
    Bool(&'a BoolArray),
    Chunked(&'a ChunkedArray),
    Constant(&'a ConstantArray),
    Primitive(&'a PrimitiveArray),
    Sparse(&'a SparseArray),
    Struct(&'a StructArray),
    Typed(&'a TypedArray),
    VarBin(&'a VarBinArray),
    VarBinView(&'a VarBinViewArray),
    Other(&'a dyn Array),
}

impl<'a> From<&'a dyn Array> for ArrayKind<'a> {
    fn from(value: &'a dyn Array) -> Self {
        match *value.encoding().id() {
            BOOL_ENCODING => ArrayKind::Bool(value.as_bool()),
            CHUNKED_ENCODING => ArrayKind::Chunked(value.as_chunked()),
            CONSTANT_ENCODING => ArrayKind::Constant(value.as_constant()),
            PRIMITIVE_ENCODING => ArrayKind::Primitive(value.as_primitive()),
            SPARSE_ENCODING => ArrayKind::Sparse(value.as_sparse()),
            STRUCT_ENCODING => ArrayKind::Struct(value.as_struct()),
            TYPED_ENCODING => ArrayKind::Typed(value.as_typed()),
            VARBIN_ENCODING => ArrayKind::VarBin(value.as_varbin()),
            VARBINVIEW_ENCODING => ArrayKind::VarBinView(value.as_varbinview()),
            _ => ArrayKind::Other(value),
        }
    }
}

impl Display for dyn Array {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        ArrayFormatter::new(f, "".to_string(), self.nbytes()).array(self)
    }
}
