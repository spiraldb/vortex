//! Vortex crate containing core logic for encoding and memory representation of [arrays](Array).
//!
//! At the heart of Vortex are [arrays](Array) and [encodings](crate::encoding::ArrayEncoding).
//! Arrays are typed views of memory buffers that hold [scalars](vortex_scalar::Scalar). These
//! buffers can be held in a number of physical encodings to perform lightweight compression that
//! exploits the particular data distribution of the array's values.
//!
//! Every data type recognized by Vortex also has a canonical physical encoding format, which
//! arrays can be [canonicalized](Canonical) into for ease of access in compute functions.
//!
use std::fmt::{Debug, Display, Formatter};
use std::future::ready;

pub use canonical::*;
pub use context::*;
pub use data::*;
pub use implementation::*;
use itertools::Itertools;
pub use metadata::*;
pub use ::paste;
pub use typed::*;
pub use view::*;
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_panic, VortexExpect, VortexResult};

use crate::compute::ArrayCompute;
use crate::encoding::{ArrayEncodingRef, EncodingId, EncodingRef};
use crate::iter::{ArrayIterator, ArrayIteratorAdapter};
use crate::stats::{ArrayStatistics, ArrayStatisticsCompute};
use crate::stream::{ArrayStream, ArrayStreamAdapter};
use crate::validity::ArrayValidity;
use crate::variants::ArrayVariants;
use crate::visitor::{AcceptArrayVisitor, ArrayVisitor};

pub mod accessor;
pub mod array;
pub mod arrow;
mod canonical;
pub mod compress;
pub mod compute;
mod context;
mod data;
pub mod elementwise;
pub mod encoding;
mod implementation;
pub mod iter;
mod metadata;
pub mod stats;
pub mod stream;
mod tree;
mod typed;
pub mod validity;
pub mod variants;
pub mod vendored;
mod view;
pub mod visitor;

pub mod flatbuffers {
    pub use vortex_flatbuffers::array::*;
}

#[derive(Debug, Clone)]
pub enum Array {
    Data(ArrayData),
    View(ArrayView),
}

impl Array {
    pub fn encoding(&self) -> EncodingRef {
        match self {
            Self::Data(d) => d.encoding(),
            Self::View(v) => v.encoding(),
        }
    }

    #[allow(clippy::same_name_method)]
    pub fn len(&self) -> usize {
        match self {
            Self::Data(d) => d.len(),
            Self::View(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Data(d) => d.is_empty(),
            Self::View(v) => v.is_empty(),
        }
    }

    pub fn nbytes(&self) -> usize {
        self.with_dyn(|a| a.nbytes())
    }

    pub fn child<'a>(&'a self, idx: usize, dtype: &'a DType, len: usize) -> Option<Self> {
        match self {
            Self::Data(d) => d.child(idx, dtype, len).cloned(),
            Self::View(v) => v.child(idx, dtype, len).map(Array::View),
        }
    }

    pub fn children(&self) -> Vec<Array> {
        match self {
            Array::Data(d) => d.children().iter().cloned().collect_vec(),
            Array::View(v) => v.children(),
        }
    }

    pub fn nchildren(&self) -> usize {
        match self {
            Self::Data(d) => d.nchildren(),
            Self::View(v) => v.nchildren(),
        }
    }

    pub fn depth_first_traversal(&self) -> ArrayChildrenIterator {
        ArrayChildrenIterator::new(self.clone())
    }

    /// Return the buffer offsets and the total length of all buffers, assuming the given alignment.
    /// This includes all child buffers.
    pub fn all_buffer_offsets(&self, alignment: usize) -> Vec<u64> {
        let mut offsets = vec![];
        let mut offset = 0;

        for col_data in self.depth_first_traversal() {
            if let Some(buffer) = col_data.buffer() {
                offsets.push(offset as u64);

                let buffer_size = buffer.len();
                let aligned_size = (buffer_size + (alignment - 1)) & !(alignment - 1);
                offset += aligned_size;
            }
        }
        offsets.push(offset as u64);

        offsets
    }

    pub fn buffer(&self) -> Option<&Buffer> {
        match self {
            Self::Data(d) => d.buffer(),
            Self::View(v) => v.buffer(),
        }
    }

    pub fn into_buffer(self) -> Option<Buffer> {
        match self {
            Self::Data(d) => d.into_buffer(),
            Self::View(v) => v.buffer().cloned(),
        }
    }

    pub fn into_array_iterator(self) -> impl ArrayIterator {
        ArrayIteratorAdapter::new(self.dtype().clone(), std::iter::once(Ok(self)))
    }

    pub fn into_array_stream(self) -> impl ArrayStream {
        ArrayStreamAdapter::new(
            self.dtype().clone(),
            futures_util::stream::once(ready(Ok(self))),
        )
    }

    /// Checks whether array is of given encoding
    pub fn is_encoding(&self, id: EncodingId) -> bool {
        self.encoding().id() == id
    }

    #[inline]
    pub fn with_dyn<R, F>(&self, mut f: F) -> R
    where
        F: FnMut(&dyn ArrayTrait) -> R,
    {
        let mut result = None;

        self.encoding()
            .with_dyn(self, &mut |array| {
                // Sanity check that the encoding implements the correct array trait
                debug_assert!(
                    match array.dtype() {
                        DType::Null => array.as_null_array().is_some(),
                        DType::Bool(_) => array.as_bool_array().is_some(),
                        DType::Primitive(..) => array.as_primitive_array().is_some(),
                        DType::Utf8(_) => array.as_utf8_array().is_some(),
                        DType::Binary(_) => array.as_binary_array().is_some(),
                        DType::Struct(..) => array.as_struct_array().is_some(),
                        DType::List(..) => array.as_list_array().is_some(),
                        DType::Extension(..) => array.as_extension_array().is_some(),
                    },
                    "Encoding {} does not implement the variant trait for {}",
                    self.encoding().id(),
                    array.dtype()
                );

                result = Some(f(array));
                Ok(())
            })
            .unwrap_or_else(|err| {
                vortex_panic!(
                    err,
                    "Failed to convert Array to {}",
                    std::any::type_name::<dyn ArrayTrait>()
                )
            });

        // Now we unwrap the optional, which we know to be populated by the closure.
        result.vortex_expect("Failed to get result from Array::with_dyn")
    }
}

/// A depth-first pre-order iterator over a ArrayData.
pub struct ArrayChildrenIterator {
    stack: Vec<Array>,
}

impl ArrayChildrenIterator {
    pub fn new(array: Array) -> Self {
        Self { stack: vec![array] }
    }
}

impl Iterator for ArrayChildrenIterator {
    type Item = Array;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.stack.pop()?;
        for child in next.children().into_iter().rev() {
            self.stack.push(child);
        }
        Some(next)
    }
}

pub trait ToArray {
    fn to_array(&self) -> Array;
}

pub trait IntoArray {
    fn into_array(self) -> Array;
}

pub trait AsArray {
    fn as_array_ref(&self) -> &Array;
}

pub trait ToArrayData {
    fn to_array_data(&self) -> ArrayData;
}

/// Collects together the behaviour of an array.
pub trait ArrayTrait:
    ArrayEncodingRef
    + ArrayCompute
    + ArrayDType
    + ArrayLen
    + ArrayVariants
    + IntoCanonical
    + ArrayValidity
    + AcceptArrayVisitor
    + ArrayStatistics
    + ArrayStatisticsCompute
    + ToArrayData
{
    fn nbytes(&self) -> usize {
        let mut visitor = NBytesVisitor(0);
        self.accept(&mut visitor)
            .vortex_expect("Failed to get nbytes from Array");
        visitor.0
    }
}

pub trait ArrayDType {
    // TODO(ngates): move into ArrayTrait?
    fn dtype(&self) -> &DType;
}

pub trait ArrayLen {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool;
}

struct NBytesVisitor(usize);

impl ArrayVisitor for NBytesVisitor {
    fn visit_child(&mut self, _name: &str, array: &Array) -> VortexResult<()> {
        self.0 += array.with_dyn(|a| a.nbytes());
        Ok(())
    }

    fn visit_buffer(&mut self, buffer: &Buffer) -> VortexResult<()> {
        self.0 += buffer.len();
        Ok(())
    }
}

impl Display for Array {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let prefix = match self {
            Self::Data(_) => "",
            Self::View(_) => "$",
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

impl ToArrayData for Array {
    fn to_array_data(&self) -> ArrayData {
        match self {
            Self::Data(d) => d.clone(),
            Self::View(_) => self.with_dyn(|a| a.to_array_data()),
        }
    }
}
