use std::any::Any;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

use vortex_error::{vortex_bail, VortexResult};
use vortex_schema::{DType, Nullability};

use crate::array::bool::{BoolArray, BoolEncoding};
use crate::array::chunked::{ChunkedArray, ChunkedEncoding};
use crate::array::composite::{CompositeArray, CompositeEncoding};
use crate::array::constant::{ConstantArray, ConstantEncoding};
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::primitive::{PrimitiveArray, PrimitiveEncoding};
use crate::array::sparse::{SparseArray, SparseEncoding};
use crate::array::struct_::{StructArray, StructEncoding};
use crate::array::varbin::{VarBinArray, VarBinEncoding};
use crate::array::varbinview::{VarBinViewArray, VarBinViewEncoding};
use crate::compute::ArrayCompute;
use crate::formatter::{ArrayDisplay, ArrayFormatter};
use crate::serde::ArraySerde;
use crate::stats::Stats;
use crate::validity::Validity;

pub mod bool;
pub mod chunked;
pub mod composite;
pub mod constant;
pub mod downcast;
pub mod primitive;
pub mod sparse;
pub mod struct_;
pub mod varbin;
pub mod varbinview;

pub type ArrayRef = Arc<dyn OwnedArray>;

/// A Vortex Array is the base object representing all arrays in enc.
///
/// Arrays have a dtype and an encoding. DTypes represent the logical type of the
/// values stored in a vortex array. Encodings represent the physical layout of the
/// array.
///
/// This differs from Apache Arrow where logical and physical are combined in
/// the data type, e.g. LargeString, RunEndEncoded.
pub trait Array: ArrayValidity + ArrayDisplay + Debug {
    fn to_array(&self) -> ArrayRef;

    /// Get the length of the array
    fn len(&self) -> usize;
    /// Check whether the array is empty
    fn is_empty(&self) -> bool;
    /// Get the dtype of the array
    fn dtype(&self) -> &DType;
    /// Get the nullability of the array
    fn nullability(&self) -> Nullability {
        self.dtype().nullability()
    }

    /// Get statistics for the array
    /// TODO(ngates): this is interesting. What type do we return from this?
    /// Maybe we actually need to model stats more like compute?
    fn stats(&self) -> Stats;

    /// Limit array to start..stop range
    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef>;
    /// Encoding kind of the array
    fn encoding(&self) -> EncodingRef;
    /// Approximate size in bytes of the array. Only takes into account variable size portion of the array

    fn nbytes(&self) -> usize;

    fn with_compute_mut(
        &self,
        _f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()>;

    fn serde(&self) -> Option<&dyn ArraySerde> {
        None
    }

    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()>;
}

// TODO(ngates): OwnedArray -> Array, Array -> ArrayView.
pub trait OwnedArray: Array + Send + Sync {
    /// Converts itself to a reference of [`Any`], which enables downcasting to concrete types.
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn into_array(self) -> ArrayRef;
}

impl<'v, T: Array> ToOwnedView<'v> for &'v T {
    type Owned = ArrayRef;

    fn to_owned_view(&self) -> ArrayRef {
        self.clone().into_array()
    }
}

impl<'v, T: Array> AsView<'v, &'v T> for ArrayRef {
    fn as_view(&'v self) -> &'v T {
        self.as_ref()
    }
}

// impl<'v> AsView<'v, &'v dyn Array> for dyn OwnedArray + '_ {
//     fn as_view(&self) -> &dyn Array {
//         self
//     }
// }

pub trait WithArrayCompute {
    fn with_compute<R, F: Fn(&dyn ArrayCompute) -> VortexResult<R>>(&self, f: F)
        -> VortexResult<R>;
}

impl WithArrayCompute for dyn Array + '_ {
    fn with_compute<R, F: Fn(&dyn ArrayCompute) -> VortexResult<R>>(
        &self,
        f: F,
    ) -> VortexResult<R> {
        let mut result: Option<R> = None;
        self.with_compute_mut(&mut |compute| {
            result = Some(f(compute)?);
            Ok(())
        })?;
        Ok(result.unwrap())
    }
}

pub trait IntoArray {
    fn into_array(self) -> ArrayRef;
}

#[macro_export]
macro_rules! impl_array {
    () => {
        #[inline]
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        #[inline]
        fn into_any(self: std::sync::Arc<Self>) -> std::sync::Arc<dyn std::any::Any + Send + Sync> {
            self
        }

        #[inline]
        fn into_array(self) -> ArrayRef {
            std::sync::Arc::new(self)
        }
    };
}

pub use impl_array;

use crate::encoding::EncodingRef;
use crate::validity::ArrayValidity;
use crate::view::{AsView, ToOwnedView};
use crate::ArrayWalker;

impl Array for ArrayRef {
    fn to_array(&self) -> ArrayRef {
        self.as_ref().to_array()
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    fn dtype(&self) -> &DType {
        self.as_ref().dtype()
    }

    fn stats(&self) -> Stats {
        self.as_ref().stats()
    }

    fn slice(&self, start: usize, stop: usize) -> VortexResult<ArrayRef> {
        self.as_ref().slice(start, stop)
    }

    fn encoding(&self) -> EncodingRef {
        self.as_ref().encoding()
    }

    fn nbytes(&self) -> usize {
        self.as_ref().nbytes()
    }

    fn with_compute_mut(
        &self,
        f: &mut dyn FnMut(&dyn ArrayCompute) -> VortexResult<()>,
    ) -> VortexResult<()> {
        self.as_ref().with_compute_mut(f)
    }

    fn serde(&self) -> Option<&dyn ArraySerde> {
        self.as_ref().serde()
    }

    #[allow(unused_variables)]
    fn walk(&self, walker: &mut dyn ArrayWalker) -> VortexResult<()> {
        self.as_ref().walk(walker)
    }
}

impl ArrayValidity for ArrayRef {
    fn logical_validity(&self) -> Validity {
        self.as_ref().logical_validity()
    }

    fn is_valid(&self, index: usize) -> bool {
        self.as_ref().is_valid(index)
    }
}

impl ArrayDisplay for ArrayRef {
    fn fmt(&self, fmt: &'_ mut ArrayFormatter) -> std::fmt::Result {
        ArrayDisplay::fmt(self.as_ref(), fmt)
    }
}

pub fn check_slice_bounds(array: &dyn Array, start: usize, stop: usize) -> VortexResult<()> {
    if start > array.len() {
        vortex_bail!(OutOfBounds: start, 0, array.len());
    }
    if stop > array.len() {
        vortex_bail!(OutOfBounds: stop, 0, array.len());
    }
    Ok(())
}

pub fn check_validity_buffer(validity: Option<&ArrayRef>, expected_len: usize) -> VortexResult<()> {
    if let Some(v) = validity {
        if !matches!(v.dtype(), DType::Bool(Nullability::NonNullable)) {
            vortex_bail!(MismatchedTypes: DType::Bool(Nullability::NonNullable), v.dtype());
        }
        if v.len() != expected_len {
            vortex_bail!(
                "Validity buffer {} has incorrect length {}, expected {}",
                v,
                v.len(),
                expected_len
            );
        }
    }

    Ok(())
}

#[derive(Debug, Clone)]
pub enum ArrayKind<'a> {
    Bool(&'a BoolArray),
    Chunked(&'a ChunkedArray),
    Composite(&'a CompositeArray),
    Constant(&'a ConstantArray),
    Primitive(&'a PrimitiveArray),
    Sparse(&'a SparseArray),
    Struct(&'a StructArray),
    VarBin(&'a VarBinArray),
    VarBinView(&'a VarBinViewArray),
    Other(&'a dyn Array),
}

impl<'a> From<&'a dyn Array> for ArrayKind<'a> {
    fn from(value: &'a dyn Array) -> Self {
        match value.encoding().id() {
            BoolEncoding::ID => ArrayKind::Bool(value.as_bool()),
            ChunkedEncoding::ID => ArrayKind::Chunked(value.as_chunked()),
            CompositeEncoding::ID => ArrayKind::Composite(value.as_composite()),
            ConstantEncoding::ID => ArrayKind::Constant(value.as_constant()),
            PrimitiveEncoding::ID => ArrayKind::Primitive(value.as_primitive()),
            SparseEncoding::ID => ArrayKind::Sparse(value.as_sparse()),
            StructEncoding::ID => ArrayKind::Struct(value.as_struct()),
            VarBinEncoding::ID => ArrayKind::VarBin(value.as_varbin()),
            VarBinViewEncoding::ID => ArrayKind::VarBinView(value.as_varbinview()),
            _ => ArrayKind::Other(value),
        }
    }
}

impl Display for dyn Array + '_ {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}, len={})",
            self.encoding().id(),
            self.dtype(),
            self.len()
        )
    }
}
