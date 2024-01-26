use arrow::array::ArrayRef;

use crate::array::bool::BoolArray;
use crate::array::chunked::ChunkedArray;
use crate::array::constant::ConstantArray;
use crate::array::patched::PatchedArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::ree::REEArray;
use crate::array::stats::Stats;
use crate::array::struct_::StructArray;
use crate::array::typed::TypedArray;
use crate::array::varbin::VarBinArray;
use crate::array::varbinview::VarBinViewArray;
use crate::error::{EncError, EncResult};
use crate::scalar::Scalar;
use crate::types::DType;

pub mod bool;
pub mod chunked;
pub mod constant;
pub mod encode;
pub mod patched;
pub mod primitive;
pub mod ree;
pub mod stats;
pub mod struct_;
pub mod typed;
pub mod varbin;
pub mod varbinview;

pub type ArrowIterator = dyn Iterator<Item = ArrayRef>;

/// An Enc Array is the base object representing all arrays in enc.
///
/// Arrays have a dtype and an encoding. DTypes represent the logical type of the
/// values stored in an pyenc array. Encodings represent the physical layout of the
/// array.
///
/// This differs from Apache Arrow where logical and physical are combined in
/// the data type, e.g. LargeString, RunEndEncoded.
pub trait ArrayEncoding {
    const KIND: ArrayKind;

    /// Get the length of the array
    fn len(&self) -> usize;
    /// Check whether the array is empty
    fn is_empty(&self) -> bool;
    /// Get the dtype of the array
    fn dtype(&self) -> &DType;
    /// Get statistics for the array
    fn stats(&self) -> Stats;
    /// Get scalar value at given index
    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>>;
    /// Produce arrow batches from the encoding
    fn iter_arrow(&self) -> Box<ArrowIterator>;
    /// Limit array to start..stop range
    fn slice(&self, start: usize, stop: usize) -> EncResult<Array>;

    /// Encoding kind of the array
    fn kind(&self) -> ArrayKind;

    /// Approximate size in bytes of the array. Only takes into account variable size portion of the array
    fn nbytes(&self) -> usize;

    fn check_slice_bounds(&self, start: usize, stop: usize) -> EncResult<()> {
        if start > self.len() {
            return Err(EncError::OutOfBounds(start, 0, self.len()));
        }
        if stop > self.len() {
            return Err(EncError::OutOfBounds(stop, 0, self.len()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Eq, Ord, PartialOrd, PartialEq, Hash)]
pub enum ArrayKind {
    Bool,
    Chunked,
    Patched,
    Constant,
    Primitive,
    REE,
    Struct,
    Typed,
    VarBin,
    VarBinView,
}

#[derive(Debug, Clone)]
pub enum Array {
    Bool(BoolArray),
    Chunked(ChunkedArray),
    Patched(PatchedArray),
    Constant(ConstantArray),
    Primitive(PrimitiveArray),
    REE(REEArray),
    Struct(StructArray),
    Typed(TypedArray),
    VarBin(VarBinArray),
    VarBinView(VarBinViewArray),
}

macro_rules! impls_for_array {
    ($variant:tt, $E:ty) => {
        impl From<$E> for Array {
            fn from(arr: $E) -> Self {
                Self::$variant(arr)
            }
        }
    };
}

impls_for_array!(Bool, BoolArray);
impls_for_array!(Chunked, ChunkedArray);
impls_for_array!(Patched, PatchedArray);
impls_for_array!(Constant, ConstantArray);
impls_for_array!(Primitive, PrimitiveArray);
impls_for_array!(REE, REEArray);
impls_for_array!(Struct, StructArray);
impls_for_array!(Typed, TypedArray);
impls_for_array!(VarBin, VarBinArray);
impls_for_array!(VarBinView, VarBinViewArray);

macro_rules! match_each_encoding {
    ($self:expr, | $_:tt $enc:ident | $($body:tt)*) => ({
        macro_rules! __with_enc__ {( $_ $enc:ident ) => ( $($body)* )}
        match $self {
            Array::Bool(enc) => __with_enc__! { enc },
            Array::Chunked(enc) => __with_enc__! { enc },
            Array::Patched(enc) => __with_enc__! { enc },
            Array::Constant(enc) => __with_enc__! { enc },
            Array::Primitive(enc) => __with_enc__! { enc },
            Array::REE(enc) => __with_enc__! { enc },
            Array::Struct(enc) => __with_enc__! { enc },
            Array::Typed(enc) => __with_enc__! { enc },
            Array::VarBin(enc) => __with_enc__! { enc },
            Array::VarBinView(enc) => __with_enc__! { enc },
        }
    })
}

impl ArrayEncoding for Array {
    // TODO(robert): This is impossible to implement
    const KIND: ArrayKind = ArrayKind::Chunked;

    fn len(&self) -> usize {
        match_each_encoding! { self, |$enc| $enc.len() }
    }

    fn is_empty(&self) -> bool {
        match_each_encoding! { self, |$enc| $enc.is_empty() }
    }

    fn dtype(&self) -> &DType {
        match_each_encoding! { self, |$enc| $enc.dtype() }
    }

    fn stats(&self) -> Stats {
        match_each_encoding! { self, |$enc| $enc.stats() }
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        match_each_encoding! { self, |$enc| $enc.scalar_at(index) }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        match_each_encoding! { self, |$enc| $enc.iter_arrow() }
    }

    fn slice(&self, start: usize, stop: usize) -> EncResult<Array> {
        match_each_encoding! { self, |$enc| $enc.slice(start, stop) }
    }

    fn kind(&self) -> ArrayKind {
        match_each_encoding! { self, |$enc| $enc.kind() }
    }

    fn nbytes(&self) -> usize {
        match_each_encoding! { self, |$enc| $enc.nbytes() }
    }
}
