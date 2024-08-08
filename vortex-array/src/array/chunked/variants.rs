use vortex_dtype::DType;

use crate::array::chunked::ChunkedArray;
use crate::variants::{
    ArrayVariants, BinaryArrayTrait, BoolArrayTrait, ExtensionArrayTrait, ListArrayTrait,
    NullArrayTrait, PrimitiveArrayTrait, StructArrayTrait, Utf8ArrayTrait,
};
use crate::{Array, ArrayDType, IntoArray};

/// Chunked arrays support all DTypes
impl ArrayVariants for ChunkedArray {
    fn as_null_array(&self) -> Option<&dyn NullArrayTrait> {
        matches!(self.dtype(), DType::Null).then_some(self)
    }

    fn as_bool_array(&self) -> Option<&dyn BoolArrayTrait> {
        matches!(self.dtype(), DType::Bool(_)).then_some(self)
    }

    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        matches!(self.dtype(), DType::Primitive(..)).then_some(self)
    }

    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        matches!(self.dtype(), DType::Utf8(_)).then_some(self)
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        matches!(self.dtype(), DType::Binary(_)).then_some(self)
    }

    fn as_struct_array(&self) -> Option<&dyn StructArrayTrait> {
        matches!(self.dtype(), DType::Struct(..)).then_some(self)
    }

    fn as_list_array(&self) -> Option<&dyn ListArrayTrait> {
        matches!(self.dtype(), DType::List(..)).then_some(self)
    }

    fn as_extension_array(&self) -> Option<&dyn ExtensionArrayTrait> {
        matches!(self.dtype(), DType::Extension(..)).then_some(self)
    }
}

impl NullArrayTrait for ChunkedArray {}

impl BoolArrayTrait for ChunkedArray {
    fn maybe_null_indices_iter(&self) -> Box<dyn Iterator<Item = usize>> {
        todo!()
    }

    fn maybe_null_slices_iter(&self) -> Box<dyn Iterator<Item = (usize, usize)>> {
        todo!()
    }
}

impl PrimitiveArrayTrait for ChunkedArray {}

impl Utf8ArrayTrait for ChunkedArray {}

impl BinaryArrayTrait for ChunkedArray {}

impl StructArrayTrait for ChunkedArray {
    fn field(&self, idx: usize) -> Option<Array> {
        let mut chunks = Vec::with_capacity(self.nchunks());
        for chunk in self.chunks() {
            let array = chunk.with_dyn(|a| a.as_struct_array().and_then(|s| s.field(idx)))?;
            chunks.push(array);
        }
        let chunked = ChunkedArray::try_new(chunks, self.dtype().clone())
            .expect("should be correct dtype")
            .into_array();
        Some(chunked)
    }
}

impl ListArrayTrait for ChunkedArray {}

impl ExtensionArrayTrait for ChunkedArray {}
