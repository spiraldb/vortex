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
        if matches!(self.dtype(), DType::Null) {
            Some(self)
        } else {
            None
        }
    }

    fn as_bool_array(&self) -> Option<&dyn BoolArrayTrait> {
        if matches!(self.dtype(), DType::Bool(_)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        if matches!(self.dtype(), DType::Primitive(..)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        if matches!(self.dtype(), DType::Utf8(_)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        if matches!(self.dtype(), DType::Binary(_)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_struct_array(&self) -> Option<&dyn StructArrayTrait> {
        if matches!(self.dtype(), DType::Struct(..)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_list_array(&self) -> Option<&dyn ListArrayTrait> {
        if matches!(self.dtype(), DType::List(..)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_extension_array(&self) -> Option<&dyn ExtensionArrayTrait> {
        if matches!(self.dtype(), DType::Extension(..)) {
            Some(self)
        } else {
            None
        }
    }
}

impl NullArrayTrait for ChunkedArray {}

impl BoolArrayTrait for ChunkedArray {
    fn indices_iter(&self) -> Box<dyn Iterator<Item = usize>> {
        todo!()
    }

    fn slices_iter(&self) -> Box<dyn Iterator<Item = (usize, usize)>> {
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
