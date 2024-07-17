use vortex_dtype::DType;
use vortex_scalar::StructScalar;

use crate::array::sparse::SparseArray;
use crate::variants::{
    ArrayVariants, BinaryArrayTrait, BoolArrayTrait, ExtensionArrayTrait, ListArrayTrait,
    NullArrayTrait, PrimitiveArrayTrait, StructArrayTrait, Utf8ArrayTrait,
};
use crate::{Array, ArrayDType, IntoArray};

/// Sparse arrays support all DTypes
impl ArrayVariants for SparseArray {
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

impl NullArrayTrait for SparseArray {}

impl BoolArrayTrait for SparseArray {}

impl PrimitiveArrayTrait for SparseArray {}

impl Utf8ArrayTrait for SparseArray {}

impl BinaryArrayTrait for SparseArray {}

impl StructArrayTrait for SparseArray {
    fn field(&self, idx: usize) -> Option<Array> {
        let values = self
            .values()
            .with_dyn(|s| s.as_struct_array().and_then(|s| s.field(idx)))?;
        let scalar = StructScalar::try_from(self.fill_value())
            .ok()?
            .field_by_idx(idx)?;

        Some(
            SparseArray::try_new_with_offset(
                self.indices().clone(),
                values,
                self.len(),
                self.indices_offset(),
                scalar,
            )
            .unwrap()
            .into_array(),
        )
    }
}

impl ListArrayTrait for SparseArray {}

impl ExtensionArrayTrait for SparseArray {}
