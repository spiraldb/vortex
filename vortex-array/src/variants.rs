use vortex_dtype::{DType, FieldNames};

/// This module defines array traits for each Vortex DType.
///
/// When callers only want to make assumptions about the DType, and not about any specific
/// encoding, they can use these traits to write encoding-agnostic code.
use crate::{Array, ArrayTrait};

pub trait ArrayVariants {
    fn as_null_array(&self) -> Option<&dyn NullArrayTrait> {
        None
    }

    fn as_bool_array(&self) -> Option<&dyn BoolArrayTrait> {
        None
    }

    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        None
    }

    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        None
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        None
    }

    fn as_struct_array(&self) -> Option<&dyn StructArrayTrait> {
        None
    }

    fn as_list_array(&self) -> Option<&dyn ListArrayTrait> {
        None
    }

    fn as_extension_array(&self) -> Option<&dyn ExtensionArrayTrait> {
        None
    }
}

pub trait NullArrayTrait: ArrayTrait {}

pub trait BoolArrayTrait: ArrayTrait {}

pub trait PrimitiveArrayTrait: ArrayTrait {}

pub trait Utf8ArrayTrait: ArrayTrait {}

pub trait BinaryArrayTrait: ArrayTrait {}

pub trait StructArrayTrait: ArrayTrait {
    fn names(&self) -> &FieldNames {
        let DType::Struct(st, _) = self.dtype() else {
            unreachable!()
        };
        st.names()
    }

    fn dtypes(&self) -> &[DType] {
        let DType::Struct(st, _) = self.dtype() else {
            unreachable!()
        };
        st.dtypes()
    }

    fn nfields(&self) -> usize {
        self.names().len()
    }

    fn field(&self, idx: usize) -> Option<Array>;

    fn field_by_name(&self, name: &str) -> Option<Array> {
        let field_idx = self
            .names()
            .iter()
            .position(|field_name| field_name.as_ref() == name);

        field_idx.and_then(|field_idx| self.field(field_idx))
    }
}

pub trait ListArrayTrait: ArrayTrait {}

pub trait ExtensionArrayTrait: ArrayTrait {}
