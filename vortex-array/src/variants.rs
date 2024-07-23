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

    fn as_null_array_unchecked(&self) -> &dyn NullArrayTrait {
        self.as_null_array().expect("Expected NullArray")
    }

    fn as_bool_array(&self) -> Option<&dyn BoolArrayTrait> {
        None
    }

    fn as_bool_array_unchecked(&self) -> &dyn BoolArrayTrait {
        self.as_bool_array().expect("Expected BoolArray")
    }

    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        None
    }

    fn as_primitive_array_unchecked(&self) -> &dyn PrimitiveArrayTrait {
        self.as_primitive_array().expect("Expected PrimitiveArray")
    }

    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        None
    }

    fn as_utf8_array_unchecked(&self) -> &dyn Utf8ArrayTrait {
        self.as_utf8_array().expect("Expected Utf8Array")
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        None
    }

    fn as_binary_array_unchecked(&self) -> &dyn BinaryArrayTrait {
        self.as_binary_array().expect("Expected BinaryArray")
    }

    fn as_struct_array(&self) -> Option<&dyn StructArrayTrait> {
        None
    }

    fn as_struct_array_unchecked(&self) -> &dyn StructArrayTrait {
        self.as_struct_array().expect("Expected StructArray")
    }

    fn as_list_array(&self) -> Option<&dyn ListArrayTrait> {
        None
    }

    fn as_list_array_unchecked(&self) -> &dyn ListArrayTrait {
        self.as_list_array().expect("Expected ListArray")
    }

    fn as_extension_array(&self) -> Option<&dyn ExtensionArrayTrait> {
        None
    }

    fn as_extension_array_unchecked(&self) -> &dyn ExtensionArrayTrait {
        self.as_extension_array().expect("Expected ExtensionArray")
    }
}

pub trait NullArrayTrait: ArrayTrait {}

pub trait BoolArrayTrait: ArrayTrait {
    // An iterator over the sorted indices of set values in the underlying boolean array
    // good to array with low number of set values.
    fn maybe_null_indices_iter<'a>(&'a self) -> Box<dyn Iterator<Item = usize> + 'a>;

    // An iterator over the sorted disjoint contiguous range set values in the underlying boolean
    // array good for arrays with only long runs of set values.
    fn maybe_null_slices_iter<'a>(&'a self) -> Box<dyn Iterator<Item = (usize, usize)> + 'a>;

    // Other possible iterators include:
    //  - True(usize) | False(usize) | Mixed(BooleanBuffer) where True/False are long runs of either
    //                                                            true or false values and mixed
    //                                                            is everything else
    //  - T|F + [(usize, BooleanBuffer)] where usize represents an offset into the original array
    //                                         and the buffer is a slice of that array, omitted slices
    //                                         could be either true or false signified by the initial
    //                                         value returned.
}

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
