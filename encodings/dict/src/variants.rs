use vortex::variants::{ArrayVariants, BinaryArrayTrait, PrimitiveArrayTrait, Utf8ArrayTrait};

use crate::DictArray;

impl ArrayVariants for DictArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        Some(self)
    }

    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        Some(self)
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        Some(self)
    }
}

impl PrimitiveArrayTrait for DictArray {}

impl Utf8ArrayTrait for DictArray {}

impl BinaryArrayTrait for DictArray {}
