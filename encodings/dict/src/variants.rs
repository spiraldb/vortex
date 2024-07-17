use vortex::variants::{ArrayVariants, BinaryArrayTrait, PrimitiveArrayTrait, Utf8ArrayTrait};
use vortex::ArrayDType;
use vortex_dtype::DType;

use crate::DictArray;

impl ArrayVariants for DictArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        if matches!(self.dtype(), DType::Primitive(..)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        if matches!(self.dtype(), DType::Utf8(..)) {
            Some(self)
        } else {
            None
        }
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        if matches!(self.dtype(), DType::Binary(..)) {
            Some(self)
        } else {
            None
        }
    }
}

impl PrimitiveArrayTrait for DictArray {}

impl Utf8ArrayTrait for DictArray {}

impl BinaryArrayTrait for DictArray {}
