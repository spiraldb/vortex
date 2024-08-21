use vortex::iter::ArrayIter;
use vortex::variants::{ArrayVariants, BinaryArrayTrait, PrimitiveArrayTrait, Utf8ArrayTrait};
use vortex::ArrayDType;
use vortex_dtype::DType;

use crate::DictArray;

impl ArrayVariants for DictArray {
    fn as_primitive_array(&self) -> Option<&dyn PrimitiveArrayTrait> {
        matches!(self.dtype(), DType::Primitive(..)).then_some(self)
    }

    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        matches!(self.dtype(), DType::Utf8(..)).then_some(self)
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        matches!(self.dtype(), DType::Binary(..)).then_some(self)
    }
}

impl PrimitiveArrayTrait for DictArray {
    fn float32_iter(&self) -> Option<ArrayIter<f32>> {
        todo!()
    }

    fn float64_iter(&self) -> Option<ArrayIter<f64>> {
        todo!()
    }
}

impl Utf8ArrayTrait for DictArray {}

impl BinaryArrayTrait for DictArray {}
