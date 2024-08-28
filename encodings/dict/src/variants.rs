use vortex::iter::AccessorRef;
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
    fn u8_accessor(&self) -> Option<AccessorRef<u8>> {
        todo!()
    }

    fn u16_accessor(&self) -> Option<AccessorRef<u16>> {
        todo!()
    }

    fn u32_accessor(&self) -> Option<AccessorRef<u32>> {
        todo!()
    }

    fn u64_accessor(&self) -> Option<AccessorRef<u64>> {
        todo!()
    }

    fn i8_accessor(&self) -> Option<AccessorRef<i8>> {
        todo!()
    }

    fn i16_accessor(&self) -> Option<AccessorRef<i16>> {
        todo!()
    }

    fn i32_accessor(&self) -> Option<AccessorRef<i32>> {
        todo!()
    }

    fn i64_accessor(&self) -> Option<AccessorRef<i64>> {
        todo!()
    }

    fn f32_accessor(&self) -> Option<AccessorRef<f32>> {
        todo!()
    }

    fn f64_accessor(&self) -> Option<AccessorRef<f64>> {
        todo!()
    }
}

impl Utf8ArrayTrait for DictArray {}

impl BinaryArrayTrait for DictArray {}
