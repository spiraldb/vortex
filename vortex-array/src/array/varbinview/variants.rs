use vortex_dtype::DType;

use crate::array::varbinview::VarBinViewArray;
use crate::variants::{ArrayVariants, BinaryArrayTrait, Utf8ArrayTrait};
use crate::ArrayDType;

impl ArrayVariants for VarBinViewArray {
    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        matches!(self.dtype(), DType::Utf8(..)).then_some(self)
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        matches!(self.dtype(), DType::Binary(..)).then_some(self)
    }
}

impl Utf8ArrayTrait for VarBinViewArray {}

impl BinaryArrayTrait for VarBinViewArray {}
