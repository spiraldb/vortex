use vortex_dtype::DType;

use crate::array::varbinview::VarBinViewArray;
use crate::variants::{ArrayVariants, BinaryArrayTrait, Utf8ArrayTrait};
use crate::ArrayDType;

impl ArrayVariants for VarBinViewArray {
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

impl Utf8ArrayTrait for VarBinViewArray {}

impl BinaryArrayTrait for VarBinViewArray {}
