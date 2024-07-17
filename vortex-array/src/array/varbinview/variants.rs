use crate::array::varbinview::VarBinViewArray;
use crate::variants::{ArrayVariants, BinaryArrayTrait, Utf8ArrayTrait};

impl ArrayVariants for VarBinViewArray {
    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        Some(self)
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        Some(self)
    }
}

impl Utf8ArrayTrait for VarBinViewArray {}

impl BinaryArrayTrait for VarBinViewArray {}
