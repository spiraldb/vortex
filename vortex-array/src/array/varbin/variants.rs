use crate::array::varbin::VarBinArray;
use crate::variants::{ArrayVariants, BinaryArrayTrait, Utf8ArrayTrait};

impl ArrayVariants for VarBinArray {
    fn as_utf8_array(&self) -> Option<&dyn Utf8ArrayTrait> {
        Some(self)
    }

    fn as_binary_array(&self) -> Option<&dyn BinaryArrayTrait> {
        Some(self)
    }
}

impl Utf8ArrayTrait for VarBinArray {}

impl BinaryArrayTrait for VarBinArray {}
