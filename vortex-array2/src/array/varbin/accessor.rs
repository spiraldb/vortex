use crate::accessor::ArrayAccessor;
use crate::array::primitive::PrimitiveArray;
use crate::array::varbin::VarBinArray;
use crate::validity::ArrayValidity;

impl<'a> ArrayAccessor<'a, &'a [u8]> for VarBinArray<'a> {
    fn value(&'a self, index: usize) -> Option<&'a [u8]> {
        if self.is_valid(index) {
            let start = self.offset_at(index);
            let end = self.offset_at(index + 1);
            Some(
                &PrimitiveArray::try_from(self.bytes())
                    .unwrap()
                    .buffer()
                    .as_slice()[start..end],
            )
        } else {
            None
        }
    }
}

impl<'a> ArrayAccessor<'a, Vec<u8>> for VarBinArray<'a> {
    fn value(&self, index: usize) -> Option<Vec<u8>> {
        if self.is_valid(index) {
            Some(self.bytes_at(index).unwrap())
        } else {
            None
        }
    }
}
