use crate::accessor::ArrayAccessor;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::varbin::VarBinArray;
use crate::validity::ArrayValidity;

impl<'a> ArrayAccessor<'a, &'a [u8]> for VarBinArray {
    fn value(&'a self, index: usize) -> Option<&'a [u8]> {
        if self.is_valid(index) {
            let start = self.offset_at(index);
            let end = self.offset_at(index + 1);
            Some(&self.bytes().as_primitive().buffer()[start..end])
        } else {
            None
        }
    }
}

impl ArrayAccessor<'_, Vec<u8>> for VarBinArray {
    fn value(&self, index: usize) -> Option<Vec<u8>> {
        if self.is_valid(index) {
            Some(self.bytes_at(index).unwrap())
        } else {
            None
        }
    }
}
