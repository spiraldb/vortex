use crate::accessor::ArrayAccessor;
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::varbinview::VarBinViewArray;
use crate::validity::ArrayValidity;

impl<'a> ArrayAccessor<'a, &'a [u8]> for VarBinViewArray {
    fn value(&'a self, index: usize) -> Option<&'a [u8]> {
        if self.is_valid(index) {
            let view = &self.view_slice()[index];
            if view.is_inlined() {
                Some(unsafe { &view.inlined.data })
            } else {
                let offset = unsafe { view._ref.offset as usize };
                let buffer_idx = unsafe { view._ref.buffer_index as usize };
                Some(&self.data()[buffer_idx].as_primitive().buffer()[offset..offset + view.size()])
            }
        } else {
            None
        }
    }
}

impl<'a> ArrayAccessor<'a, Vec<u8>> for VarBinViewArray {
    fn value(&'a self, index: usize) -> Option<Vec<u8>> {
        if self.is_valid(index) {
            Some(self.bytes_at(index).unwrap())
        } else {
            None
        }
    }
}
