use vortex_error::VortexResult;

use crate::accessor::ArrayAccessor;
use crate::array::primitive::PrimitiveArray;
use crate::array::varbinview::VarBinViewArray;
use crate::validity::ArrayValidity;

impl ArrayAccessor<[u8]> for VarBinViewArray {
    fn with_iterator<F: for<'a> FnOnce(&mut dyn Iterator<Item = Option<&'a [u8]>>) -> R, R>(
        &self,
        f: F,
    ) -> VortexResult<R> {
        let views = self.view_slice();
        let bytes: Vec<PrimitiveArray> = (0..self.metadata().n_children)
            .map(|i| self.bytes(i).flatten_primitive())
            .collect::<VortexResult<Vec<_>>>()?;
        let validity = self.logical_validity().to_null_buffer()?;

        match validity {
            None => {
                let mut iter = views.iter().map(|view| {
                    if view.is_inlined() {
                        Some(unsafe { &view.inlined.data as &[u8] })
                    } else {
                        let offset = unsafe { view._ref.offset as usize };
                        let buffer_idx = unsafe { view._ref.buffer_index as usize };
                        Some(&bytes[buffer_idx].typed_data::<u8>()[offset..offset + view.size()])
                    }
                });
                Ok(f(&mut iter))
            }
            Some(validity) => {
                let mut iter = views.iter().zip(validity.iter()).map(|(view, valid)| {
                    if valid {
                        if view.is_inlined() {
                            Some(unsafe { &view.inlined.data as &[u8] })
                        } else {
                            let offset = unsafe { view._ref.offset as usize };
                            let buffer_idx = unsafe { view._ref.buffer_index as usize };
                            Some(
                                &bytes[buffer_idx].typed_data::<u8>()[offset..offset + view.size()],
                            )
                        }
                    } else {
                        None
                    }
                });
                Ok(f(&mut iter))
            }
        }
    }
}
