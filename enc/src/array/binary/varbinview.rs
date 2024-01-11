use std::iter;

use arrow2::array::{
    Array as ArrowArray, PrimitiveArray as ArrowPrimitiveArray, Utf8Array as ArrowUtf8Array,
};
use arrow2::datatypes::{PhysicalType, PrimitiveType};

use crate::error::EncResult;
use crate::scalar::{Scalar, Utf8Scalar};
use crate::types::DType;

use super::super::{Array, ArrayEncoding, ArrowIterator};

#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct Inlined {
    size: u32,
    data: [u8; 12],
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
struct Ref {
    size: u32,
    prefix: [u8; 4],
    buffer_index: u32,
    offset: u32,
}

#[derive(Clone, Copy)]
#[repr(C, align(8))]
union BinaryView {
    inlined: Inlined,
    _ref: Ref,
}

impl BinaryView {
    #[inline]
    pub fn from_le_bytes(bytes: &[u8]) -> BinaryView {
        let size = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if size > 12 {
            BinaryView {
                _ref: Ref {
                    size,
                    prefix: bytes[4..8].try_into().unwrap(),
                    buffer_index: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
                    offset: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
                },
            }
        } else {
            BinaryView {
                inlined: Inlined {
                    size,
                    data: bytes[4..16].try_into().unwrap(),
                },
            }
        }
    }

    #[inline]
    #[allow(clippy::wrong_self_convention)]
    #[allow(dead_code)]
    pub fn to_le_bytes(&self) -> [u8; 16] {
        let mut bytes: [u8; 16] = [0; 16];
        unsafe {
            bytes[0..4].copy_from_slice(&self.inlined.size.to_le_bytes());
            if self.inlined.size > 12 {
                bytes[4..8].copy_from_slice(&self._ref.prefix);
                bytes[8..12].copy_from_slice(&self._ref.buffer_index.to_le_bytes());
                bytes[12..16].copy_from_slice(&self._ref.offset.to_le_bytes());
            } else {
                bytes[4..16].copy_from_slice(&self.inlined.data);
            }
        }
        bytes
    }
}

pub const VIEW_SIZE: usize = std::mem::size_of::<BinaryView>();

#[derive(Debug, Clone, PartialEq)]
pub struct VarBinViewArray {
    views: ArrowPrimitiveArray<u8>,
    data: Vec<Array>,
}

impl VarBinViewArray {
    pub fn new(views: ArrowPrimitiveArray<u8>, data: Vec<Array>) -> Self {
        Self { views, data }
    }

    pub fn plain_size(&self) -> usize {
        (0..self.views.len() / VIEW_SIZE).fold(0usize, |acc, i| {
            let view = self.view_at(i);
            unsafe { acc + view.inlined.size as usize }
        })
    }

    #[inline]
    pub(self) fn view_at(&self, index: usize) -> BinaryView {
        let view_slice: &[u8] =
            &self.views.values().as_slice()[index * VIEW_SIZE..(index + 1) * VIEW_SIZE];
        BinaryView::from_le_bytes(view_slice)
    }
}

impl ArrayEncoding for VarBinViewArray {
    fn len(&self) -> usize {
        self.views.len() / std::mem::size_of::<BinaryView>()
    }

    fn is_empty(&self) -> bool {
        self.views.values().is_empty()
    }

    #[inline]
    fn dtype(&self) -> DType {
        DType::Utf8
    }

    fn scalar_at(&self, index: usize) -> EncResult<Box<dyn Scalar>> {
        let view = self.view_at(index);
        unsafe {
            if view.inlined.size > 12 {
                let data_buffer = self.data.get(view._ref.buffer_index as usize).unwrap();
                // TODO(robert): Make sure that we consume whole iter_arrow result,
                //  BUT we are slicing only 16 bytes so this should all be in one chunk
                let arrow_data_buffer = data_buffer
                    .slice(view._ref.offset as usize, view._ref.size as usize)?
                    .iter_arrow()
                    .next()
                    .unwrap();

                match arrow_data_buffer.as_ref().data_type().to_physical_type() {
                    PhysicalType::Primitive(PrimitiveType::UInt8) => {
                        let primitive_array = arrow_data_buffer
                            .as_any()
                            .downcast_ref::<ArrowPrimitiveArray<u8>>()
                            .unwrap();

                        Ok(Utf8Scalar::new(String::from_utf8_unchecked(
                            primitive_array.values().as_slice().to_vec(),
                        ))
                        .boxed())
                    }

                    _ => panic!("TODO(robert): Implement more"),
                }
            } else {
                Ok(Utf8Scalar::new(String::from_utf8_unchecked(
                    view.inlined.data[..view.inlined.size as usize].to_vec(),
                ))
                .boxed())
            }
        }
    }

    // TODO(robert): This could be better if we had compute dispatch but for now it's using scalar_at
    // and wraps values needlessly instead of memcopy
    fn iter_arrow(&self) -> Box<ArrowIterator> {
        let total_size = self.plain_size();
        let mut data_buf = arrow2::array::MutableUtf8ValuesArray::<i64>::with_capacity(total_size);
        for i in 0..self.views.len() / VIEW_SIZE {
            data_buf.push(
                self.scalar_at(i)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Utf8Scalar>()
                    .unwrap()
                    .value(),
            )
        }
        let data_arr: Box<dyn ArrowArray> = Box::<ArrowUtf8Array<i64>>::new(data_buf.into());
        Box::new(iter::once(data_arr))
    }

    fn slice(&self, offset: usize, length: usize) -> EncResult<Array> {
        self.check_slice_bounds(offset, length)?;

        let mut cloned = self.clone();
        unsafe {
            cloned
                .views
                .slice_unchecked(offset * VIEW_SIZE, length * VIEW_SIZE);
        }
        Ok(Array::VarBinView(cloned))
    }
}

#[cfg(test)]
mod test {
    use arrow2::array;

    use crate::array::primitive::PrimitiveArray;

    use super::*;

    fn binary_array() -> VarBinViewArray {
        let values = PrimitiveArray::new(Box::new(array::PrimitiveArray::<u8>::from_slice(
            "abcdefabcdefabcdef",
        )));
        let mut view1 = BinaryView {
            inlined: Inlined {
                size: 8,
                data: [0u8; 12],
            },
        };
        let databytes: [u8; 8] = "abcdefgh".as_bytes().try_into().unwrap();
        unsafe { view1.inlined.data[..databytes.len()].copy_from_slice(&databytes) };
        let view2 = BinaryView {
            _ref: Ref {
                size: 13,
                prefix: "cdef".as_bytes().try_into().unwrap(),
                buffer_index: 0,
                offset: 2,
            },
        };
        let view_arr = array::PrimitiveArray::<u8>::from_slice(
            vec![view1.to_le_bytes(), view2.to_le_bytes()]
                .into_iter()
                .flatten()
                .collect::<Vec<u8>>(),
        );

        VarBinViewArray::new(view_arr, vec![values.into()])
    }

    #[test]
    pub fn varbin_view() {
        let binary_arr = binary_array();
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(
            binary_arr.scalar_at(0).unwrap(),
            Utf8Scalar::new("abcdefgh".into()).boxed()
        );
        assert_eq!(
            binary_arr.scalar_at(1).unwrap(),
            Utf8Scalar::new("cdefabcdefabc".into()).boxed()
        )
    }

    #[test]
    pub fn slice() {
        let binary_arr = binary_array().slice(1, 1).unwrap();
        assert_eq!(
            binary_arr.scalar_at(0).unwrap(),
            Utf8Scalar::new("cdefabcdefabc".into()).boxed()
        );
    }

    #[test]
    pub fn iter() {
        let binary_array = binary_array();
        assert_eq!(
            binary_array
                .iter_arrow()
                .next()
                .unwrap()
                .as_any()
                .downcast_ref::<ArrowUtf8Array<i64>>()
                .unwrap(),
            &ArrowUtf8Array::<i64>::from_trusted_len_values_iter(
                vec!["abcdefgh", "cdefabcdefabc"].into_iter()
            )
        );
    }
}
