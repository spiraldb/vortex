use arrow2::array::{PrimitiveArray as ArrowPrimitiveArray, Utf8Array};
use arrow2::datatypes::{PhysicalType, PrimitiveType};
use arrow2::scalar::{Scalar, Utf8Scalar};

use crate::types::DType;

use super::{impl_array, Array, ArrowIterator};

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

pub const KIND: &str = "enc.varbinary";
pub const VIEW_SIZE: usize = std::mem::size_of::<BinaryView>();

#[derive(Clone)]
pub struct VarBinaryArray {
    views: ArrowPrimitiveArray<u8>,
    data: Vec<Box<dyn Array>>,
    dtype: DType,
}

impl VarBinaryArray {
    pub fn new(views: ArrowPrimitiveArray<u8>, data: Vec<Box<dyn Array>>) -> Self {
        Self {
            views,
            data,
            dtype: DType::Binary,
        }
    }

    // TODO(robert): Validate data is utf8
    pub fn new_utf8(views: ArrowPrimitiveArray<u8>, data: Vec<Box<dyn Array>>) -> Self {
        Self {
            views,
            data,
            dtype: DType::Utf8,
        }
    }
}

impl Array for VarBinaryArray {
    impl_array!();

    fn len(&self) -> usize {
        self.views.len() / std::mem::size_of::<BinaryView>()
    }

    #[inline]
    fn dtype(&self) -> &DType {
        &self.dtype
    }

    #[inline]
    fn kind(&self) -> &str {
        KIND
    }

    fn scalar_at(&self, index: usize) -> Box<dyn Scalar> {
        let view_slice: &[u8] =
            &self.views.values().as_slice()[index * VIEW_SIZE..(index + 1) * VIEW_SIZE];
        let view = BinaryView::from_le_bytes(view_slice);
        unsafe {
            if view.inlined.size > 12 {
                let data_buffer = self.data.get(view._ref.buffer_index as usize).unwrap();
                // TODO(robert): Combine arrays if there are many
                let arrow_data_buffer = data_buffer.as_ref().iter_arrow().next().unwrap();

                match arrow_data_buffer.as_ref().data_type().to_physical_type() {
                    PhysicalType::Primitive(PrimitiveType::UInt8) => {
                        let primitive_array = arrow_data_buffer
                            .as_any()
                            .downcast_ref::<ArrowPrimitiveArray<u8>>()
                            .unwrap();

                        Box::new(Utf8Scalar::<i32>::new(Some(String::from_utf8_unchecked(
                            primitive_array.values().as_slice()[view._ref.offset as usize
                                ..(view._ref.offset + view._ref.size) as usize]
                                .to_vec(),
                        ))))
                    }
                    PhysicalType::Utf8 => {
                        let utf8_array = arrow_data_buffer
                            .as_any()
                            .downcast_ref::<Utf8Array<i32>>()
                            .unwrap();
                        arrow2::scalar::new_scalar(utf8_array, view._ref.offset as usize)
                    }
                    _ => panic!("TODO(robert): Implement more"),
                }
            } else {
                Box::new(Utf8Scalar::<i32>::new(Some(String::from_utf8_unchecked(
                    view.inlined.data[..view.inlined.size as usize].to_vec(),
                ))))
            }
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::array::binary::{BinaryView, Inlined, Ref, VarBinaryArray};
    use crate::array::primitive::PrimitiveArray;
    use crate::array::Array;
    use arrow2::scalar::Utf8Scalar;
    use arrow2::*;

    #[test]
    pub fn test_varbin() {
        let values = PrimitiveArray::new(&array::PrimitiveArray::<u8>::from_slice(
            "abcdefabcdefabcdef",
        ));
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
        let binary_arr = VarBinaryArray::new(view_arr, vec![values.boxed()]);
        assert_eq!(binary_arr.len(), 2);
        assert_eq!(
            binary_arr.scalar_at(0),
            Box::new(Utf8Scalar::<i32>::new(Some("abcdefgh"))) as Box<dyn scalar::Scalar>
        );
        assert_eq!(
            binary_arr.scalar_at(1),
            Box::new(Utf8Scalar::<i32>::new(Some("cdefabcdefabc"))) as Box<dyn scalar::Scalar>
        )
    }
}
