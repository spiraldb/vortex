use arrow2::array::{Array as ArrowArray, PrimitiveArray as ArrowPrimitiveArray};
use arrow2::scalar::{Scalar, Utf8Scalar};
use std::fmt::Binary;

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

    pub fn to_le_bytes(view: BinaryView) -> [u8; 16] {
        let mut bytes: [u8; 16] = [0; 16];
        unsafe {
            match view {
                BinaryView { inlined } => {
                    bytes[0..4].copy_from_slice(&inlined.size.to_le_bytes());
                    bytes[4..16].copy_from_slice(&inlined.data);
                }
                BinaryView { _ref } => {
                    bytes[0..4].copy_from_slice(&_ref.size.to_le_bytes());
                    bytes[4..8].copy_from_slice(&_ref.prefix);
                    bytes[8..12].copy_from_slice(&_ref.buffer_index.to_le_bytes());
                    bytes[12..16].copy_from_slice(&_ref.offset.to_le_bytes());
                }
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
            match view {
                BinaryView { inlined } => Box::new(Utf8Scalar::<i32>::new(Some(
                    String::from_utf8_unchecked(inlined.data.to_vec()),
                ))),
                // BinaryView { _ref } => {
                //     let data_buffer = self.data.get(_ref.buffer_index as usize).unwrap();
                //     data_buffer.as_ref()
                // },
            }
        }
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!()
    }
}
