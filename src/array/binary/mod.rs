use super::{impl_array, Array};
use crate::types::DType;
use arrow2::array::PrimitiveArray as ArrowPrimitiveArray;

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

pub const KIND: &str = "enc.varbinary";

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
}
