use super::Array;
use crate::types::DType;

mod mutable;

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

#[derive(Clone)]
// TODO(robert): Abstract over Utf8/Binary
pub struct BinaryArray {
    views: arrow2::array::PrimitiveArray<u8>,
    data: Vec<Box<dyn Array>>,
}

impl Array for BinaryArray {
    fn len(&self) -> usize {
        self.views.len() / std::mem::size_of::<BinaryView>()
    }

    fn datatype(&self) -> DType {
        DType::Utf8
    }
}
