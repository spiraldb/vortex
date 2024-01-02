use super::array::Array;

mod mutable;

#[derive(Clone)]
enum BinaryView {
    Inlined {
        size: u32,
        data: [u8; 12],
    },
    Ref {
        size: u32,
        prefix: [u8; 4],
        buffer_index: u32,
        offset: u32,
    },
}

#[derive(Clone)]
pub struct BinaryArray {
    views: arrow2::array::PrimitiveArray<u8>,
    data: Vec<Box<dyn Array>>,
}
