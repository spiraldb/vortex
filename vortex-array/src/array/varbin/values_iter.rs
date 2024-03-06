use arrow::array::AsArray;
use arrow::datatypes::UInt8Type;

use crate::array::Array;
use crate::arrow::CombineChunks;
use crate::compute::scalar_at::usize_at;

#[derive(Debug)]
pub struct VarBinPrimitiveIter<'a> {
    bytes: &'a [u8],
    offsets: &'a dyn Array,
    last_offset: usize,
    idx: usize,
}

impl<'a> VarBinPrimitiveIter<'a> {
    pub fn new(bytes: &'a [u8], offsets: &'a dyn Array) -> Self {
        assert!(offsets.len() > 1);
        let last_offset = usize_at(offsets, 0).unwrap();
        Self {
            bytes,
            offsets,
            last_offset,
            idx: 1,
        }
    }
}

impl<'a> Iterator for VarBinPrimitiveIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.offsets.len() {
            return None;
        }

        let next_offset: usize = usize_at(self.offsets, self.idx).unwrap();
        let slice_bytes = &self.bytes[self.last_offset..next_offset];
        self.last_offset = next_offset;
        self.idx += 1;
        Some(slice_bytes)
    }
}

#[derive(Debug)]
pub struct VarBinIter<'a> {
    bytes: &'a dyn Array,
    offsets: &'a dyn Array,
    last_offset: usize,
    idx: usize,
}

impl<'a> VarBinIter<'a> {
    pub fn new(bytes: &'a dyn Array, offsets: &'a dyn Array) -> Self {
        assert!(offsets.len() > 1);
        let last_offset = usize_at(offsets, 0).unwrap();
        Self {
            bytes,
            offsets,
            last_offset,
            idx: 1,
        }
    }
}

impl<'a> Iterator for VarBinIter<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx == self.offsets.len() {
            return None;
        }

        let next_offset: usize = usize_at(self.offsets, self.idx).unwrap();
        let slice_bytes = self.bytes.slice(self.last_offset, next_offset).unwrap();
        self.last_offset = next_offset;
        self.idx += 1;
        // TODO(robert): iter as primitive vs arrow
        Some(
            slice_bytes
                .iter_arrow()
                .combine_chunks()
                .as_primitive::<UInt8Type>()
                .values()
                .to_vec(),
        )
    }
}
