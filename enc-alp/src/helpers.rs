use arrow::buffer::BooleanBuffer;
use codecz::{AlignedVec, ALIGNED_ALLOCATOR};

use enc::ptype::NativePType;

pub fn into_u32_vec(bb: &BooleanBuffer, cardinality: usize) -> AlignedVec<u32> {
    let mut vec: AlignedVec<u32> = AlignedVec::with_capacity_in(cardinality, ALIGNED_ALLOCATOR);
    for idx in bb.set_indices() {
        vec.push(idx as u32);
    }
    vec
}

pub fn gather_patches<T: NativePType>(data: &[T], indices: &[u32]) -> AlignedVec<T> {
    let mut vec: AlignedVec<T> = AlignedVec::with_capacity_in(indices.len(), ALIGNED_ALLOCATOR);
    for idx in indices {
        vec.push(data[*idx as usize]);
    }
    vec
}
