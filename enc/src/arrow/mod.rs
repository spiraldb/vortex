use crate::array::ArrowIterator;
use arrow::array::ArrayRef;
use itertools::Itertools;

pub mod aligned_iter;
pub mod compute;

pub trait CombineChunks {
    fn combine_chunks(self) -> ArrayRef;
}

impl CombineChunks for Box<ArrowIterator> {
    fn combine_chunks(self) -> ArrayRef {
        let chunks = self.collect_vec();
        let chunk_refs = chunks.iter().map(|a| a.as_ref()).collect_vec();
        arrow::compute::concat(&chunk_refs).unwrap()
    }
}
