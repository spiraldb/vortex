use arrow::buffer::{BooleanBuffer, Buffer};

use crate::array::bool::BoolArray;
use crate::array::{Array, ArrayRef};
use crate::compress::{sampled_compression, ArrayCompression, CompressCtx};
use crate::sampling::default_sample;

impl ArrayCompression for BoolArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        sampled_compression(self, ctx, bool_sampler)
    }
}

fn bool_sampler(array: &dyn Array, sample_size: u16, sample_count: u16) -> ArrayRef {
    let bool_array = array.as_any().downcast_ref::<BoolArray>().unwrap();
    let sample_bytes = default_sample(
        // TODO(ngates): we should respect the array offset, at least to the nearest byte.
        bool_array.buffer().values(),
        // Sample size over 8 since each byte holds 8 bools
        sample_size / 8,
        sample_count,
    );
    let sample_len = sample_bytes.len();
    BoolArray::new(
        BooleanBuffer::new(Buffer::from_vec(sample_bytes), 0, sample_len * 8),
        None,
    )
    .boxed()
}
