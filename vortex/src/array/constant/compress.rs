use crate::array::constant::{ConstantArray, ConstantEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use crate::compute::scalar_at::scalar_at;
use crate::stats::Stat;

impl EncodingCompression for ConstantEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.stats().get_or_compute_or(false, &Stat::IsConstant) {
            Some(&(constant_compressor as Compressor))
        } else {
            None
        }
    }
}

fn constant_compressor(
    array: &dyn Array,
    _like: Option<&dyn Array>,
    _ctx: CompressCtx,
) -> ArrayRef {
    ConstantArray::new(scalar_at(array, 0).unwrap(), array.len()).boxed()
}
