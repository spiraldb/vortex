use crate::array::constant::{ConstantArray, ConstantEncoding};
use crate::array::{Array, ArrayRef, Encoding};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use crate::stats::Stat;

impl EncodingCompression for ConstantEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if !config.is_enabled(self.id()) {
            return None;
        }

        if array.stats().get_or_compute_or(false, &Stat::IsConstant) {
            Some(&(constant_compressor as Compressor))
        } else {
            None
        }
    }
}

fn constant_compressor(array: &dyn Array, _opts: CompressCtx) -> ArrayRef {
    ConstantArray::new(array.scalar_at(0).unwrap(), array.len()).boxed()
}
