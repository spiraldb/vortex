use crate::array::constant::{ConstantArray, ConstantEncoding};
use crate::array::stats::Stat;
use crate::array::{Array, ArrayRef, Encoding};
use crate::compute::compress::{CompressConfig, CompressCtx, CompressedEncoding, Compressor};

impl CompressedEncoding for ConstantEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if !config.is_enabled(self.id()) {
            return None;
        }

        if array.stats().get_or_compute_or(false, &Stat::IsConstant) {
            Some(&(compress as Compressor))
        } else {
            None
        }
    }
}

fn compress(array: &dyn Array, _opts: CompressCtx) -> ArrayRef {
    ConstantArray::new(array.scalar_at(0).unwrap(), array.len()).boxed()
}
