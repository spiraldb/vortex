use crate::array::constant::ConstantArray;
use crate::array::primitive::PrimitiveArray;
use crate::array::stats::Stat;
use crate::array::{Array, ArrayEncoding};
use crate::compute::compress::{CompressConfig, CompressCtx, CompressedEncoding, Compressor};

impl CompressedEncoding for ConstantArray {
    fn compressor(array: &Array, config: &CompressConfig) -> Option<&'static Compressor> {
        if !config.is_enabled(&Self::KIND) {
            return None;
        }

        if array.stats().get_or_compute_or(false, &Stat::IsConstant) {
            Some(&(compress as Compressor))
        } else {
            None
        }
    }
}

fn compress(array: &Array, opts: CompressCtx) -> Array {
    match array {
        Array::Primitive(p) => compress_primitive_array(p, opts),
        _ => unimplemented!(),
    }
}

fn compress_primitive_array(array: &PrimitiveArray, _opts: CompressCtx) -> Array {
    Array::Constant(ConstantArray::new(array.scalar_at(0).unwrap(), array.len()))
}
