use half::f16;

use codecz::AlignedAllocator;

use crate::array::primitive::PrimitiveArray;
use crate::array::ree::REEArray;
use crate::array::ree::REEEncoding;
use crate::array::{Array, ArrayKind, ArrayRef, Encoding};
use crate::compress::{
    compress_typed, CompressConfig, CompressCtx, CompressedEncoding, Compressor,
};
use crate::ptype::{match_each_native_ptype, PType};
use crate::stats::Stat;

impl CompressedEncoding for REEEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if !config.is_enabled(self.id()) {
            return None;
        }

        if array.as_any().downcast_ref::<PrimitiveArray>().is_some()
            && array.len() as f32
                / array
                    .stats()
                    .get_or_compute_or::<usize>(array.len(), &Stat::RunCount)
                    as f32
                >= config.ree_average_run_threshold
        {
            return Some(&(ree_compress as Compressor));
        }

        None
    }
}

fn ree_compress(array: &dyn Array, opts: CompressCtx) -> ArrayRef {
    match ArrayKind::from(array) {
        ArrayKind::Primitive(p) => ree_compress_primitive_array(p, opts),
        _ => panic!("Compress more arrays"),
    }
}

fn ree_compress_primitive_array(array: &PrimitiveArray, ctx: CompressCtx) -> ArrayRef {
    match_each_native_ptype!(array.ptype(), |$P| {
        let (values, runs) = codecz::ree::encode(array.buffer().typed_data::<$P>()).unwrap();
        let compressed_values = compress_typed(PrimitiveArray::from_vec_in::<$P, AlignedAllocator>(values), ctx.next_level());
        let compressed_ends = compress_typed(PrimitiveArray::from_vec_in::<u32, AlignedAllocator>(runs), ctx.next_level());
        REEArray::new(compressed_ends, compressed_values).boxed()
    })
}
