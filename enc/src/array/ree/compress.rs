use half::f16;

use codecz::AlignedAllocator;

use crate::array::primitive::PrimitiveArray;
use crate::array::ree::{REEArray, REEEncoding};
use crate::array::{Array, ArrayKind, ArrayRef, Encoding};
use crate::compress::{
    ArrayCompression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};
use crate::dtype::{DType, IntWidth, Nullability, Signedness};
use crate::ptype::{match_each_native_ptype, PType};
use crate::stats::Stat;

impl ArrayCompression for REEArray {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef {
        REEArray::new(ctx.compress(self.ends()), ctx.compress(self.values())).boxed()
    }
}

impl EncodingCompression for REEEncoding {
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
            return Some(&(ree_compressor as Compressor));
        }

        None
    }
}

fn ree_compressor(array: &dyn Array, _opts: CompressCtx) -> ArrayRef {
    let (ends, values) = match ArrayKind::from(array) {
        ArrayKind::Primitive(p) => ree_encode(p),
        _ => panic!("Compress more arrays"),
    };
    REEArray::new(ends.boxed(), values.boxed()).boxed()
}

pub fn ree_encode(array: &PrimitiveArray) -> (PrimitiveArray, PrimitiveArray) {
    match_each_native_ptype!(array.ptype(), |$P| {
        let (values, runs) = codecz::ree::encode(array.buffer().typed_data::<$P>()).unwrap();
        let compressed_values = PrimitiveArray::from_vec_in::<$P, AlignedAllocator>(values);
        let compressed_ends = PrimitiveArray::from_vec_in::<u32, AlignedAllocator>(runs);
        (compressed_ends, compressed_values)
    })
}

pub fn ree_decode(ends: &PrimitiveArray, values: &PrimitiveArray) -> PrimitiveArray {
    assert!(matches!(
        ends.dtype(),
        DType::Int(
            IntWidth::_32,
            Signedness::Unsigned,
            Nullability::NonNullable
        )
    ));
    match_each_native_ptype!(values.ptype(), |$P| {
        let decoded = codecz::ree::decode::<$P>(values.buffer().typed_data::<$P>(), ends.buffer().typed_data::<u32>()).unwrap();
        PrimitiveArray::from_vec_in::<$P, AlignedAllocator>(decoded)
    })
}
