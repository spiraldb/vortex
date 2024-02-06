use half::f16;

use codecz::AlignedAllocator;

use crate::{REEArray, REEEncoding};
use enc::array::nullable::NullableArray;
use enc::array::primitive::PrimitiveArray;
use enc::array::{Array, ArrayKind, ArrayRef, Encoding};
use enc::compress::{
    ArrayCompression, CompressConfig, CompressCtx, Compressor, EncodingCompression,
};
use enc::dtype::{DType, IntWidth, Nullability, Signedness};
use enc::ptype::{match_each_native_ptype, PType};
use enc::stats::Stat;

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

        let run_count = array.len() as f32
            / array
                .stats()
                .get_or_compute_or::<usize>(array.len(), &Stat::RunCount) as f32;

        if array.as_any().downcast_ref::<PrimitiveArray>().is_some()
            && run_count >= config.ree_average_run_threshold
        {
            return Some(&(ree_compressor as Compressor));
        }

        None
    }
}

fn ree_compressor(array: &dyn Array, ctx: CompressCtx) -> ArrayRef {
    ctx.compress(
        match ArrayKind::from(array) {
            ArrayKind::Primitive(primitive_array) => {
                // FIXME(ngates): ree should respect nulls?
                let (ends, values) = ree_encode(primitive_array);
                let ree = REEArray::new(ends.boxed(), values.boxed()).boxed();
                if let Some(validity) = primitive_array.validity() {
                    NullableArray::new(ree, validity.clone()).boxed()
                } else {
                    ree
                }
            }
            _ => panic!("Compress more arrays"),
        }
        .as_ref(),
    )
}

pub fn ree_encode(array: &PrimitiveArray) -> (PrimitiveArray, PrimitiveArray) {
    match_each_native_ptype!(array.ptype(), |$P| {
        let (values, runs) = codecz::ree::encode(array.buffer().typed_data::<$P>()).unwrap();

        let compressed_values = PrimitiveArray::from_vec_in::<$P, AlignedAllocator>(values);
        compressed_values.stats().set(Stat::IsConstant, false.into());
        compressed_values.stats().set(Stat::RunCount, compressed_values.len().into());
        compressed_values.stats().set_many(&array.stats(), vec![
            &Stat::Min, &Stat::Max, &Stat::IsSorted, &Stat::IsStrictSorted,
        ]);

        let compressed_ends = PrimitiveArray::from_vec_in::<u32, AlignedAllocator>(runs);
        compressed_ends.stats().set(Stat::IsSorted, true.into());
        compressed_ends.stats().set(Stat::IsStrictSorted, true.into());
        compressed_ends.stats().set(Stat::IsConstant, false.into());
        compressed_ends.stats().set(Stat::Max, array.len().into());
        compressed_ends.stats().set(Stat::RunCount, compressed_ends.len().into());

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
