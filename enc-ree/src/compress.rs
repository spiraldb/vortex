use codecz::AlignedAllocator;

use crate::{REEArray, REEEncoding};
use enc::array::bool::BoolArray;
use enc::array::primitive::PrimitiveArray;
use enc::array::{Array, ArrayKind, ArrayRef};
use enc::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use enc::dtype::{DType, IntWidth, Nullability, Signedness};
use enc::ptype::match_each_native_ptype;
use enc::stats::Stat;

impl EncodingCompression for REEEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        let avg_run_length = array.len() as f32
            / array
                .stats()
                .get_or_compute_or::<usize>(array.len(), &Stat::RunCount) as f32;

        if array.as_any().downcast_ref::<PrimitiveArray>().is_some()
            && avg_run_length >= config.ree_average_run_threshold
        {
            return Some(&(ree_compressor as Compressor));
        }

        None
    }
}

fn ree_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let ree_like = like.map(|like_arr| like_arr.as_any().downcast_ref::<REEArray>().unwrap());
    let (compressed_ends, compressed_values) = match ArrayKind::from(array) {
        ArrayKind::Primitive(primitive_array) => {
            let (ends, values) = ree_encode(primitive_array);
            (
                ctx.next_level()
                    .compress(ends.as_ref(), ree_like.map(|ree| ree.ends())),
                ctx.next_level()
                    .compress(values.as_ref(), ree_like.map(|ree| ree.values())),
            )
        }
        _ => unreachable!("This array kind should have been filtered out"),
    };

    REEArray::new(compressed_ends, compressed_values, array.len()).boxed()
}

pub fn ree_encode(array: &PrimitiveArray) -> (PrimitiveArray, PrimitiveArray) {
    match_each_native_ptype!(array.ptype(), |$P| {
        let (values, ends) = codecz::ree::encode(array.buffer().typed_data::<$P>()).unwrap();
        let validity = array.validity().map(|_| {
            BoolArray::from(
                ends.iter()
                    .map(|end| array.is_valid((*end as usize) - 1))
                    .collect::<Vec<bool>>(),
            ).boxed()
        });

        let compressed_values = PrimitiveArray::from_nullable_in::<$P, AlignedAllocator>(values, validity);
        compressed_values.stats().set(Stat::IsConstant, false.into());
        compressed_values.stats().set(Stat::RunCount, compressed_values.len().into());
        compressed_values.stats().set_many(&array.stats(), vec![
            &Stat::Min, &Stat::Max, &Stat::IsSorted, &Stat::IsStrictSorted,
        ]);

        let compressed_ends = PrimitiveArray::from_vec_in::<u32, AlignedAllocator>(ends);
        compressed_ends.stats().set(Stat::IsSorted, true.into());
        compressed_ends.stats().set(Stat::IsStrictSorted, true.into());
        compressed_ends.stats().set(Stat::IsConstant, false.into());
        compressed_ends.stats().set(Stat::Max, array.len().into());
        compressed_ends.stats().set(Stat::RunCount, compressed_ends.len().into());

        (compressed_ends, compressed_values)
    })
}

#[allow(dead_code)]
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

#[cfg(test)]
mod test {
    use crate::compress::ree_encode;
    use arrow::array::AsArray;
    use arrow::datatypes::Int32Type;
    use enc::array::primitive::PrimitiveArray;
    use enc::array::Array;
    use enc::arrow::CombineChunks;
    use itertools::Itertools;

    #[test]
    fn encode_nullable() {
        let arr = PrimitiveArray::from_iter(vec![
            Some(1),
            Some(1),
            Some(1),
            Some(3),
            Some(3),
            None,
            None,
            Some(4),
            Some(4),
            None,
            None,
        ]);
        let (_ends, values) = ree_encode(&arr);
        values
            .iter_arrow()
            .combine_chunks()
            .as_primitive::<Int32Type>()
            .into_iter()
            .zip_eq([Some(1), Some(3), None, Some(4), None])
            .for_each(|(arrow_scalar, test_scalar)| assert_eq!(arrow_scalar, test_scalar));
    }
}
