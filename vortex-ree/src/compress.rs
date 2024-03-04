use codecz::AlignedAllocator;
use vortex::array::downcast::DowncastArrayBuiltin;
use vortex::array::primitive::{PrimitiveArray, PrimitiveEncoding};
use vortex::array::{Array, ArrayRef, CloneOptionalArray};
use vortex::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use vortex::dtype::{DType, IntWidth, Nullability};
use vortex::ptype::match_each_native_ptype;
use vortex::stats::Stat;

use crate::downcast::DowncastREE;
use crate::{REEArray, REEEncoding};

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

        if array.encoding().id() == &PrimitiveEncoding::ID
            && avg_run_length >= config.ree_average_run_threshold
        {
            return Some(&(ree_compressor as Compressor));
        }

        None
    }
}

fn ree_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let ree_like = like.map(|like_arr| like_arr.as_ree());
    let primitive_array = array.as_primitive();

    let (ends, values) = ree_encode(primitive_array);
    let compressed_ends = ctx
        .next_level()
        .compress(ends.as_ref(), ree_like.map(|ree| ree.ends()));
    let compressed_values = ctx
        .next_level()
        .compress(values.as_ref(), ree_like.map(|ree| ree.values()));

    REEArray::new(
        compressed_ends,
        compressed_values,
        primitive_array.validity().clone_optional(),
        array.len(),
    )
    .boxed()
}

pub fn ree_encode(array: &PrimitiveArray) -> (PrimitiveArray, PrimitiveArray) {
    match_each_native_ptype!(array.ptype(), |$P| {
        let (values, ends) = codecz::ree::encode(array.buffer().typed_data::<$P>()).unwrap();

        let compressed_values = PrimitiveArray::from_nullable_in::<$P, AlignedAllocator>(values, None);
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
pub fn ree_decode(
    ends: &PrimitiveArray,
    values: &PrimitiveArray,
    validity: Option<ArrayRef>,
) -> PrimitiveArray {
    assert!(matches!(
        ends.dtype(),
        DType::Int(IntWidth::_32, _, Nullability::NonNullable)
    ));
    match_each_native_ptype!(values.ptype(), |$P| {
        let decoded = codecz::ree::decode::<$P>(values.buffer().typed_data::<$P>(), ends.buffer().typed_data::<u32>()).unwrap();
        PrimitiveArray::from_nullable_in::<$P, AlignedAllocator>(decoded, validity)
    })
}

#[cfg(test)]
mod test {
    use arrow::buffer::BooleanBuffer;

    use vortex::array::bool::BoolArray;
    use vortex::array::downcast::DowncastArrayBuiltin;
    use vortex::array::{Array, CloneOptionalArray};

    use crate::compress::ree_decode;
    use crate::REEArray;

    #[test]
    fn encode_nullable() {
        let validity = {
            let mut validity = vec![true; 10];
            validity[2] = false;
            validity[7] = false;
            BoolArray::from(validity)
        };
        let arr = REEArray::new(
            vec![2u32, 5, 10].into(),
            vec![1i32, 2, 3].into(),
            Some(validity.boxed()),
            10,
        );

        let decoded = ree_decode(
            arr.ends().as_primitive(),
            arr.values().as_primitive(),
            arr.validity().clone_optional(),
        );

        assert_eq!(
            decoded.buffer().typed_data::<i32>(),
            vec![1i32, 1, 2, 2, 2, 3, 3, 3, 3, 3].as_slice()
        );
        assert_eq!(
            decoded.validity().unwrap().as_bool().buffer(),
            &BooleanBuffer::from(vec![
                true, true, false, true, true, true, true, false, true, true,
            ])
        );
    }
}
