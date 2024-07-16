use std::cmp::min;

use itertools::Itertools;
use num_traits::{AsPrimitive, FromPrimitive};
use vortex::array::primitive::PrimitiveArray;
use vortex::stats::{ArrayStatistics, Stat};
use vortex::validity::Validity;
use vortex::ArrayDType;
use vortex_dtype::Nullability;
use vortex_dtype::{match_each_integer_ptype, match_each_native_ptype, NativePType};
use vortex_error::VortexResult;

pub fn runend_primitive_encode(array: &PrimitiveArray) -> (PrimitiveArray, PrimitiveArray) {
    let validity = if array.dtype().nullability() == Nullability::NonNullable {
        Validity::NonNullable
    } else {
        Validity::AllValid
    };

    match_each_native_ptype!(array.ptype(), |$P| {
        let (ends, values) = runend_primitive_encode_slice(array.maybe_null_slice::<$P>());

        let mut compressed_values = PrimitiveArray::from_vec(values, validity);
        compressed_values.statistics().set(Stat::IsConstant, false.into());
        compressed_values.statistics().set(Stat::RunCount, compressed_values.len().into());
        array.statistics().get(Stat::Min).map(|s| compressed_values.statistics().set(Stat::Min, s));
        array.statistics().get(Stat::Max).map(|s| compressed_values.statistics().set(Stat::Max, s));
        array.statistics().get(Stat::IsSorted).map(|s| compressed_values.statistics().set(Stat::IsSorted, s));
        array.statistics().get(Stat::IsStrictSorted).map(|s| compressed_values.statistics().set(Stat::IsStrictSorted, s));

        let compressed_ends = PrimitiveArray::from(ends);
        compressed_ends.statistics().set(Stat::IsSorted, true.into());
        compressed_ends.statistics().set(Stat::IsStrictSorted, true.into());
        compressed_ends.statistics().set(Stat::IsConstant, false.into());
        compressed_ends.statistics().set(Stat::Max, array.len().into());
        compressed_ends.statistics().set(Stat::RunCount, compressed_ends.len().into());

        assert_eq!(array.dtype(), compressed_values.dtype());
        (compressed_ends, compressed_values)
    })
}

fn runend_primitive_encode_slice<T: NativePType>(elements: &[T]) -> (Vec<u64>, Vec<T>) {
    let mut ends = Vec::new();
    let mut values = Vec::new();

    if elements.is_empty() {
        return (ends, values);
    }

    // Run-end encode the values
    let mut last = elements[0];
    let mut end = 1;
    for &e in elements.iter().skip(1) {
        if e != last {
            ends.push(end);
            values.push(last);
        }
        last = e;
        end += 1;
    }
    ends.push(end);
    values.push(last);

    (ends, values)
}

pub fn runend_primitive_decode(
    ends: &PrimitiveArray,
    values: &PrimitiveArray,
    validity: Validity,
    offset: usize,
    length: usize,
) -> VortexResult<PrimitiveArray> {
    match_each_native_ptype!(values.ptype(), |$P| {
        match_each_integer_ptype!(ends.ptype(), |$E| {
            Ok(PrimitiveArray::from_vec(runend_primitive_decode_slice(
                ends.maybe_null_slice::<$E>(),
                values.maybe_null_slice::<$P>(),
                offset,
                length,
            ), validity))
        })
    })
}

pub fn runend_primitive_decode_slice<
    E: NativePType + AsPrimitive<usize> + FromPrimitive + Ord,
    T: NativePType,
>(
    run_ends: &[E],
    values: &[T],
    offset: usize,
    length: usize,
) -> Vec<T> {
    let offset_e = E::from_usize(offset).unwrap();
    let length_e = E::from_usize(length).unwrap();
    let trimmed_ends = run_ends
        .iter()
        .map(|v| *v - offset_e)
        .map(|v| min(v, length_e));

    let mut decoded = Vec::with_capacity(length);
    for (end, &value) in trimmed_ends.zip_eq(values) {
        decoded.extend(std::iter::repeat(value).take(end.as_() - decoded.len()));
    }
    decoded
}

#[cfg(test)]
mod test {
    use vortex::array::primitive::PrimitiveArray;
    use vortex::validity::{ArrayValidity, Validity};
    use vortex::IntoArray;

    use crate::primitive::compress::{runend_primitive_decode, runend_primitive_encode};
    use crate::primitive::RunEndPrimitiveArray;

    #[test]
    fn encode() {
        let arr = PrimitiveArray::from(vec![1i32, 1, 2, 2, 2, 3, 3, 3, 3, 3]);
        let (ends, values) = runend_primitive_encode(&arr);

        assert_eq!(ends.maybe_null_slice::<u64>(), vec![2, 5, 10]);
        assert_eq!(values.maybe_null_slice::<i32>(), vec![1, 2, 3]);
    }

    #[test]
    fn decode() {
        let ends = PrimitiveArray::from(vec![2, 5, 10]);
        let values = PrimitiveArray::from(vec![1i32, 2, 3]);
        let decoded =
            runend_primitive_decode(&ends, &values, Validity::NonNullable, 0, 10).unwrap();

        assert_eq!(
            decoded.maybe_null_slice::<i32>(),
            vec![1i32, 1, 2, 2, 2, 3, 3, 3, 3, 3]
        );
    }

    #[test]
    fn decode_nullable() {
        let validity = {
            let mut validity = vec![true; 10];
            validity[2] = false;
            validity[7] = false;
            Validity::from(validity)
        };
        let arr = RunEndPrimitiveArray::try_new(
            vec![2u32, 5, 10].into_array(),
            PrimitiveArray::from_vec(vec![1i32, 2, 3], Validity::AllValid).into_array(),
            validity,
        )
        .unwrap();

        let decoded = runend_primitive_decode(
            &arr.ends().as_primitive(),
            &arr.values().as_primitive(),
            arr.validity(),
            0,
            arr.len(),
        )
        .unwrap();

        assert_eq!(
            decoded.maybe_null_slice::<i32>(),
            vec![1i32, 1, 2, 2, 2, 3, 3, 3, 3, 3].as_slice()
        );
        assert_eq!(
            decoded.logical_validity().into_validity(),
            Validity::from(vec![
                true, true, false, true, true, true, true, false, true, true,
            ])
        );
    }
}
