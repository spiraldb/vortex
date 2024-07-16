use std::cmp::min;

use arrow_buffer::buffer::BooleanBuffer;
use num_traits::{AsPrimitive, FromPrimitive};
use vortex::array::bool::BoolArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::validity::Validity;
use vortex_dtype::{match_each_integer_ptype, NativePType};
use vortex_error::VortexResult;

pub fn runend_bool_encode(elements: &BoolArray) -> (PrimitiveArray, bool) {
    let (arr, start) = runend_bool_encode_slice(elements.boolean_buffer().into_iter());
    (arr.into(), start)
}

pub fn runend_bool_encode_slice(elements: impl IntoIterator<Item = bool>) -> (Vec<u64>, bool) {
    let mut ends = Vec::new();
    let mut elements = elements.into_iter();

    let Some(mut last) = elements.next() else {
        return (ends, true);
    };
    let first = last;

    let mut end = 1;
    for e in elements {
        if e != last {
            ends.push(end);
        }
        last = e;
        end += 1;
    }
    ends.push(end);
    (ends, first)
}

pub fn runend_bool_decode(
    run_ends: &PrimitiveArray,
    start: bool,
    validity: Validity,
    offset: usize,
    length: usize,
) -> VortexResult<BoolArray> {
    match_each_integer_ptype!(run_ends.ptype(), |$E| {
        let bools = runend_bool_decode_slice::<$E>(run_ends.maybe_null_slice(), start, offset, length);
        BoolArray::try_new(BooleanBuffer::from(bools), validity)
    })
}

pub fn runend_bool_decode_slice<E: NativePType + AsPrimitive<usize> + FromPrimitive + Ord>(
    run_ends: &[E],
    start: bool,
    offset: usize,
    length: usize,
) -> Vec<bool> {
    let offset_e = E::from_usize(offset).unwrap();
    let length_e = E::from_usize(length).unwrap();
    let trimmed_ends = run_ends
        .iter()
        .map(|v| *v - offset_e)
        .map(|v| min(v, length_e));

    let mut decoded = Vec::with_capacity(length);
    for (idx, end) in trimmed_ends.enumerate() {
        decoded
            .extend(std::iter::repeat(value_at_index(idx, start)).take(end.as_() - decoded.len()));
    }
    decoded
}

pub fn value_at_index(idx: usize, start: bool) -> bool {
    if idx % 2 == 0 {
        start
    } else {
        !start
    }
}

#[cfg(test)]
mod test {
    use crate::bool::compress::{runend_bool_decode_slice, runend_bool_encode_slice};

    #[test]
    fn encode_bool() {
        let encoded = runend_bool_encode_slice([true, true, false, true]);
        assert_eq!(encoded, (vec![2, 3, 4], true))
    }

    #[test]
    fn encode_bool_false() {
        let encoded = runend_bool_encode_slice([false, false, true, false]);
        assert_eq!(encoded, (vec![2, 3, 4], false))
    }

    #[test]
    fn encode_decode_bool() {
        let input = [true, true, false, true, true, false];
        let (ends, start) = runend_bool_encode_slice(input);

        let decoded = runend_bool_decode_slice(ends.as_slice(), start, 0, input.len());
        assert_eq!(decoded, input)
    }

    #[test]
    fn encode_decode_bool_false_start() {
        let input = [false, false, true, true, false, true, true, false];
        let (ends, start) = runend_bool_encode_slice(input);

        let decoded = runend_bool_decode_slice(ends.as_slice(), start, 0, input.len());
        assert_eq!(decoded, input)
    }
}
