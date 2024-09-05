#![no_main]

use libfuzzer_sys::{fuzz_target, Corpus};
use vortex::compute::unary::scalar_at;
use vortex::compute::{search_sorted, slice, take, SearchResult, SearchSorted, SearchSortedSide};
use vortex::encoding::EncodingId;
use vortex::Array;
use vortex_error::VortexResult;
use vortex_fuzz::{Action, FuzzArrayAction};
use vortex_sampling_compressor::SamplingCompressor;
use vortex_scalar::{PValue, Scalar, ScalarValue};

fuzz_target!(|fuzz_action: FuzzArrayAction| -> Corpus {
    let FuzzArrayAction { array, actions } = fuzz_action;
    match &actions[0] {
        Action::Compress(c) => match fuzz_compress(&array, c) {
            Some(compressed_array) => {
                assert_array_eq(&array, &compressed_array);
                Corpus::Keep
            }
            None => Corpus::Reject,
        },
        Action::Slice(range) => {
            let slice = slice(&array, range.start, range.end).unwrap();
            assert_slice(&array, &slice, range.start);
            Corpus::Keep
        }
        Action::SearchSorted(s, side) => {
            if !array_is_sorted(&array).unwrap() || s.is_null() {
                return Corpus::Reject;
            }

            let search_result = search_sorted(&array, s.clone(), *side).unwrap();
            assert_search_sorted(&array, s, *side, search_result);
            Corpus::Keep
        }
        Action::Take(indices) => {
            if indices.is_empty() {
                return Corpus::Reject;
            }
            let taken = take(&array, indices).unwrap();
            assert_take(&array, &taken, indices);
            Corpus::Keep
        }
    }
});

fn fuzz_compress(array: &Array, compressor: &SamplingCompressor) -> Option<Array> {
    let compressed_array = compressor.compress(array, None).unwrap();

    compressed_array
        .path()
        .is_some()
        .then(|| compressed_array.into_array())
}

fn assert_search_sorted(
    original: &Array,
    value: &Scalar,
    side: SearchSortedSide,
    search_result: SearchResult,
) {
    let result = SearchSorted::search_sorted(original, value, side);
    assert_eq!(
        result,
        search_result,
        "Searching for {value} in {} from {side}",
        original.encoding().id()
    )
}

fn assert_take(original: &Array, taken: &Array, indices: &Array) {
    assert_eq!(taken.len(), indices.len());
    for idx in 0..indices.len() {
        let to_take = usize::try_from(&scalar_at(indices, idx).unwrap()).unwrap();
        let o = scalar_at(original, to_take).unwrap();
        let s = scalar_at(taken, idx).unwrap();

        fuzzing_scalar_cmp(o, s, original.encoding().id(), indices.encoding().id(), idx);
    }
}

fn assert_slice(original: &Array, slice: &Array, start: usize) {
    for idx in 0..slice.len() {
        let o = scalar_at(original, start + idx).unwrap();
        let s = scalar_at(slice, idx).unwrap();

        fuzzing_scalar_cmp(o, s, original.encoding().id(), slice.encoding().id(), idx);
    }
}

fn assert_array_eq(lhs: &Array, rhs: &Array) {
    assert_eq!(lhs.len(), rhs.len());
    for idx in 0..lhs.len() {
        let l = scalar_at(lhs, idx).unwrap();
        let r = scalar_at(rhs, idx).unwrap();

        fuzzing_scalar_cmp(l, r, lhs.encoding().id(), rhs.encoding().id(), idx);
    }
}

fn fuzzing_scalar_cmp(
    l: Scalar,
    r: Scalar,
    lhs_encoding: EncodingId,
    rhs_encoding: EncodingId,
    idx: usize,
) {
    let equal_values = match (l.value(), r.value()) {
        (ScalarValue::Primitive(l), ScalarValue::Primitive(r))
            if l.ptype().is_float() && r.ptype().is_float() =>
        {
            match (l, r) {
                (PValue::F16(l), PValue::F16(r)) => l == r || (l.is_nan() && r.is_nan()),
                (PValue::F32(l), PValue::F32(r)) => l == r || (l.is_nan() && r.is_nan()),
                (PValue::F64(l), PValue::F64(r)) => l == r || (l.is_nan() && r.is_nan()),
                _ => unreachable!(),
            }
        }
        _ => l.value() == r.value(),
    };

    assert!(
        equal_values,
        "{l} != {r} at index {idx}, lhs is {lhs_encoding} rhs is {rhs_encoding}",
    );
    assert_eq!(l.is_valid(), r.is_valid());
}

fn array_is_sorted(array: &Array) -> VortexResult<bool> {
    if array.is_empty() {
        return Ok(true);
    }

    let mut last_value = scalar_at(array, 0)?;
    for i in 1..array.len() {
        let next_value = scalar_at(array, i)?;
        if next_value < last_value {
            return Ok(false);
        }
        last_value = next_value;
    }
    Ok(true)
}
