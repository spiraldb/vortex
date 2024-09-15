#![no_main]

use std::collections::HashSet;

use libfuzzer_sys::{fuzz_target, Corpus};
use vortex::array::{
    BoolEncoding, PrimitiveEncoding, StructEncoding, VarBinEncoding, VarBinViewEncoding,
};
use vortex::compute::unary::scalar_at;
use vortex::compute::{search_sorted, slice, take, SearchResult, SearchSortedSide};
use vortex::encoding::{EncodingId, EncodingRef};
use vortex::{Array, IntoCanonical};
use vortex_fuzz::{sort_canonical_array, Action, FuzzArrayAction};
use vortex_sampling_compressor::SamplingCompressor;
use vortex_scalar::{PValue, Scalar, ScalarValue};

fuzz_target!(|fuzz_action: FuzzArrayAction| -> Corpus {
    let FuzzArrayAction { array, actions } = fuzz_action;
    let mut current_array = array.clone();
    for (i, (action, expected)) in actions.into_iter().enumerate() {
        match action {
            Action::Compress(c) => {
                match fuzz_compress(&current_array.into_canonical().unwrap().into(), &c) {
                    Some(compressed_array) => {
                        assert_array_eq(&expected.array(), &compressed_array, i);
                        current_array = compressed_array;
                    }
                    None => return Corpus::Reject,
                }
            }
            Action::Slice(range) => {
                current_array = slice(&current_array, range.start, range.end).unwrap();
                assert_array_eq(&expected.array(), &current_array, i);
            }
            Action::Take(indices) => {
                if indices.is_empty() {
                    return Corpus::Reject;
                }
                current_array = take(&current_array, &indices).unwrap();
                assert_array_eq(&expected.array(), &current_array, i);
            }
            Action::SearchSorted(s, side) => {
                // TODO(robert): Ideally we'd preserve the encoding perfectly but this is close enough
                let mut sorted = sort_canonical_array(&current_array);
                if !HashSet::from([
                    &PrimitiveEncoding as EncodingRef,
                    &VarBinEncoding,
                    &VarBinViewEncoding,
                    &BoolEncoding,
                    &StructEncoding,
                ])
                .contains(&current_array.encoding())
                {
                    sorted =
                        fuzz_compress(&sorted, &SamplingCompressor::default()).unwrap_or(sorted);
                }
                assert_search_sorted(sorted, s, side, expected.search(), i)
            }
        }
    }
    Corpus::Keep
});

fn fuzz_compress(array: &Array, compressor: &SamplingCompressor) -> Option<Array> {
    let compressed_array = compressor.compress(array, None).unwrap();

    compressed_array
        .path()
        .is_some()
        .then(|| compressed_array.into_array())
}

fn assert_search_sorted(
    array: Array,
    s: Scalar,
    side: SearchSortedSide,
    expected: SearchResult,
    step: usize,
) {
    let search_result = search_sorted(&array, s.clone(), side).unwrap();
    assert_eq!(
        search_result,
        expected,
        "Expected to find {s} at {expected} in {} from {side} but instead found it at {search_result} in step {step}",
        array.encoding().id()
    );
}

fn assert_array_eq(lhs: &Array, rhs: &Array, step: usize) {
    assert_eq!(lhs.len(), rhs.len());
    for idx in 0..lhs.len() {
        let l = scalar_at(lhs, idx).unwrap();
        let r = scalar_at(rhs, idx).unwrap();

        fuzzing_scalar_cmp(l, r, lhs.encoding().id(), rhs.encoding().id(), idx, step);
    }
}

fn fuzzing_scalar_cmp(
    l: Scalar,
    r: Scalar,
    lhs_encoding: EncodingId,
    rhs_encoding: EncodingId,
    idx: usize,
    step: usize,
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
        "{l} != {r} at index {idx}, lhs is {lhs_encoding} rhs is {rhs_encoding} in step {step}",
    );
    assert_eq!(l.is_valid(), r.is_valid());
}
