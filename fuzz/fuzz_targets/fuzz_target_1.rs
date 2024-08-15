#![no_main]

use std::collections::HashSet;

use libfuzzer_sys::{fuzz_target, Corpus};
use vortex::compute::slice;
use vortex::compute::unary::scalar_at;
use vortex::encoding::EncodingId;
use vortex::Array;
use vortex_fuzz::FuzzArrayAction;
use vortex_sampling_compressor::compressors::CompressorRef;
use vortex_sampling_compressor::SamplingCompressor;
use vortex_scalar::{PValue, Scalar, ScalarValue};

fuzz_target!(|fuzz_action: FuzzArrayAction| -> Corpus {
    let FuzzArrayAction { array, action } = fuzz_action;

    // TODO(adamg): We actually might want to test empty things, but I'm punting this issue for now
    if array.is_empty() {
        return Corpus::Reject;
    };

    match action {
        vortex_fuzz::Action::Compress(c) => match fuzz_compress(&array, c.as_ref()) {
            Some(compressed_array) => {
                assert_array_eq(&array, &compressed_array);
                Corpus::Keep
            }
            None => return Corpus::Reject,
        },
        vortex_fuzz::Action::Slice(range) => {
            let slice = slice(&array, range.start, range.end).unwrap();
            assert_slice(&array, &slice, range.start);
            Corpus::Keep
        }
        vortex_fuzz::Action::NoOp => Corpus::Reject,
    }
});

fn fuzz_compress(array: &Array, compressor_ref: CompressorRef<'_>) -> Option<Array> {
    let ctx = SamplingCompressor::new(HashSet::from([compressor_ref]));
    let compressed_array = ctx.compress(array, None).unwrap();

    compressed_array
        .path()
        .is_some()
        .then(|| compressed_array.into_array())
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
        "{l} != {r} at index {idx}, lhs is {} rhs is {}",
        lhs_encoding, rhs_encoding
    );
    assert_eq!(l.is_valid(), r.is_valid());
}
