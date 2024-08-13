#![no_main]

use std::collections::HashSet;

use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::{fuzz_target, Corpus};
use vortex::compute::slice;
use vortex::compute::unary::scalar_at;
use vortex::Array;
use vortex_sampling_compressor::compressors::alp::ALPCompressor;
use vortex_sampling_compressor::compressors::bitpacked::BitPackedCompressor;
use vortex_sampling_compressor::compressors::dict::DictCompressor;
use vortex_sampling_compressor::compressors::r#for::FoRCompressor;
use vortex_sampling_compressor::compressors::roaring_bool::RoaringBoolCompressor;
use vortex_sampling_compressor::compressors::roaring_int::RoaringIntCompressor;
use vortex_sampling_compressor::compressors::runend::DEFAULT_RUN_END_COMPRESSOR;
use vortex_sampling_compressor::compressors::sparse::SparseCompressor;
use vortex_sampling_compressor::compressors::zigzag::ZigZagCompressor;
use vortex_sampling_compressor::compressors::CompressorRef;
use vortex_sampling_compressor::SamplingCompressor;

fuzz_target!(|data: &[u8]| -> Corpus {
    let mut u = Unstructured::new(data);

    let array = Array::arbitrary(&mut u).unwrap();

    // TODO(adamg): We actually might want to test empty things, but I'm punting this issue for now
    if array.is_empty() {
        return Corpus::Reject;
    };
    match u.int_in_range(0..=9).unwrap() {
        0 => {
            let start = u.choose_index(array.len()).unwrap();
            let stop = u.choose_index(array.len() - start).unwrap() + start;
            let slice = slice(&array, start, stop).unwrap();
            assert_slice(&array, &slice, start);
        }
        1 => match fuzz_compress(&array, &ALPCompressor) {
            Some(compressed_array) => assert_array_eq(&array, &compressed_array),
            None => return Corpus::Reject,
        },
        2 => match fuzz_compress(&array, &BitPackedCompressor) {
            Some(compressed_array) => assert_array_eq(&array, &compressed_array),
            None => return Corpus::Reject,
        },
        3 => match fuzz_compress(&array, &DictCompressor) {
            Some(compressed_array) => assert_array_eq(&array, &compressed_array),
            None => return Corpus::Reject,
        },
        4 => match fuzz_compress(&array, &FoRCompressor) {
            Some(compressed_array) => assert_array_eq(&array, &compressed_array),
            None => return Corpus::Reject,
        },
        5 => match fuzz_compress(&array, &RoaringBoolCompressor) {
            Some(compressed_array) => assert_array_eq(&array, &compressed_array),
            None => return Corpus::Reject,
        },
        6 => match fuzz_compress(&array, &RoaringIntCompressor) {
            Some(compressed_array) => assert_array_eq(&array, &compressed_array),
            None => return Corpus::Reject,
        },
        7 => match fuzz_compress(&array, &DEFAULT_RUN_END_COMPRESSOR) {
            Some(compressed_array) => assert_array_eq(&array, &compressed_array),
            None => return Corpus::Reject,
        },
        8 => match fuzz_compress(&array, &SparseCompressor) {
            Some(compressed_array) => assert_array_eq(&array, &compressed_array),
            None => return Corpus::Reject,
        },
        9 => match fuzz_compress(&array, &ZigZagCompressor) {
            Some(compressed_array) => assert_array_eq(&array, &compressed_array),
            None => return Corpus::Reject,
        },
        _ => unreachable!(),
    }

    Corpus::Keep
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

        assert_eq!(o.value(), s.value());
        assert_eq!(o.is_valid(), s.is_valid());
    }
}

fn assert_array_eq(lhs: &Array, rhs: &Array) {
    assert_eq!(lhs.len(), rhs.len());
    for idx in 0..lhs.len() {
        let l = scalar_at(lhs, idx).unwrap();
        let r = scalar_at(rhs, idx).unwrap();

        assert_eq!(
            l.value(),
            r.value(),
            "{l} != {r} at index {idx}, lhs is {} rhs is {}",
            lhs.encoding().id(),
            rhs.encoding().id()
        );
        assert_eq!(l.is_valid(), r.is_valid());
    }
}
