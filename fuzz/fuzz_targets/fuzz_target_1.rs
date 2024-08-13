#![no_main]

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
use vortex_sampling_compressor::compressors::EncodingCompressor;
use vortex_sampling_compressor::SamplingCompressor;

fuzz_target!(|data: &[u8]| -> Corpus {
    let mut u = Unstructured::new(data);

    let array = Array::arbitrary(&mut u).unwrap();

    // TODO(adamg): We actually might want to test empty things, but I'm punting this issue for now
    if array.is_empty() {
        return Corpus::Reject;
    }

    match u.int_in_range(0..=9).unwrap() {
        0 => {
            let start = u.choose_index(array.len()).unwrap();
            let stop = u.choose_index(array.len() - start).unwrap() + start;
            let slice = slice(&array, start, stop).unwrap();
            assert_slice(&array, &slice, start);
        }
        1 => {
            println!("alp");
            // lets compress
            let ctx = SamplingCompressor::default();
            let compressed_array = match ALPCompressor
                .can_compress(&array)
                .map(|compressor| compressor.compress(&array, None, ctx))
            {
                Some(r) => r.unwrap(),
                None => return Corpus::Reject,
            }
            .into_array();

            assert_array_eq(&array, &compressed_array);
        }
        2 => {
            println!("bitpacked");
            // lets compress
            let ctx = SamplingCompressor::default();
            let compressed_array = match BitPackedCompressor
                .can_compress(&array)
                .map(|compressor| compressor.compress(&array, None, ctx))
            {
                Some(r) => r.unwrap(),
                None => return Corpus::Reject,
            }
            .into_array();

            assert_array_eq(&array, &compressed_array);
        }
        3 => {
            println!("dict");
            // lets compress
            let ctx = SamplingCompressor::default();
            let compressed_array = match DictCompressor
                .can_compress(&array)
                .map(|compressor| compressor.compress(&array, None, ctx))
            {
                Some(r) => r.unwrap(),
                None => return Corpus::Reject,
            }
            .into_array();

            assert_array_eq(&array, &compressed_array);
        }
        4 => {
            println!("for");
            // lets compress
            let ctx = SamplingCompressor::default();
            let compressed_array = match FoRCompressor
                .can_compress(&array)
                .map(|compressor| compressor.compress(&array, None, ctx))
            {
                Some(r) => r.unwrap(),
                None => return Corpus::Reject,
            }
            .into_array();

            assert_array_eq(&array, &compressed_array);
        }
        5 => {
            println!("roaring bool");
            // lets compress
            let ctx = SamplingCompressor::default();
            let compressed_array = match RoaringBoolCompressor
                .can_compress(&array)
                .map(|compressor| compressor.compress(&array, None, ctx))
            {
                Some(r) => r.unwrap(),
                None => return Corpus::Reject,
            }
            .into_array();

            assert_array_eq(&array, &compressed_array);
        }
        6 => {
            println!("roaring int");
            // lets compress
            let ctx = SamplingCompressor::default();
            let compressed_array = match RoaringIntCompressor
                .can_compress(&array)
                .map(|compressor| compressor.compress(&array, None, ctx))
            {
                Some(r) => r.unwrap(),
                None => return Corpus::Reject,
            }
            .into_array();

            assert_array_eq(&array, &compressed_array);
        }
        7 => {
            println!("default runend");
            // lets compress
            let ctx = SamplingCompressor::default();
            let compressed_array = match DEFAULT_RUN_END_COMPRESSOR
                .can_compress(&array)
                .map(|compressor| compressor.compress(&array, None, ctx))
            {
                Some(r) => r.unwrap(),
                None => return Corpus::Reject,
            }
            .into_array();

            assert_array_eq(&array, &compressed_array);
        }
        8 => {
            println!("sparse");
            // lets compress
            let ctx = SamplingCompressor::default();
            let compressed_array = match SparseCompressor
                .can_compress(&array)
                .map(|compressor| compressor.compress(&array, None, ctx))
            {
                Some(r) => r.unwrap(),
                None => return Corpus::Reject,
            }
            .into_array();

            assert_array_eq(&array, &compressed_array);
        }
        9 => {
            println!("zigag");
            // lets compress
            let ctx = SamplingCompressor::default();
            let compressed_array = match ZigZagCompressor
                .can_compress(&array)
                .map(|compressor| compressor.compress(&array, None, ctx))
            {
                Some(r) => r.unwrap(),
                None => return Corpus::Reject,
            }
            .into_array();

            assert_array_eq(&array, &compressed_array);
        }
        _ => unreachable!(),
    }

    Corpus::Keep
});

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
