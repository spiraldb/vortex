#![no_main]

use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::{fuzz_target, Corpus};
use vortex::array::PrimitiveArray;
use vortex::compute::slice;
use vortex::compute::unary::scalar_at;
use vortex::validity::Validity;
use vortex::{Array, IntoArray};
use vortex_sampling_compressor::compressors::alp::ALPCompressor;
use vortex_sampling_compressor::compressors::bitpacked::BitPackedCompressor;
use vortex_sampling_compressor::compressors::dict::DictCompressor;
use vortex_sampling_compressor::compressors::r#for::FoRCompressor;
use vortex_sampling_compressor::compressors::EncodingCompressor;
use vortex_sampling_compressor::SamplingCompressor;

fuzz_target!(|data: &[u8]| -> Corpus {
    let mut u = Unstructured::new(data);

    let array = random_array(&mut u);

    if array.len() == 0 {
        return Corpus::Reject;
    }

    match u.int_in_range(0..=4).unwrap() {
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
        _ => unreachable!(),
    }

    Corpus::Keep
});

fn random_array(u: &mut Unstructured) -> Array {
    match u.int_in_range(0..=7).unwrap() {
        0 => {
            let v = Vec::<u8>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            PrimitiveArray::from_vec(v, validity).into_array()
        }
        1 => {
            let v = Vec::<u16>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            PrimitiveArray::from_vec(v, validity).into_array()
        }
        2 => {
            let v = Vec::<u32>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            PrimitiveArray::from_vec(v, validity).into_array()
        }
        3 => {
            let v = Vec::<u64>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            PrimitiveArray::from_vec(v, validity).into_array()
        }
        4 => {
            let v = Vec::<i8>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            PrimitiveArray::from_vec(v, validity).into_array()
        }
        5 => {
            let v = Vec::<i16>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            PrimitiveArray::from_vec(v, validity).into_array()
        }
        6 => {
            let v = Vec::<i32>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            PrimitiveArray::from_vec(v, validity).into_array()
        }
        7 => {
            let v = Vec::<i64>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            PrimitiveArray::from_vec(v, validity).into_array()
        }
        _ => unreachable!(),
    }
}

fn random_validity(u: &mut Unstructured, len: usize) -> Validity {
    match u.int_in_range(0..=3).unwrap() {
        0 => Validity::AllValid,
        1 => Validity::AllInvalid,
        2 => Validity::NonNullable,
        3 => {
            let bools = (0..len)
                .map(|_| bool::arbitrary(u).unwrap())
                .collect::<Vec<_>>();
            Validity::from(bools)
        }
        _ => unreachable!(),
    }
}

fn assert_slice(original: &Array, slice: &Array, start: usize) {
    for idx in 0..slice.len() {
        let o = scalar_at(&original, start + idx).unwrap();
        let s = scalar_at(&slice, idx).unwrap();

        assert_eq!(o, s);
    }
}

fn assert_array_eq(lhs: &Array, rhs: &Array) {
    assert_eq!(lhs.len(), rhs.len());
    for idx in 0..lhs.len() {
        let l = scalar_at(&lhs, idx).unwrap();
        let r = scalar_at(&rhs, idx).unwrap();

        assert_eq!(l.value(), r.value());
        assert_eq!(l.is_valid(), r.is_valid());
    }
}
