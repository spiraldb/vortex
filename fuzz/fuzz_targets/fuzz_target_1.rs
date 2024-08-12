#![no_main]

use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::{fuzz_target, Corpus};
use vortex::array::{BoolArray, PrimitiveArray};
use vortex::compute::slice;
use vortex::compute::unary::scalar_at;
use vortex::validity::Validity;
use vortex::{Array, IntoArray};
use vortex_dtype::NativePType;
use vortex_sampling_compressor::compressors::alp::ALPCompressor;
use vortex_sampling_compressor::compressors::bitpacked::BitPackedCompressor;
use vortex_sampling_compressor::compressors::dict::DictCompressor;
use vortex_sampling_compressor::compressors::r#for::FoRCompressor;
use vortex_sampling_compressor::compressors::EncodingCompressor;
use vortex_sampling_compressor::SamplingCompressor;

fuzz_target!(|data: &[u8]| -> Corpus {
    let mut u = Unstructured::new(data);

    let array = random_array(&mut u);

    if array.is_empty() {
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
    match u.int_in_range(0..=9).unwrap() {
        0 => random_primitive::<u8>(u),
        1 => random_primitive::<u16>(u),
        2 => random_primitive::<u32>(u),
        3 => random_primitive::<u64>(u),
        4 => random_primitive::<i8>(u),
        5 => random_primitive::<i16>(u),
        6 => random_primitive::<i32>(u),
        7 => random_primitive::<i64>(u),
        8 => random_primitive::<f32>(u),
        9 => random_primitive::<f64>(u),
        10 => random_bool(u),
        _ => unreachable!(),
    }
}

fn random_primitive<'a, T: Arbitrary<'a> + NativePType>(u: &mut Unstructured<'a>) -> Array {
    let v = Vec::<T>::arbitrary(u).unwrap();
    let validity = random_validity(u, v.len());
    PrimitiveArray::from_vec(v, validity).into_array()
}

fn random_bool(u: &mut Unstructured) -> Array {
    let len: usize = u.arbitrary_len::<bool>().unwrap();
    let v = (0..len)
        .map(|_| bool::arbitrary(u).unwrap())
        .collect::<Vec<_>>();
    let validity = random_validity(u, len);

    BoolArray::from_vec(v, validity).into_array()
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

        assert_eq!(l.value(), r.value());
        assert_eq!(l.is_valid(), r.is_valid());
    }
}
