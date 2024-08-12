#![no_main]

use libfuzzer_sys::arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use vortex::array::PrimitiveArray;
use vortex::compute::slice;
use vortex::compute::unary::scalar_at;
use vortex::validity::Validity;
use vortex::{Array, IntoArray};

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);

    let Some(array) = random_array(&mut u) else {
        return;
    };

    if array.len() == 0 {
        return;
    }

    let start = u.choose_index(array.len()).unwrap();
    let stop = u.choose_index(array.len() - start).unwrap() + start;
    let slice = slice(&array, start, stop).unwrap();
    assert_slice(&array, &slice, start);
});

fn random_array(u: &mut Unstructured) -> Option<Array> {
    match u8::arbitrary(u).unwrap() {
        0 => {
            let v = Vec::<u8>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            Some(PrimitiveArray::from_vec(v, validity).into_array())
        }
        1 => {
            let v = Vec::<u16>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            Some(PrimitiveArray::from_vec(v, validity).into_array())
        }
        2 => {
            let v = Vec::<u32>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            Some(PrimitiveArray::from_vec(v, validity).into_array())
        }
        3 => {
            let v = Vec::<u64>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            Some(PrimitiveArray::from_vec(v, validity).into_array())
        }
        4 => {
            let v = Vec::<i8>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            Some(PrimitiveArray::from_vec(v, validity).into_array())
        }
        5 => {
            let v = Vec::<i16>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            Some(PrimitiveArray::from_vec(v, validity).into_array())
        }
        6 => {
            let v = Vec::<i32>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            Some(PrimitiveArray::from_vec(v, validity).into_array())
        }
        7 => {
            let v = Vec::<i64>::arbitrary(u).unwrap();
            let validity = random_validity(u, v.len());
            Some(PrimitiveArray::from_vec(v, validity).into_array())
        }
        _ => None,
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
