#![allow(clippy::unwrap_used)]

use arrow::array::{as_primitive_array, ArrowNativeTypeOp, ArrowPrimitiveType};
use arrow::datatypes::{Float32Type, Float64Type};
use divan::{black_box, Bencher};
use vortex::array::PrimitiveArray;
use vortex::validity::Validity;
use vortex::variants::PrimitiveArrayTrait;
use vortex::IntoCanonical;
use vortex_alp::{alp_encode_components, ALPArray, ALPFloat, Exponents};
use vortex_dtype::NativePType;

fn main() {
    divan::main();
}

#[divan::bench(types = [f32, f64], args = [100_000, 10_000_000])]
fn alp_compress<T: ALPFloat>(n: usize) -> (Exponents, Vec<T::ALPInt>, Vec<u64>, Vec<T>) {
    let values: Vec<T> = vec![T::from(1.234).unwrap(); n];
    T::encode(values.as_slice(), None)
}

#[divan::bench(types = [f32, f64], args = [100_000, 1_000_000, 10_000_000])]
fn alp_iter<T>(bencher: Bencher, n: usize)
where
    T: ALPFloat + NativePType,
    T::ALPInt: NativePType,
{
    let values = PrimitiveArray::from_vec(vec![T::from(1.234).unwrap(); n], Validity::AllValid);
    let (exponents, encoded, patches) = alp_encode_components::<T>(&values, None);

    let alp_array = ALPArray::try_new(encoded, exponents, patches).unwrap();

    bencher.bench_local(move || black_box(alp_sum(alp_array.clone())));
}

#[divan::bench(types = [Float32Type, Float64Type], args = [100_000, 1_000_000, 10_000_000])]
fn alp_iter_to_arrow<T>(bencher: Bencher, n: usize)
where
    T: ArrowPrimitiveType,
    T::Native: ALPFloat + NativePType + From<f32>,
    <T::Native as ALPFloat>::ALPInt: NativePType,
{
    let values = PrimitiveArray::from_vec(vec![T::Native::from(1.234_f32); n], Validity::AllValid);
    let (exponents, encoded, patches) = alp_encode_components::<T::Native>(&values, None);

    let alp_array = ALPArray::try_new(encoded, exponents, patches).unwrap();

    bencher.bench_local(move || black_box(alp_canonicalize_sum::<T>(alp_array.clone())));
}

fn alp_canonicalize_sum<T: ArrowPrimitiveType>(array: ALPArray) -> T::Native {
    let array = array.into_canonical().unwrap().into_arrow();
    let arrow_primitive = as_primitive_array::<T>(array.as_ref().unwrap());
    arrow_primitive
        .iter()
        .fold(T::default_value(), |acc, value| {
            if let Some(value) = value {
                acc.add_wrapping(value)
            } else {
                acc
            }
        })
}

fn alp_sum(array: ALPArray) -> f64 {
    if let Some(iter) = array.f32_iter() {
        let mut sum = 0.0_f32;

        for batch in iter {
            for idx in 0..batch.len() {
                if batch.is_valid(idx) {
                    sum += unsafe { batch.get_unchecked(idx) }
                }
            }
        }

        return sum as f64;
    }

    if let Some(iter) = array.f64_iter() {
        let mut sum = 0.0_f64;

        for batch in iter {
            for idx in 0..batch.len() {
                if batch.is_valid(idx) {
                    sum += unsafe { batch.get_unchecked(idx) }
                }
            }
        }

        return sum;
    }

    unreachable!()
}
