use vortex::array::PrimitiveArray;
use vortex::validity::Validity;
use vortex::variants::PrimitiveArrayTrait;
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

#[divan::bench(types = [f32, f64], args = [100_000, 10_000_000])]
fn alp_iter<T: ALPFloat + NativePType>(n: usize) -> f64
where
    T::ALPInt: NativePType,
{
    let values = PrimitiveArray::from_vec(vec![T::from(1.234).unwrap(); n], Validity::AllValid);
    let (exponents, encoded, patches) = alp_encode_components::<T>(&values, None);

    let alp_array = ALPArray::try_new(encoded, exponents, patches).unwrap();

    if let Some(iter) = alp_array.f32_iter() {
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

    if let Some(iter) = alp_array.f64_iter() {
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
