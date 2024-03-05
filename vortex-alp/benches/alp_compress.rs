use vortex::array::primitive::PrimitiveArray;
use vortex::array::ArrayRef;
use vortex_alp::{ALPArray, ALPFloat, Exponents};

fn main() {
    divan::main();
}

#[divan::bench(types = [f32, f64], args = [100_000, 10_000_000])]
fn alp_compress<T: ALPFloat>(n: usize) -> (Exponents, Vec<T::ALPInt>, Vec<u64>, Vec<T>) {
    let values: Vec<T> = vec![T::from(1.234).unwrap(); n];
    T::encode(values.as_slice(), None)
}

// TODO(ngates): remove this
#[divan::bench(args = [100_000, 10_000_000])]
fn alp_compress_array(n: usize) -> ArrayRef {
    let array = PrimitiveArray::from_vec(vec![1.234f64; n]);
    ALPArray::encode(&array).unwrap()
}
