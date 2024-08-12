use vortex_alp::{ALPFloat, Exponents};

fn main() {
    divan::main();
}

#[allow(clippy::unwrap_used)]
#[divan::bench(types = [f32, f64], args = [100_000, 10_000_000])]
fn alp_compress<T: ALPFloat>(n: usize) -> (Exponents, Vec<T::ALPInt>, Vec<u64>, Vec<T>) {
    let values: Vec<T> = vec![T::from(1.234).unwrap(); n];
    T::encode(values.as_slice(), None)
}
