use criterion::{black_box, criterion_group, criterion_main, Criterion};
use rand::distributions::Uniform;
use rand::{thread_rng, Rng};
use vortex_fastlanes::{bitpack_primitive, unpack_primitive, unpack_single_primitive};

fn values(len: usize, bits: usize) -> Vec<u32> {
    let rng = thread_rng();
    let range = Uniform::new(0_u32, 2_u32.pow(bits as u32));
    rng.sample_iter(range).take(len).collect()
}

fn unpack_singles(packed: &[u8], bit_width: usize, length: usize) -> Vec<u32> {
    let mut output = Vec::with_capacity(length);
    for i in 0..length {
        unsafe {
            output.push(unpack_single_primitive(packed, bit_width, i).unwrap());
        }
    }
    output
}

fn pack_unpack(c: &mut Criterion) {
    let bits: usize = 8;
    let values = values(1_000_000, bits);

    c.bench_function("bitpack_primitive", |b| {
        b.iter(|| black_box(bitpack_primitive(&values, bits)));
    });

    let packed = bitpack_primitive(&values, bits);
    c.bench_function("unpack_primitive", |b| {
        b.iter(|| black_box(unpack_primitive::<u32>(&packed, bits, values.len())));
    });

    c.bench_function("unpack_all_singles", |b| {
        b.iter(|| black_box(unpack_singles(&packed, 8, values.len())));
    });

    c.bench_function("unpack_single_primitive", |b| {
        b.iter(|| black_box(unsafe { unpack_single_primitive::<u32>(&packed, 8, 0) }));
    });
}

criterion_group!(benches, pack_unpack);
criterion_main!(benches);
