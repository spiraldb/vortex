#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

use std::mem::MaybeUninit;

use arrayref::array_mut_ref;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fastlanez::{BitPack, BitPack2};

fn bitpacking(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitpacking");

    group.bench_function("pack 8 -> 3", |b| {
        const WIDTH: usize = 3;
        let values = [3u8; 1024];

        let mut packed = vec![0u8; 128 * WIDTH];

        b.iter(|| {
            BitPack2::<WIDTH>::bitpacker(&values, array_mut_ref![packed, 0, 128 * WIDTH]);
        });
    });

    group.bench_function("old pack 16 -> 3", |b| {
        const WIDTH: usize = 3;
        let values = [3u16; 1024];

        b.iter(|| {
            let mut packed = [MaybeUninit::new(0u8); 128 * WIDTH];
            black_box(BitPack::<WIDTH>::pack(&values, &mut packed));
        });
    });
}

criterion_group!(benches, bitpacking);
criterion_main!(benches);
