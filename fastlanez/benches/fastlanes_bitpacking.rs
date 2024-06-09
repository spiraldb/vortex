#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

use std::mem::MaybeUninit;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fastlanez::{BitPack, BitPack2};

fn bitpacking(c: &mut Criterion) {
    let mut group = c.benchmark_group("bitpacking");

    group.bench_function("pack 16 -> 3", |b| {
        const WIDTH: usize = 3;
        let values = [3u16; 1024];

        b.iter(|| {
            let mut packed = [0u8; 128 * WIDTH];
            BitPack2::<WIDTH>::bitpacker(&values, &mut packed);
            black_box(packed);
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
