#![allow(incomplete_features)]
#![feature(generic_const_exprs)]

use std::mem::size_of;

use arrayref::{array_mut_ref, array_ref};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fastlanes::BitPacking;

fn bitpacking(c: &mut Criterion) {
    {
        let mut group = c.benchmark_group("bit-packing");
        group.bench_function("pack 16 -> 3 heap", |b| {
            const WIDTH: usize = 3;
            let values = vec![3u16; 1024];
            let mut packed = vec![0; 128 * WIDTH / size_of::<u16>()];

            b.iter(|| {
                BitPacking::bitpack::<WIDTH>(
                    array_ref![values, 0, 1024],
                    array_mut_ref![packed, 0, 192],
                );
            });
        });

        group.bench_function("pack 16 -> 3 stack", |b| {
            const WIDTH: usize = 3;
            let values = [3u16; 1024];
            let mut packed = [0; 128 * WIDTH / size_of::<u16>()];
            b.iter(|| BitPacking::bitpack::<WIDTH>(&values, &mut packed));
        });
    }

    {
        let mut group = c.benchmark_group("bit-unpacking");
        group.bench_function("unpack 16 <- 3 stack", |b| {
            const WIDTH: usize = 3;
            let values = [3u16; 1024];
            let mut packed = [0; 128 * WIDTH / size_of::<u16>()];
            BitPacking::bitpack::<WIDTH>(&values, &mut packed);

            let mut unpacked = [0u16; 1024];
            b.iter(|| BitPacking::bitunpack::<WIDTH>(&packed, &mut unpacked));
        });
    }

    {
        let mut group = c.benchmark_group("unpack-single");
        group.bench_function("unpack single 16 <- 3", |b| {
            const WIDTH: usize = 3;
            let values = [3u16; 1024];
            let mut packed = [0; 128 * WIDTH / size_of::<u16>()];
            BitPacking::bitpack::<WIDTH>(&values, &mut packed);

            b.iter(|| {
                for i in 0..1024 {
                    black_box::<u16>(BitPacking::bitunpack_single::<WIDTH>(&packed, i));
                }
            });
        });
    }
}

criterion_group!(benches, bitpacking);
criterion_main!(benches);
