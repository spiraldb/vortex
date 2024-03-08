use bench_vortex::{compress_taxi_data, download_taxi_data};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

fn enc_compress(c: &mut Criterion) {
    download_taxi_data();
    c.benchmark_group("compress");
    c.bench_function("compress", |b| b.iter(|| black_box(compress_taxi_data())));
}

criterion_group!(benches, enc_compress);
criterion_main!(benches);
