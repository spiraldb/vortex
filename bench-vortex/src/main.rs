use std::time::Instant;

use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::CMSprovider;
use log::{warn, LevelFilter};
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};
use vortex_sampling_compressor::SamplingCompressor;

fn main() {
    TermLogger::init(
        LevelFilter::Warn,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();

    let compressor = SamplingCompressor::default();
    let dataset = BenchmarkDatasets::PBI(CMSprovider);

    let start = Instant::now();
    let uncompressed = dataset.to_vortex_array().unwrap();
    let compressed = compressor.compress(&uncompressed, None).unwrap();
    let duration = start.elapsed();

    warn!("Time elapsed: {:?}", duration);
    warn!("Uncompressed size: {}", uncompressed.nbytes());
    warn!("Compressed size: {}", compressed.nbytes());
}
