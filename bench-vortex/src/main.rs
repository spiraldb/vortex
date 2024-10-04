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
    let compressed = compressor
        .compress(&dataset.to_vortex_array().unwrap(), None)
        .unwrap();
    let duration = start.elapsed();

    warn!("Time elapsed: {:?}", duration);
    warn!("{}", compressed.nbytes());
}
