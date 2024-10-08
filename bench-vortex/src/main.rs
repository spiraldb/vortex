use std::env;
use std::time::Instant;

use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets;
use bench_vortex::public_bi_data::PBIDataset::{
    AirlineSentiment, Arade, Bimbo, CMSprovider, Euro2016, Food, HashTags,
};
use log::{warn, LevelFilter};
use simplelog::{ColorChoice, Config, TermLogger, TerminalMode};
use vortex_sampling_compressor::SamplingCompressor;

fn main() {
    let mut args = env::args().skip(1);

    if args.len() > 1 {
        panic!("too many arguments");
    } else if args.len() < 1 {
        panic!("too few arguments");
    }

    let dataset: &str = &args.nth(0).unwrap();

    TermLogger::init(
        LevelFilter::Warn,
        Config::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )
    .unwrap();

    let compressor = SamplingCompressor::default();
    let dataset = match dataset {
        "AirlineSentiment" => BenchmarkDatasets::PBI(AirlineSentiment),
        "Arade" => BenchmarkDatasets::PBI(Arade),
        "CMSprovider" => BenchmarkDatasets::PBI(CMSprovider),
        "HashTags" => BenchmarkDatasets::PBI(HashTags),
        "Bimbo" => BenchmarkDatasets::PBI(Bimbo),
        "Euro2016" => BenchmarkDatasets::PBI(Euro2016),
        "Food" => BenchmarkDatasets::PBI(Food),
        _ => panic!("invalid dataset"),
    };
    let start = Instant::now();
    let uncompressed = dataset.to_vortex_array().unwrap();
    let compressed = compressor.compress(&uncompressed, None).unwrap();
    let duration = start.elapsed();

    warn!("Time elapsed: {:?}", duration);
    warn!("Uncompressed size: {}", uncompressed.nbytes());
    warn!("Compressed size: {}", compressed.nbytes());
    warn!("Encoding: {}", compressed.into_array().tree_display());
}
