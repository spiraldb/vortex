use std::fs::File;
use std::os::unix::prelude::MetadataExt;
use std::path::PathBuf;

use bench_vortex::data_downloads::BenchmarkDataset;
use bench_vortex::public_bi_data::BenchmarkDatasets::PBI;
use bench_vortex::public_bi_data::PBIDataset;
use bench_vortex::reader::{open_vortex, rewrite_parquet_as_vortex};
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::{data_path, setup_logger};
use log::LevelFilter;
use vortex::array::Array;
use vortex::formatter::display_tree;

pub fn main() {
    setup_logger(LevelFilter::Info);
    compress_pbi(PBIDataset::Medicare1);
    compress_taxi();
}

fn compress_taxi() {
    let path: PathBuf = data_path("taxi_data.vortex");
    {
        let mut write = File::create(&path).unwrap();
        rewrite_parquet_as_vortex(taxi_data_parquet(), &mut write).unwrap();
    }

    let taxi_vortex = open_vortex(&path).unwrap();

    let pq_size = taxi_data_parquet().metadata().unwrap().size();
    let vx_size = taxi_vortex.nbytes();

    println!("{}\n\n", display_tree(taxi_vortex.as_ref()));
    println!("Parquet size: {}, Vortex size: {}", pq_size, vx_size);
    println!("Compression ratio: {}", vx_size as f32 / pq_size as f32);
}

fn compress_pbi(which_pbi: PBIDataset) {
    let dataset = PBI(which_pbi);
    dataset.uncompressed();
    dataset.write_as_vortex();
    dataset.write_as_parquet();
}
