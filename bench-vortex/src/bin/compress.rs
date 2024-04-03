use bench_vortex::reader::{compress_parquet_to_vortex, open_vortex};
use bench_vortex::setup_logger;
use bench_vortex::taxi_data::taxi_data_parquet;
use log::LevelFilter;
use std::fs::File;
use std::os::unix::prelude::MetadataExt;
use std::path::PathBuf;
use vortex::array::Array;
use vortex::formatter::display_tree;

pub fn main() {
    setup_logger(LevelFilter::Debug);

    let path: PathBuf = "taxi_data.vortex".into();
    {
        let mut write = File::create(&path).unwrap();
        compress_parquet_to_vortex(&taxi_data_parquet(), &mut write).unwrap();
    }

    let taxi_vortex = open_vortex(&path).unwrap();

    let pq_size = taxi_data_parquet().metadata().unwrap().size();
    let vx_size = taxi_vortex.nbytes();

    println!("{}\n\n", display_tree(taxi_vortex.as_ref()));
    println!("Parquet size: {}, Vortex size: {}", pq_size, vx_size);
    println!("Compression ratio: {}", vx_size as f32 / pq_size as f32);
}
