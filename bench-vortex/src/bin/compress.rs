use std::fs::File;
use std::os::unix::prelude::MetadataExt;
use std::path::PathBuf;

use bench_vortex::medicare_data::medicare_data_csv;
use bench_vortex::reader::{
    compress_csv_to_vortex, default_csv_format, open_vortex, rewrite_parquet_as_vortex,
    write_csv_as_parquet,
};
use bench_vortex::taxi_data::taxi_data_parquet;
use bench_vortex::{data_path, setup_logger};
use log::LevelFilter;
use vortex::array::Array;
use vortex::formatter::display_tree;

pub fn main() {
    setup_logger(LevelFilter::Debug);
    compress_taxi();
    compress_medicare();
    write_medicare_as_parquet();
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

fn compress_medicare() {
    let path: PathBuf = data_path("medicare.vortex");
    {
        let mut write = File::create(&path).unwrap();
        let delimiter = u8::try_from('|').unwrap();
        compress_csv_to_vortex(
            medicare_data_csv(),
            default_csv_format().with_delimiter(delimiter),
            &mut write,
        )
        .unwrap();
    }

    let medicare_vortex = open_vortex(&path).unwrap();

    let pq_size = medicare_data_csv().metadata().unwrap().size();
    let vx_size = medicare_vortex.nbytes();

    println!("{}\n\n", display_tree(medicare_vortex.as_ref()));
    println!("Csv size: {}, Vortex size: {}", pq_size, vx_size);
    println!("Compression ratio: {}", vx_size as f32 / pq_size as f32);
}

pub fn write_medicare_as_parquet() {
    let path = data_path("medicare.parquet");
    let delimiter = u8::try_from('|').unwrap();
    let format = default_csv_format().with_delimiter(delimiter);
    let file = File::create(path).unwrap();
    write_csv_as_parquet(medicare_data_csv(), format, file).unwrap();
}
