use crate::data_downloads::{
    data_vortex_uncompressed, download_data, parquet_to_lance, parquet_to_vortex,
};
use crate::idempotent;
use std::fs::File;
use std::path::PathBuf;
use vortex_error::VortexError;

fn download_taxi_data() -> PathBuf {
    let taxi_parquet_fname = "yellow-tripdata-2023-11.parquet";
    let taxi_data_url =
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet";
    download_data(taxi_parquet_fname, taxi_data_url)
}

pub fn taxi_data_parquet() -> PathBuf {
    download_taxi_data()
}

pub fn taxi_data_lance() -> PathBuf {
    idempotent("taxi_lance", |output_fname| {
        let taxi_data = File::open(taxi_data_parquet()).unwrap();
        Ok::<PathBuf, VortexError>(parquet_to_lance(output_fname, taxi_data))
    })
    .unwrap()
}

pub fn taxi_data_vortex_uncompressed() -> PathBuf {
    data_vortex_uncompressed("taxi-uncompressed.vortex", download_taxi_data())
}

pub fn taxi_data_vortex() -> PathBuf {
    idempotent("taxi.vortex", |output_fname| {
        Ok::<PathBuf, VortexError>(parquet_to_vortex(output_fname, taxi_data_parquet()))
    })
    .unwrap()
}
