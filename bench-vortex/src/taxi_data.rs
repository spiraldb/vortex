use std::fs::File;
use std::path::PathBuf;
use crate::data_downloads::{data_lance, data_vortex, data_vortex_uncompressed, download_data};


fn download_taxi_data() -> PathBuf {
    let taxi_parquet_fname = "yellow-tripdata-2023-11.parquet";
    let taxi_data_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet";
    download_data(taxi_parquet_fname, taxi_data_url)
}


pub fn taxi_data_parquet() -> PathBuf {
    download_taxi_data()
}

pub fn taxi_data_lance() -> PathBuf {
    data_lance("taxi_lance", File::open(taxi_data_parquet()).unwrap())
}

pub fn taxi_data_vortex_uncompressed() -> PathBuf {
    data_vortex_uncompressed("taxi-uncompressed.vortex", download_taxi_data())
}

pub fn taxi_data_vortex() -> PathBuf {
    data_vortex("taxi.vortex", taxi_data_parquet())
}
