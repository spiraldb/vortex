use std::path::PathBuf;

use futures::executor::block_on;
use tokio::fs::File;
use vortex_error::VortexError;

use crate::data_downloads::{data_vortex_uncompressed, download_data, parquet_to_lance};
use crate::reader::rewrite_parquet_as_vortex;
use crate::{idempotent, IdempotentPath};

fn download_taxi_data() -> PathBuf {
    let taxi_parquet_fpath = "yellow-tripdata-2023-11.parquet".to_data_path();
    let taxi_data_url =
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet";
    download_data(taxi_parquet_fpath, taxi_data_url)
}

pub fn taxi_data_parquet() -> PathBuf {
    download_taxi_data()
}

pub fn taxi_data_lance() -> PathBuf {
    idempotent("taxi_lance", |output_fname| {
        parquet_to_lance(output_fname, taxi_data_parquet().as_path())
    })
    .unwrap()
}

pub fn taxi_data_vortex_uncompressed() -> PathBuf {
    data_vortex_uncompressed("taxi-uncompressed.vortex", download_taxi_data())
}

pub fn taxi_data_vortex() -> PathBuf {
    idempotent("taxi.vortex", |output_fname| {
        block_on(async {
            let output_file = File::create(output_fname).await?;
            rewrite_parquet_as_vortex(taxi_data_parquet(), output_file).await?;
            Ok::<PathBuf, VortexError>(output_fname.to_path_buf())
        })
    })
    .unwrap()
}
