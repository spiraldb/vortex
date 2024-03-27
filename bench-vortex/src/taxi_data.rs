use itertools::Itertools;
use lance;
use lance::dataset::WriteParams;
use lance::Dataset;
use lance_parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder as LanceParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use arrow_array::RecordBatchReader;
use std::fs::File;
use std::path::PathBuf;
use tokio::runtime::Runtime;
use vortex::array::chunked::ChunkedArray;
use vortex::array::IntoArray;
use vortex::arrow::FromArrowType;
use vortex::serde::WriteCtx;
use vortex_schema::DType;

use crate::reader::compress_vortex;
use crate::{data_path, idempotent};

fn download_taxi_data() -> PathBuf {
    idempotent("yellow-tripdata-2023-11.parquet", |file| {
        reqwest::blocking::get(
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet",
        )
        .unwrap()
        .copy_to(file)
    })
    .unwrap()
}

pub fn taxi_data_parquet() -> PathBuf {
    download_taxi_data()
}

pub fn taxi_data_lance() -> PathBuf {
    let write_params = WriteParams::default();

    let read = File::open(taxi_data_parquet()).unwrap();
    let reader = LanceParquetRecordBatchReaderBuilder::try_new(read)
        .unwrap()
        .build()
        .unwrap();

    let lance_path = data_path("taxi.lance");
    Runtime::new()
        .unwrap()
        .block_on(Dataset::write(
            reader,
            lance_path.as_os_str().to_str().unwrap(),
            Some(write_params),
        ))
        .unwrap();
    lance_path
}

pub fn taxi_data_vortex() -> PathBuf {
    idempotent("taxi-uncompressed.vortex", |write| {
        let taxi_pq = File::open(download_taxi_data()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(taxi_pq).unwrap();

        // FIXME(ngates): #157 the compressor should handle batch size.
        let reader = builder.with_batch_size(65_536).build().unwrap();

        let dtype = DType::from_arrow(reader.schema());

        let chunks = reader
            .map(|batch_result| batch_result.unwrap())
            .map(|record_batch| record_batch.into_array())
            .collect_vec();
        let chunked = ChunkedArray::new(chunks, dtype.clone());

        let mut write_ctx = WriteCtx::new(write);
        write_ctx.dtype(&dtype)?;
        write_ctx.write(&chunked)
    })
    .unwrap()
}

pub fn taxi_data_vortex_compressed() -> PathBuf {
    idempotent("taxi.vortex", |write| {
        compress_vortex(&taxi_data_parquet(), write)
    })
    .unwrap()
}
