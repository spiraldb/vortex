use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use arrow_array::RecordBatchReader;
use bzip2::read::BzDecoder;
use itertools::Itertools;
use lance::dataset::WriteParams;
use lance::Dataset;
use lance_parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder as LanceParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::runtime::Runtime;
use vortex::array::chunked::ChunkedArray;
use vortex::array::IntoArray;
use vortex::arrow::FromArrowType;
use vortex::serde::WriteCtx;
use vortex_error::VortexError;
use vortex_schema::DType;

use crate::reader::BATCH_SIZE;
use crate::{data_path, idempotent};

pub fn download_data(fname: &str, data_url: &str) -> PathBuf {
    idempotent(fname, |path| {
        let mut file = File::create(path).unwrap();
        println!("Downloading {} from {}", fname, data_url);
        reqwest::blocking::get(data_url).unwrap().copy_to(&mut file)
    })
    .unwrap()
}

pub fn parquet_to_lance(lance_fname: &Path, read: File) -> PathBuf {
    let write_params = WriteParams::default();
    let reader = LanceParquetRecordBatchReaderBuilder::try_new(read)
        .unwrap()
        .build()
        .unwrap();

    Runtime::new()
        .unwrap()
        .block_on(Dataset::write(
            reader,
            lance_fname.to_str().unwrap(),
            Some(write_params),
        ))
        .unwrap();
    PathBuf::from(lance_fname)
}

pub fn data_vortex_uncompressed(fname_out: &str, downloaded_data: PathBuf) -> PathBuf {
    idempotent(fname_out, |path| {
        let taxi_pq = File::open(downloaded_data).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(taxi_pq).unwrap();

        // FIXME(ngates): #157 the compressor should handle batch size.
        let reader = builder.with_batch_size(BATCH_SIZE).build().unwrap();

        let dtype = DType::from_arrow(reader.schema());

        let chunks = reader
            .map(|batch_result| batch_result.unwrap())
            .map(|record_batch| record_batch.into_array())
            .collect_vec();
        let chunked = ChunkedArray::new(chunks, dtype.clone());

        let mut write = File::create(path).unwrap();
        let mut write_ctx = WriteCtx::new(&mut write);
        write_ctx.dtype(&dtype)?;
        write_ctx.write(&chunked)
    })
    .unwrap()
}

pub fn decompress_bz2(input_path: &str, output_path: &str) -> PathBuf {
    idempotent(output_path, |path| {
        println!("Decompressing bzip from {} to {}", input_path, output_path);
        let input_file = File::open(data_path(input_path)).unwrap();
        let mut decoder = BzDecoder::new(input_file);

        let mut buffer = Vec::new();
        decoder.read_to_end(&mut buffer).unwrap();

        let mut output_file = File::create(path).unwrap();
        output_file.write_all(&buffer).unwrap();
        Ok::<PathBuf, VortexError>(data_path(output_path))
    })
    .unwrap()
}

pub trait BenchmarkDataset {
    fn uncompressed(&self);
    fn write_as_parquet(&self);
    fn write_as_vortex(&self);
    fn list_files(&self) -> Vec<String>;
    fn directory_location(&self) -> String;
}
