use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow_csv::reader::Format;
use itertools::Itertools;
use vortex::array::chunked::ChunkedArray;
use vortex::array::IntoArray;
use vortex::arrow::FromArrowType;
use vortex::serde::WriteCtx;
use vortex_error::VortexError;
use vortex_schema::DType;

use crate::data_downloads::{decompress_bz2, download_data, parquet_to_lance};
use crate::reader::{compress_csv_to_vortex, default_csv_format, write_csv_as_parquet};
use crate::{data_path, idempotent};

pub fn medicare_data_csv() -> PathBuf {
    let fname = "Medicare1_1.csv.bz2";
    download_data(
        data_path(fname),
        "http://www.cwi.nl/~boncz/PublicBIbenchmark/Medicare1/Medicare1_1.csv.bz2",
    );
    decompress_bz2(data_path(fname), data_path("Medicare1_1.csv"))
}

pub fn medicare_data_lance() -> PathBuf {
    let taxi_data = File::open(medicare_data_parquet()).unwrap();
    idempotent("medicare.lance", |path| {
        Ok::<PathBuf, VortexError>(parquet_to_lance(path, taxi_data))
    })
    .unwrap()
}

pub fn medicare_data_vortex_uncompressed() -> PathBuf {
    idempotent("medicare-uncompressed.vortex", |path| {
        let csv_file = File::open(medicare_data_csv()).unwrap();
        let reader = BufReader::new(csv_file.try_clone().unwrap());

        let (schema, _) = Format::default()
            .infer_schema(&mut csv_file.try_clone().unwrap(), None)
            .unwrap();

        let csv_reader = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
            .with_batch_size(crate::reader::BATCH_SIZE)
            .build(reader)?;

        let dtype = DType::from_arrow(SchemaRef::new(schema.clone()));

        let chunks = csv_reader
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

pub fn medicare_data_vortex() -> PathBuf {
    idempotent("medicare.vortex", |path| {
        let mut write = File::create(path).unwrap();
        let delimiter = u8::try_from('|').unwrap();
        compress_csv_to_vortex(
            medicare_data_csv(),
            default_csv_format().with_delimiter(delimiter),
            &mut write,
        )
    })
    .unwrap()
}

pub fn medicare_data_parquet() -> PathBuf {
    idempotent("medicare.parquet", |path| {
        let delimiter = u8::try_from('|').unwrap();
        let format = default_csv_format().with_delimiter(delimiter);
        let file = File::create(path).unwrap();
        write_csv_as_parquet(medicare_data_csv(), format, file)
    })
    .unwrap()
}
