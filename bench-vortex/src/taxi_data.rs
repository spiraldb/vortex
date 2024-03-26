use arrow_array::RecordBatchReader;
use itertools::Itertools;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::fs::File;
use std::path::{Path, PathBuf};
use vortex::array::chunked::ChunkedArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{ArrayRef, IntoArray};
use vortex::arrow::FromArrowType;
use vortex::compute::take::take;
use vortex::formatter::display_tree;
use vortex::ptype::PType;
use vortex::serde::{ReadCtx, WriteCtx};
use vortex_schema::DType;

use crate::{compress_ctx, idempotent};

pub fn download_taxi_data() -> PathBuf {
    idempotent("yellow-tripdata-2023-11.parquet", |file| {
        reqwest::blocking::get(
            "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-11.parquet",
        )
        .unwrap()
        .copy_to(file)
        .unwrap();
    })
}

pub fn write_taxi_data() -> PathBuf {
    idempotent("taxi.spiral", |write| {
        let taxi_pq = File::open(download_taxi_data()).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(taxi_pq).unwrap();
        let _mask = ProjectionMask::roots(builder.parquet_schema(), (0..14).collect_vec());

        // FIXME(ngates): the compressor should handle batch size.
        let reader = builder
            // .with_limit(100)
            // .with_projection(_mask)
            .with_batch_size(65_536)
            .build()
            .unwrap();

        let dtype = DType::from_arrow(reader.schema());
        println!("SCHEMA {:?}\nDTYPE: {:?}", reader.schema(), dtype);
        let ctx = compress_ctx();

        let chunks = reader
            .map(|batch_result| batch_result.unwrap())
            .map(|record_batch| {
                println!("RBSCHEMA: {:?}", record_batch.schema());
                let vortex_array = record_batch.into_array();
                let compressed = ctx.compress(&vortex_array, None).unwrap();
                println!("COMPRESSED {}", display_tree(&compressed));
                compressed
            })
            .collect_vec();
        let chunked = ChunkedArray::new(chunks, dtype.clone());

        let mut write_ctx = WriteCtx::new(write);
        write_ctx.dtype(&dtype).unwrap();
        write_ctx.write(&chunked).unwrap();
    })
}

pub fn take_taxi_data(path: &Path, indices: &[u64]) -> ArrayRef {
    let chunked = {
        let mut file = File::open(path).unwrap();
        let dummy_dtype: DType = PType::U8.into();
        let mut read_ctx = ReadCtx::new(&dummy_dtype, &mut file);
        let dtype = read_ctx.dtype().unwrap();
        read_ctx.with_schema(&dtype).read().unwrap()
    };
    take(&chunked, &PrimitiveArray::from(indices.to_vec())).unwrap()
}
