use arrow_array::types::Int64Type;
use arrow_array::{
    ArrayRef as ArrowArrayRef, PrimitiveArray as ArrowPrimitiveArray, RecordBatch,
    RecordBatchReader,
};
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use itertools::Itertools;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use vortex::array::chunked::ChunkedArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{ArrayRef, IntoArray};
use vortex::arrow::FromArrowType;
use vortex::compute::take::take;
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
        let ctx = compress_ctx();

        let chunks = reader
            .map(|batch_result| batch_result.unwrap())
            .map(|record_batch| {
                let vortex_array = record_batch.into_array();
                ctx.compress(&vortex_array, None).unwrap()
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

pub fn take_taxi_data_arrow(path: &Path, indices: &[u64]) -> RecordBatch {
    let file = File::open(path).unwrap();

    // TODO(ngates): enable read_page_index
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();

    // We figure out which row groups we need to read and a selection filter for each of them.
    let mut row_groups = HashMap::new();
    let mut row_group_offsets = vec![0];
    row_group_offsets.extend(
        builder
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| rg.num_rows())
            .scan(0i64, |acc, x| {
                *acc += x;
                Some(*acc)
            }),
    );

    for idx in indices {
        let row_group_idx = row_group_offsets
            .binary_search(&(*idx as i64))
            .unwrap_or_else(|e| e - 1);
        if !row_groups.contains_key(&row_group_idx) {
            row_groups.insert(row_group_idx, Vec::new());
        }
        row_groups
            .get_mut(&row_group_idx)
            .unwrap()
            .push((*idx as i64) - row_group_offsets[row_group_idx]);
    }
    let row_group_indices = row_groups
        .keys()
        .sorted()
        .map(|i| row_groups.get(i).unwrap().clone())
        .collect_vec();

    let reader = builder
        .with_row_groups(row_groups.keys().copied().collect_vec())
        // FIXME(ngates): our indices code assumes the batch size == the row group sizes
        .with_batch_size(10_000_000)
        .build()
        .unwrap();

    let schema = reader.schema();

    let batches = reader
        .into_iter()
        .enumerate()
        .map(|(idx, batch)| {
            let batch = batch.unwrap();
            let indices = ArrowPrimitiveArray::<Int64Type>::from(row_group_indices[idx].clone());
            let indices_array: ArrowArrayRef = Arc::new(indices);
            take_record_batch(&batch, &indices_array).unwrap()
        })
        .collect_vec();

    concat_batches(&schema, &batches).unwrap()
}
