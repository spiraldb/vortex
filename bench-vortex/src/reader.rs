use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use arrow_array::types::Int64Type;
use arrow_array::{
    ArrayRef as ArrowArrayRef, PrimitiveArray as ArrowPrimitiveArray, RecordBatch,
    RecordBatchReader,
};
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use itertools::Itertools;
use lance::Dataset;
use log::info;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::runtime::Runtime;
use vortex::array::chunked::ChunkedArray;
use vortex::arrow::FromArrowType;
use vortex::compress::Compressor;
use vortex::compute::take::take;
use vortex::stream::ArrayStreamExt;
use vortex::{Array, IntoArray, ToArrayData, ViewContext};
use vortex_dtype::DType;
use vortex_error::VortexResult;
use vortex_ipc::io::{TokioAdapter, VortexWrite};
use vortex_ipc::writer::ArrayWriter;
use vortex_ipc::MessageReader;

use crate::CTX;

pub const BATCH_SIZE: usize = 65_536;

pub fn open_vortex(path: &Path) -> VortexResult<Array> {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let file = tokio::fs::File::open(path).await.unwrap();
            let mut msgs = MessageReader::try_new(TokioAdapter(file)).await.unwrap();
            msgs.array_stream_from_messages(&CTX)
                .await
                .unwrap()
                .collect_chunked()
                .await
        })
        .map(|a| a.into_array())
}

pub async fn rewrite_parquet_as_vortex<W: VortexWrite>(
    parquet_path: PathBuf,
    write: W,
) -> VortexResult<()> {
    let chunked = compress_parquet_to_vortex(parquet_path.as_path())?;

    ArrayWriter::new(write, ViewContext::from(&CTX.clone()))
        .write_context()
        .await?
        .write_array(chunked.into_array())
        .await?;

    Ok(())
}

pub fn compress_parquet_to_vortex(parquet_path: &Path) -> VortexResult<ChunkedArray> {
    let taxi_pq = File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(taxi_pq)?;

    // FIXME(ngates): #157 the compressor should handle batch size.
    let reader = builder.with_batch_size(BATCH_SIZE).build()?;

    let dtype = DType::from_arrow(reader.schema());

    let chunks = reader
        .map(|batch_result| batch_result.unwrap())
        .map(|record_batch| {
            let vortex_array = record_batch.to_array_data().into_array();
            Compressor::new(&CTX).compress(&vortex_array, None).unwrap()
        })
        .collect_vec();
    ChunkedArray::try_new(chunks, dtype)
}

pub fn write_csv_as_parquet(csv_path: PathBuf, output_path: &Path) -> VortexResult<()> {
    info!(
        "Compressing {} to parquet",
        csv_path.as_path().to_str().unwrap()
    );
    Command::new("duckdb")
        .arg("-c")
        .arg(format!(
            "COPY (SELECT * FROM read_csv('{}', delim = '|', header = false, nullstr = 'null')) TO '{}' (COMPRESSION ZSTD);",
            csv_path.as_path().to_str().unwrap(),
            output_path.to_str().unwrap()
        ))
        .status()?
        .exit_ok()
        .unwrap();
    Ok(())
}

pub fn take_vortex(path: &Path, indices: &[u64]) -> VortexResult<Array> {
    let array = open_vortex(path)?;
    let taken = take(&array, &indices.to_vec().into_array())?;
    // For equivalence.... we flatten to make sure we're not cheating too much.
    taken.flatten().map(|x| x.into_array())
}

pub fn take_parquet(path: &Path, indices: &[u64]) -> VortexResult<RecordBatch> {
    let file = File::open(path)?;

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
        row_groups
            .entry(row_group_idx)
            .or_insert_with(Vec::new)
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

    Ok(concat_batches(&schema, &batches)?)
}

pub fn take_lance(path: &Path, indices: &[u64]) -> RecordBatch {
    Runtime::new()
        .unwrap()
        .block_on(async_take_lance(path, indices))
}

async fn async_take_lance(path: &Path, indices: &[u64]) -> RecordBatch {
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    dataset.take(indices, dataset.schema()).await.unwrap()
}
