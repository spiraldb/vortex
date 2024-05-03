use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
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
use monoio::buf::{IoBufMut, IoVecBufMut};
use monoio::io::AsyncReadRent;
use monoio::{BufResult, FusionDriver, RuntimeBuilder};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tokio::runtime::Runtime;
use vortex::array::chunked::ChunkedArray;
use vortex::arrow::FromArrowType;
use vortex::compress::Compressor;
use vortex::compute::take::take;
use vortex::{IntoArray, OwnedArray, ToArrayData, ToStatic};
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_ipc::iter::FallibleLendingIterator;
use vortex_ipc::reader::StreamReader;
use vortex_ipc::writer::StreamWriter;

use crate::CTX;

pub const BATCH_SIZE: usize = 65_536;

pub fn open_vortex(path: &Path) -> VortexResult<OwnedArray> {
    let mut rt = RuntimeBuilder::<FusionDriver>::new()
        .build()
        .expect("Unable to build runtime");
    rt.block_on(async_open_vortex(path))
}

struct MonoioFile(pub monoio::fs::File, pub u64);

impl AsyncReadRent for MonoioFile {
    async fn read<T: IoBufMut>(&mut self, buf: T) -> BufResult<usize, T> {
        let (len_res, buf) = self.0.read_at(buf, self.1).await;
        if let Ok(n) = len_res {
            self.1 += n as u64;
        }
        (len_res, buf)
    }

    async fn readv<T: IoVecBufMut>(&mut self, _buf: T) -> BufResult<usize, T> {
        unimplemented!("Reading multiple buffers at once for files is not implemented")
    }
}

pub async fn async_open_vortex(path: &Path) -> VortexResult<OwnedArray> {
    let file = MonoioFile(
        monoio::fs::File::open(path)
            .await
            .map_err(|e| vortex_err!(IOError: e))?,
        0,
    );

    let mut reader = StreamReader::try_new(file, &CTX).await?;
    let mut reader = reader.next().await?.unwrap();
    let dtype = reader.dtype().clone();
    let mut chunks = vec![];
    while let Some(chunk) = reader.next().await? {
        chunks.push(chunk.to_static())
    }
    Ok(ChunkedArray::try_new(chunks, dtype)?.into_array())
}

pub fn rewrite_parquet_as_vortex<W: Write>(
    parquet_path: PathBuf,
    write: &mut W,
) -> VortexResult<()> {
    let chunked = compress_parquet_to_vortex(parquet_path.as_path())?;

    let mut writer = StreamWriter::try_new(write, &CTX).unwrap();
    writer.write_array(&chunked.into_array()).unwrap();
    Ok(())
}

pub fn compress_parquet_to_vortex(parquet_path: &Path) -> VortexResult<ChunkedArray<'static>> {
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
    ChunkedArray::try_new(chunks, dtype.clone())
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

pub fn take_vortex(path: &Path, indices: &[u64]) -> VortexResult<OwnedArray> {
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
