use std::collections::HashMap;
use std::fs::File;
use std::ops::Range;
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
use bytes::{Bytes, BytesMut};
use futures::stream;
use itertools::Itertools;
use log::info;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::async_reader::{AsyncFileReader, ParquetObjectReader};
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use serde::{Deserialize, Serialize};
use stream::StreamExt;
use vortex::array::chunked::ChunkedArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::arrow::FromArrowType;
use vortex::compress::CompressionStrategy;
use vortex::stream::ArrayStreamExt;
use vortex::{Array, IntoArray, IntoCanonical, ToArrayData, ViewContext};
use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_err, VortexResult};
use vortex_ipc::chunked_reader::ChunkedArrayReader;
use vortex_ipc::io::ObjectStoreExt;
use vortex_ipc::io::{TokioAdapter, VortexReadAt, VortexWrite};
use vortex_ipc::writer::ArrayWriter;
use vortex_ipc::MessageReader;
use vortex_sampling_compressor::SamplingCompressor;

use crate::{COMPRESSORS, CTX};

pub const BATCH_SIZE: usize = 65_536;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VortexFooter {
    pub byte_offsets: Vec<u64>,
    pub row_offsets: Vec<u64>,
    pub view_context_dtype_range: Range<u64>,
}

pub async fn open_vortex(path: &Path) -> VortexResult<Array> {
    let file = tokio::fs::File::open(path).await.unwrap();
    let mut msgs = MessageReader::try_new(TokioAdapter(file)).await.unwrap();
    msgs.array_stream_from_messages(&CTX)
        .await
        .unwrap()
        .collect_chunked()
        .await
        .map(|a| a.into_array())
}

pub async fn rewrite_parquet_as_vortex<W: VortexWrite>(
    parquet_path: PathBuf,
    write: W,
) -> VortexResult<()> {
    let chunked = compress_parquet_to_vortex(parquet_path.as_path())?;

    let written = ArrayWriter::new(write, ViewContext::from(&CTX.clone()))
        .write_context()
        .await?
        .write_array_stream(chunked.array_stream())
        .await?;

    let view_ctx_range = written.view_context_range().unwrap();
    let layout = written.array_layouts()[0].clone();
    let mut w = written.into_inner();
    let mut s = flexbuffers::FlexbufferSerializer::new();
    VortexFooter {
        byte_offsets: layout.chunks.byte_offsets,
        row_offsets: layout.chunks.row_offsets,
        view_context_dtype_range: view_ctx_range.begin..layout.dtype.end,
    }
    .serialize(&mut s)?;
    let footer_bytes = Buffer::Bytes(Bytes::from(s.take_buffer()));
    let footer_len = footer_bytes.len() as u64;
    w.write_all(footer_bytes).await?;
    w.write_all(footer_len.to_le_bytes()).await?;

    Ok(())
}

pub fn compress_parquet_to_vortex(parquet_path: &Path) -> VortexResult<ChunkedArray> {
    let taxi_pq = File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(taxi_pq)?;

    // FIXME(ngates): #157 the compressor should handle batch size.
    let reader = builder.with_batch_size(BATCH_SIZE).build()?;

    let dtype = DType::from_arrow(reader.schema());

    let compressor: &dyn CompressionStrategy = &SamplingCompressor::new(COMPRESSORS.clone());
    let chunks = reader
        .map(|batch_result| batch_result.unwrap())
        .map(|record_batch| {
            let vortex_array = record_batch.to_array_data().into_array();
            compressor.compress(&vortex_array).unwrap()
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

pub async fn read_vortex_footer_format<R: VortexReadAt>(
    reader: R,
    len: u64,
) -> VortexResult<ChunkedArrayReader<R>> {
    let mut buf = BytesMut::with_capacity(8);
    unsafe { buf.set_len(8) }
    buf = reader.read_at_into(len - 8, buf).await?;
    let footer_len = u64::from_le_bytes(buf.as_ref().try_into().unwrap()) as usize;

    buf.reserve(footer_len - buf.len());
    unsafe { buf.set_len(footer_len) }
    buf = reader
        .read_at_into(len - footer_len as u64 - 8, buf)
        .await?;

    let footer: VortexFooter = VortexFooter::deserialize(
        flexbuffers::Reader::get_root(buf.as_ref()).map_err(|e| vortex_err!("{}", e))?,
    )?;

    let header_len =
        (footer.view_context_dtype_range.end - footer.view_context_dtype_range.start) as usize;
    buf.reserve(header_len - buf.len());
    unsafe { buf.set_len(header_len) }
    buf = reader
        .read_at_into(footer.view_context_dtype_range.start, buf)
        .await?;
    let mut header_reader = MessageReader::try_new(buf).await?;
    let view_ctx = header_reader.read_view_context(&CTX).await?;
    let dtype = header_reader.read_dtype().await?;

    ChunkedArrayReader::try_new(
        reader,
        view_ctx,
        dtype,
        PrimitiveArray::from(footer.byte_offsets).into_array(),
        PrimitiveArray::from(footer.row_offsets).into_array(),
    )
}

pub async fn take_vortex_object_store<O: ObjectStore>(
    fs: &O,
    path: &object_store::path::Path,
    indices: &[u64],
) -> VortexResult<Array> {
    let head = fs.head(path).await?;
    let indices_array = indices.to_vec().into_array();
    let taken = read_vortex_footer_format(fs.vortex_reader(path), head.size as u64)
        .await?
        .take_rows(&indices_array)
        .await?;
    // For equivalence.... we flatten to make sure we're not cheating too much.
    Ok(taken.into_canonical()?.into_array())
}

pub async fn take_vortex_tokio(path: &Path, indices: &[u64]) -> VortexResult<Array> {
    let len = File::open(path)?.metadata()?.len();
    let indices_array = indices.to_vec().into_array();
    let taken = read_vortex_footer_format(TokioAdapter(tokio::fs::File::open(path).await?), len)
        .await?
        .take_rows(&indices_array)
        .await?;
    // For equivalence.... we flatten to make sure we're not cheating too much.
    Ok(taken.into_canonical()?.into_array())
}

pub async fn take_parquet_object_store(
    fs: Arc<dyn ObjectStore>,
    path: &object_store::path::Path,
    indices: &[u64],
) -> VortexResult<RecordBatch> {
    let meta = fs.head(path).await?;
    let reader = ParquetObjectReader::new(fs, meta);
    parquet_take_from_stream(reader, indices).await
}

pub async fn take_parquet(path: &Path, indices: &[u64]) -> VortexResult<RecordBatch> {
    let file = tokio::fs::File::open(path).await?;
    parquet_take_from_stream(file, indices).await
}

async fn parquet_take_from_stream<T: AsyncFileReader + Unpin + Send + 'static>(
    async_reader: T,
    indices: &[u64],
) -> VortexResult<RecordBatch> {
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        async_reader,
        ArrowReaderOptions::new().with_page_index(true),
    )
    .await?;

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

    let schema = reader.schema().clone();

    let batches = reader
        .enumerate()
        .map(|(idx, batch)| {
            let batch = batch.unwrap();
            let indices = ArrowPrimitiveArray::<Int64Type>::from(row_group_indices[idx].clone());
            let indices_array: ArrowArrayRef = Arc::new(indices);
            take_record_batch(&batch, &indices_array).unwrap()
        })
        .collect::<Vec<_>>()
        .await;

    Ok(concat_batches(&schema, &batches)?)
}
