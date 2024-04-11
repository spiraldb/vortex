use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::csv;
use arrow::datatypes::SchemaRef;
use arrow_array::types::Int64Type;
use arrow_array::{
    ArrayRef as ArrowArrayRef, PrimitiveArray as ArrowPrimitiveArray, RecordBatch,
    RecordBatchReader,
};
use arrow_csv::reader::Format;
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use itertools::Itertools;
use lance::Dataset;
use lance_arrow_array::RecordBatch as LanceRecordBatch;
use log::info;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use tokio::runtime::Runtime;
use vortex::array::chunked::ChunkedArray;
use vortex::array::primitive::PrimitiveArray;
use vortex::array::{ArrayRef, IntoArray};
use vortex::arrow::FromArrowType;
use vortex::compute::flatten::flatten;
use vortex::compute::take::take;
use vortex::ptype::PType;
use vortex::serde::{ReadCtx, WriteCtx};
use vortex_error::{VortexError, VortexResult};
use vortex_schema::DType;

use crate::{chunks_to_array, compress_ctx};

pub const BATCH_SIZE: usize = 65_536;
pub const CSV_SCHEMA_SAMPLE_ROWS: usize = 10_000_000;
const DEFAULT_DELIMITER: u8 = b',';

pub fn open_vortex(path: &Path) -> VortexResult<ArrayRef> {
    let mut file = File::open(path)?;
    let dummy_dtype: DType = PType::U8.into();
    let mut read_ctx = ReadCtx::new(&dummy_dtype, &mut file);
    let dtype = read_ctx.dtype()?;
    read_ctx.with_schema(&dtype).read()
}

pub fn rewrite_parquet_as_vortex<W: Write>(
    parquet_path: PathBuf,
    write: &mut W,
) -> VortexResult<()> {
    let (dtype, chunked) = compress_parquet_to_vortex(parquet_path.as_path())?;

    let mut write_ctx = WriteCtx::new(write);
    write_ctx.dtype(&dtype).unwrap();
    write_ctx.write(&chunked).unwrap();
    Ok(())
}

fn compress_parquet_to_vortex(parquet_path: &Path) -> Result<(DType, ChunkedArray), VortexError> {
    let taxi_pq = File::open(parquet_path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(taxi_pq)?;

    // FIXME(ngates): #157 the compressor should handle batch size.
    let reader = builder.with_batch_size(BATCH_SIZE).build()?;

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
    Ok((dtype, chunked))
}

pub fn default_csv_format() -> Format {
    Format::default()
        .with_delimiter(DEFAULT_DELIMITER)
        .with_header(false)
        .with_null_regex("null".parse().unwrap())
}

pub fn compress_csv_to_vortex(csv_path: PathBuf, format: Format) -> (DType, ArrayRef) {
    let csv_file = File::open(csv_path.clone()).unwrap();
    let (schema, _) = format
        .infer_schema(
            &mut csv_file.try_clone().unwrap(),
            Some(CSV_SCHEMA_SAMPLE_ROWS),
        )
        .unwrap();

    let csv_file2 = File::open(csv_path.clone()).unwrap();
    let reader = BufReader::new(csv_file2.try_clone().unwrap());

    let csv_reader = arrow::csv::ReaderBuilder::new(Arc::new(schema.clone()))
        .with_format(format)
        .with_batch_size(BATCH_SIZE)
        .build(reader)
        .unwrap();

    let ctx = compress_ctx();
    let mut uncompressed_size: usize = 0;
    let chunks = csv_reader
        .into_iter()
        .map(|batch_result| batch_result.unwrap())
        .map(|batch| batch.into_array())
        .map(|array| {
            uncompressed_size += array.nbytes();
            ctx.clone().compress(&array, None).unwrap()
        })
        .collect_vec();
    (
        DType::from_arrow(SchemaRef::new(schema.clone())),
        chunks_to_array(SchemaRef::new(schema), uncompressed_size, chunks),
    )
}

pub fn write_csv_to_vortex<W: Write>(
    csv_path: PathBuf,
    format: Format,
    write: &mut W,
) -> VortexResult<()> {
    let (dtype, chunked) = compress_csv_to_vortex(csv_path, format);

    let mut write_ctx = WriteCtx::new(write);
    write_ctx.dtype(&dtype).unwrap();
    write_ctx.write(&chunked).unwrap();
    Ok(())
}

pub fn write_csv_as_parquet<W: Write + Send + Sync>(
    csv_path: PathBuf,
    format: Format,
    write: W,
) -> VortexResult<()> {
    info!(
        "Compressing {} to parquet",
        csv_path.as_path().to_str().unwrap()
    );
    let file_handle_for_schema_inference = File::open(csv_path.clone()).unwrap();

    // Infer the schema of the CSV file
    let schema_inference_reader =
        BufReader::new(file_handle_for_schema_inference.try_clone().unwrap());
    let (schema, _) = format.infer_schema(schema_inference_reader, Some(CSV_SCHEMA_SAMPLE_ROWS))?;
    let file_handle_for_read = File::open(csv_path.clone()).unwrap();

    let file_reader = BufReader::new(file_handle_for_read.try_clone().unwrap());
    let csv_reader = csv::ReaderBuilder::new(SchemaRef::new(schema.clone()))
        .with_format(format)
        .with_batch_size(BATCH_SIZE)
        .build(file_reader)?;
    // WriterProperties can be used to set Parquet file options
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(write, SchemaRef::from(schema), Some(props)).unwrap();

    // Write CSV data to Parquet
    for maybe_batch in csv_reader {
        let record_batch = maybe_batch?;
        writer.write(&record_batch)?;
    }

    // Finalize the Parquet writer
    writer.close()?;
    Ok(())
}

pub fn take_vortex(path: &Path, indices: &[u64]) -> VortexResult<ArrayRef> {
    let array = open_vortex(path)?;
    let taken = take(&array, &PrimitiveArray::from(indices.to_vec()))?;
    // For equivalence.... we flatten to make sure we're not cheating too much.
    flatten(&taken).map(|x| x.into_array())
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

pub fn take_lance(path: &Path, indices: &[u64]) -> LanceRecordBatch {
    Runtime::new()
        .unwrap()
        .block_on(async_take_lance(path, indices))
}

async fn async_take_lance(path: &Path, indices: &[u64]) -> LanceRecordBatch {
    let dataset = Dataset::open(path.to_str().unwrap()).await.unwrap();
    dataset.take(indices, dataset.schema()).await.unwrap()
}
