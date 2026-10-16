// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::cmp::Reverse;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::bail;
use arrow_array::RecordBatch;
use arrow_schema::DataType;
use arrow_schema::Schema;
use async_trait::async_trait;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowColumnWriter;
use parquet::arrow::arrow_writer::compute_leaves;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterProperties;
use vortex_bench::Format;
use vortex_bench::compress::Compressed;
use vortex_bench::compress::CompressedData;
use vortex_bench::compress::Compressor;
use vortex_bench::compress::Uncompressed;
use vortex_bench::compress::read_projection;

/// Compressor implementation for Parquet format with ZSTD compression.
pub struct ParquetCompressor {
    compression: Compression,
}

impl ParquetCompressor {
    pub fn new() -> Self {
        Self {
            compression: Compression::ZSTD(ZstdLevel::default()),
        }
    }

    pub fn with_compression(compression: Compression) -> Self {
        Self { compression }
    }
}

impl Default for ParquetCompressor {
    fn default() -> Self {
        Self::new()
    }
}

/// Return the Arrow memory size after decoding the input Parquet file.
pub fn arrow_uncompressed_size(parquet_path: &Path) -> anyhow::Result<u64> {
    let file = File::open(parquet_path)?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
    let mut total = 0u64;

    for batch in reader {
        let batch = batch?;
        let batch_size = u64::try_from(batch.get_array_memory_size())?;
        total = total
            .checked_add(batch_size)
            .ok_or_else(|| anyhow::anyhow!("Arrow memory size exceeds u64"))?;
    }

    Ok(total)
}

#[async_trait]
impl Compressor for ParquetCompressor {
    fn format(&self) -> Format {
        Format::Parquet
    }

    async fn load(&self, parquet_path: &Path) -> anyhow::Result<Uncompressed> {
        Uncompressed::read_arrow(parquet_path)
    }

    async fn compress(&self, input: &Uncompressed) -> anyhow::Result<Compressed> {
        let (schema, batches) = input.arrow()?;
        let (schema, batches) = (Arc::clone(schema), batches.to_vec());
        let compression = self.compression;

        // Run the timed work as a task so it uses only the runtime's worker threads.
        let start = Instant::now();
        let (buf, size) = tokio::spawn(async move {
            let mut buf = Vec::new();
            let size = parquet_compress_write(&batches, schema, compression, &mut buf).await?;
            anyhow::Ok((buf, size))
        })
        .await??;
        let elapsed = start.elapsed();

        Ok(Compressed {
            data: CompressedData::Bytes(Bytes::from(buf)),
            size: size as u64,
            elapsed,
        })
    }

    async fn decompress(&self, compressed: &Compressed) -> anyhow::Result<Duration> {
        let CompressedData::Bytes(buf) = &compressed.data else {
            bail!("Parquet decompression expects in-memory bytes");
        };
        let buf = buf.clone();

        let timer = Instant::now();
        tokio::spawn(parquet_decompress_read(buf)).await??;
        Ok(timer.elapsed())
    }
}

pub async fn parquet_compress_write(
    batches: &[RecordBatch],
    schema: Arc<Schema>,
    compression: Compression,
    buf: &mut Vec<u8>,
) -> anyhow::Result<usize> {
    let mut buf = Cursor::new(buf);
    let writer_properties = WriterProperties::builder()
        .set_compression(compression)
        .build();
    let max_rows = writer_properties
        .max_row_group_row_count()
        .unwrap_or(usize::MAX);
    // `ArrowWriter` embeds the Arrow schema in the footer; converting it into the lower-level
    // writer keeps that while letting each column chunk be encoded as its own task.
    let (mut writer, factory) =
        ArrowWriter::try_new(&mut buf, Arc::clone(&schema), Some(writer_properties))?
            .into_serialized_writer()?;

    // Leaf writers grouped by the root field they belong to.
    let schema_descr = writer.schema_descr();
    let leaf_roots: Vec<usize> = (0..schema_descr.num_columns())
        .map(|leaf_idx| schema_descr.get_column_root_idx(leaf_idx))
        .collect();
    let row_groups = split_row_groups(batches, max_rows);
    let mut units = Vec::new();
    for (rg_idx, rg_batches) in row_groups.iter().enumerate() {
        let rg_batches: Arc<[RecordBatch]> = Arc::from(rg_batches.as_slice());
        let mut field_writers: Vec<Vec<ArrowColumnWriter>> =
            (0..schema.fields().len()).map(|_| Vec::new()).collect();
        for (leaf_idx, col_writer) in factory
            .create_column_writers(rg_idx)?
            .into_iter()
            .enumerate()
        {
            field_writers[leaf_roots[leaf_idx]].push(col_writer);
        }
        units.extend(
            field_writers
                .into_iter()
                .enumerate()
                .map(|(field_idx, writers)| (Arc::clone(&rg_batches), field_idx, writers)),
        );
    }

    let task_schema = Arc::clone(&schema);
    let encode_cost = |(rg_batches, field_idx, _): &(Arc<[RecordBatch]>, usize, _)| {
        rg_batches
            .iter()
            .map(|batch| batch.column(*field_idx).get_array_memory_size())
            .sum()
    };
    let chunks = parallel_map(
        units,
        encode_cost,
        move |(rg_batches, field_idx, mut writers)| {
            let field = task_schema.field(field_idx);
            for batch in rg_batches.iter() {
                let leaves = compute_leaves(field, batch.column(field_idx))?;
                for (col_writer, leaf) in writers.iter_mut().zip(&leaves) {
                    col_writer.write(leaf)?;
                }
            }
            writers
                .into_iter()
                .map(|w| Ok(w.close()?))
                .collect::<anyhow::Result<Vec<_>>>()
        },
    )
    .await?;

    // Size the output once rather than growing it chunk by chunk: repeatedly reallocating a
    // buffer of hundreds of megabytes costs as much as encoding. The slack covers the footer.
    let chunk_bytes: u64 = chunks
        .iter()
        .flatten()
        .map(|chunk| u64::try_from(chunk.close().metadata.compressed_size()).unwrap_or(0))
        .sum();
    let footer_slack = FOOTER_BYTES_PER_CHUNK * chunks.iter().map(Vec::len).sum::<usize>();
    writer
        .inner_mut()
        .get_mut()
        .reserve(usize::try_from(chunk_bytes)? + footer_slack);

    let fields_per_group = schema.fields().len();
    let mut chunks = chunks.into_iter();
    for _ in 0..row_groups.len() {
        let mut row_group_writer = writer.next_row_group()?;
        for chunk in chunks.by_ref().take(fields_per_group).flatten() {
            chunk.append_to_row_group(&mut row_group_writer)?;
        }
        row_group_writer.close()?;
    }

    let n_bytes = writer.bytes_written();
    writer.close()?;
    Ok(n_bytes)
}

/// Split `batches` into row groups of at most `max_rows` rows, matching the boundaries
/// [`ArrowWriter`] would produce.
fn split_row_groups(batches: &[RecordBatch], max_rows: usize) -> Vec<Vec<RecordBatch>> {
    let mut groups = Vec::new();
    let mut current = Vec::new();
    let mut current_rows = 0;
    for batch in batches {
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = (max_rows - current_rows).min(batch.num_rows() - offset);
            current.push(batch.slice(offset, len));
            offset += len;
            current_rows += len;
            if current_rows == max_rows {
                groups.push(std::mem::take(&mut current));
                current_rows = 0;
            }
        }
    }
    if !current.is_empty() {
        groups.push(current);
    }
    groups
}

pub async fn parquet_decompress_read(buf: Bytes) -> anyhow::Result<usize> {
    let metadata = ArrowReaderMetadata::load(&buf, ArrowReaderOptions::default())?;
    // Decode strings and binaries to view arrays, the Arrow types the Vortex reader produces.
    let view_schema = Arc::new(Schema::new_with_metadata(
        metadata
            .schema()
            .fields()
            .iter()
            .map(|field| {
                let data_type = match field.data_type() {
                    DataType::Utf8 | DataType::LargeUtf8 => DataType::Utf8View,
                    DataType::Binary | DataType::LargeBinary => DataType::BinaryView,
                    other => other.clone(),
                };
                field.as_ref().clone().with_data_type(data_type)
            })
            .collect::<Vec<_>>(),
        metadata.schema().metadata().clone(),
    ));
    let metadata = ArrowReaderMetadata::try_new(
        Arc::clone(metadata.metadata()),
        ArrowReaderOptions::new().with_schema(view_schema),
    )?;
    let num_columns = metadata.schema().fields().len();
    let columns =
        read_projection(num_columns).map_or_else(|| (0..num_columns).collect(), <[usize]>::to_vec);
    let projection = ProjectionMask::roots(metadata.parquet_schema(), columns);

    // Parallelise by row group, as DataFusion and DuckDB scan Parquet: each task decodes one whole
    // row group, all projected columns, and the runtime's workers share the tasks.
    let row_groups: Vec<(usize, usize)> = metadata
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .map(|(rg, rg_meta)| (rg, usize::try_from(rg_meta.num_rows()).unwrap_or(0)))
        .collect();

    let sizes = parallel_map(
        row_groups,
        |(_, rows)| *rows,
        move |(rg, _)| {
            let reader =
                ParquetRecordBatchReaderBuilder::new_with_metadata(buf.clone(), metadata.clone())
                    .with_row_groups(vec![rg])
                    .with_projection(projection.clone())
                    .with_batch_size(DECODE_BATCH_SIZE)
                    .build()?;
            let mut nbytes = 0;
            for batch in reader {
                nbytes += batch?.get_array_memory_size();
            }
            Ok(nbytes)
        },
    )
    .await?;

    Ok(sizes.into_iter().sum())
}

/// Footer bytes reserved per column chunk when sizing the output buffer.
const FOOTER_BYTES_PER_CHUNK: usize = 1024;

/// Rows per decoded batch: the Vortex writer's row block size, so both readers emit batches of
/// the same length.
const DECODE_BATCH_SIZE: usize = 8192;

/// Run `f` over `items` as tasks on the current Tokio runtime, returning the results in input
/// order.
///
/// Items are spawned in decreasing order of `cost`, so the largest units start first and do not
/// end up as a straggler behind many small ones.
///
/// Vortex runs its CPU work as tasks on the same runtime, so both formats share one pool of
/// worker threads, sized by `--threads`.
async fn parallel_map<T, R, C, F>(items: Vec<T>, cost: C, f: F) -> anyhow::Result<Vec<R>>
where
    T: Send + 'static,
    R: Send + 'static,
    C: Fn(&T) -> usize,
    F: Fn(T) -> anyhow::Result<R> + Send + Sync + 'static,
{
    let mut items: Vec<(usize, T)> = items.into_iter().enumerate().collect();
    items.sort_by_cached_key(|(_, item)| Reverse(cost(item)));

    let f = Arc::new(f);
    let mut handles: Vec<_> = items
        .into_iter()
        .map(|(idx, item)| {
            let f = Arc::clone(&f);
            (idx, tokio::spawn(async move { f(item) }))
        })
        .collect();
    handles.sort_unstable_by_key(|(idx, _)| *idx);

    let mut results = Vec::with_capacity(handles.len());
    for (_, handle) in handles {
        results.push(handle.await??);
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use arrow_array::StringArray;
    use arrow_schema::DataType;
    use arrow_schema::Field;

    use super::*;

    fn batch(start: i64, len: usize) -> anyhow::Result<RecordBatch> {
        let ints = Int64Array::from_iter_values(start..start + len as i64);
        let strs = StringArray::from_iter_values(ints.values().iter().map(|v| v.to_string()));
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("i", DataType::Int64, false),
                Field::new("s", DataType::Utf8, false),
            ])),
            vec![Arc::new(ints), Arc::new(strs)],
        )?)
    }

    #[test]
    fn split_row_groups_matches_writer_boundaries() -> anyhow::Result<()> {
        let batches = [batch(0, 3)?, batch(3, 5)?, batch(8, 0)?, batch(8, 2)?];
        let sizes: Vec<Vec<usize>> = split_row_groups(&batches, 4)
            .iter()
            .map(|group| group.iter().map(RecordBatch::num_rows).collect())
            .collect();
        assert_eq!(sizes, vec![vec![3, 1], vec![4], vec![2]]);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn parallel_write_round_trips() -> anyhow::Result<()> {
        let batches = [batch(0, 1000)?, batch(1000, 500)?];
        let schema = batches[0].schema();
        let mut buf = Vec::new();
        parquet_compress_write(
            &batches,
            Arc::clone(&schema),
            Compression::ZSTD(ZstdLevel::default()),
            &mut buf,
        )
        .await?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(buf.clone()))?.build()?;
        let read = reader.collect::<Result<Vec<_>, _>>()?;
        assert_eq!(
            arrow_select::concat::concat_batches(&schema, &read)?,
            arrow_select::concat::concat_batches(&schema, &batches)?
        );
        assert!(parquet_decompress_read(Bytes::from(buf)).await? > 0);
        Ok(())
    }
}
