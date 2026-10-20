// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use anyhow::ensure;
use async_trait::async_trait;
use futures::TryStreamExt;
use lance::Dataset;
use lance::cache::LanceCache;
use lance::dataset::WriteParams;
use lance::deps::arrow_array::UInt32Array;
use lance::deps::arrow_schema::Schema as ArrowSchema;
use lance::deps::datafusion::arrow::compute::concat_batches;
use lance_encoding::decoder::DecoderPlugins;
use lance_encoding::decoder::FilterExpression;
use lance_file::reader::FileReader;
use lance_file::reader::FileReaderOptions;
use lance_file::version::LanceFileVersion;
use lance_io::ReadBatchParams;
use lance_io::object_store::ObjectStore;
use lance_io::scheduler::ScanScheduler;
use lance_io::scheduler::SchedulerConfig;
use lance_io::utils::CachedFileSize;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use vortex_bench::Format;
use vortex_bench::datasets::feature_vectors;
use vortex_bench::datasets::nested_lists;
use vortex_bench::datasets::nested_structs;
use vortex_bench::datasets::taxi_data;
use vortex_bench::idempotent_async;
use vortex_bench::random_access::RandomAccessor;
use vortex_bench::random_access::RandomAccessorRet;
use vortex_bench::random_access::data_path;

/// Convert a parquet file to lance format.
///
/// Uses `idempotent_async` to skip conversion if the output already exists.
async fn parquet_to_lance_file(parquet_path: PathBuf, lance_path: &str) -> anyhow::Result<PathBuf> {
    idempotent_async(lance_path, |output_fname| async move {
        let file = File::open(&parquet_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let reader = builder.build()?;

        // Write Lance's current stable format into a single fragment, so a take reads one file
        // like the Vortex and Parquet accessors do instead of opening a reader per fragment.
        let write_params = WriteParams {
            max_rows_per_file: usize::MAX,
            ..WriteParams::with_storage_version(LanceFileVersion::Stable)
        };
        Dataset::write(
            reader,
            output_fname
                .to_str()
                .ok_or_else(|| anyhow!("Invalid output file path"))?,
            Some(write_params),
        )
        .await?;

        Ok(output_fname.to_path_buf())
    })
    .await
}

pub async fn taxi_data_lance() -> anyhow::Result<PathBuf> {
    let parquet_path = taxi_data::taxi_data_parquet().await?;
    parquet_to_lance_file(parquet_path, &data_path(taxi_data::DATASET, Format::Lance)).await
}

pub async fn feature_vectors_lance() -> anyhow::Result<PathBuf> {
    let parquet_path = feature_vectors::feature_vectors_parquet().await?;
    parquet_to_lance_file(
        parquet_path,
        &data_path(feature_vectors::DATASET, Format::Lance),
    )
    .await
}

pub async fn nested_lists_lance() -> anyhow::Result<PathBuf> {
    let parquet_path = nested_lists::nested_lists_parquet().await?;
    parquet_to_lance_file(
        parquet_path,
        &data_path(nested_lists::DATASET, Format::Lance),
    )
    .await
}

pub async fn nested_structs_lance() -> anyhow::Result<PathBuf> {
    let parquet_path = nested_structs::nested_structs_parquet().await?;
    parquet_to_lance_file(
        parquet_path,
        &data_path(nested_structs::DATASET, Format::Lance),
    )
    .await
}

/// Capacity of the cache holding the Lance file's decoded page metadata across takes.
const FILE_CACHE_BYTES: usize = 1 << 30;

/// Random accessor for Lance format files.
///
/// Like the Vortex and Parquet accessors, this reads the dataset's single data file directly
/// through an open [`FileReader`] whose metadata is parsed once at construction and reused across
/// `take()` calls. Going through `Dataset::take` would instead rebuild a projection plan and open a
/// fragment reader, with its own I/O scheduler, on every call.
pub struct LanceRandomAccessor {
    name: String,
    reader: FileReader,
}

impl LanceRandomAccessor {
    /// Open a Lance dataset and return a ready-to-use accessor.
    pub async fn open(path: PathBuf, name: impl Into<String>) -> anyhow::Result<Self> {
        Self::open_uri(
            path.to_str()
                .ok_or_else(|| anyhow!("Invalid dataset path"))?,
            name,
        )
        .await
    }

    /// Open a Lance dataset from any URI (local path, `s3://...`, ...).
    pub async fn open_uri(uri: &str, name: impl Into<String>) -> anyhow::Result<Self> {
        let dataset = Dataset::open(uri).await?;
        let fragments = dataset.get_fragments();
        let [fragment] = fragments.as_slice() else {
            anyhow::bail!(
                "Lance random access expects one fragment, found {}",
                fragments.len()
            );
        };
        let metadata = fragment.metadata();
        ensure!(
            metadata.deletion_file.is_none(),
            "Lance random access does not support deletions"
        );
        let [data_file] = metadata.files.as_slice() else {
            anyhow::bail!(
                "Lance random access expects one data file, found {}",
                metadata.files.len()
            );
        };

        let (object_store, _) = ObjectStore::from_uri(uri).await?;
        let scheduler = ScanScheduler::new(
            Arc::clone(&object_store),
            SchedulerConfig::max_bandwidth(&object_store),
        );
        let file = scheduler
            .open_file(
                &dataset.data_dir().join(data_file.path.as_str()),
                &CachedFileSize::unknown(),
            )
            .await?;
        let reader = FileReader::try_open(
            file,
            None,
            Arc::<DecoderPlugins>::default(),
            &LanceCache::with_capacity(FILE_CACHE_BYTES),
            FileReaderOptions::default(),
        )
        .await?;
        Ok(Self {
            name: name.into(),
            reader,
        })
    }
}

#[async_trait]
impl RandomAccessor for LanceRandomAccessor {
    fn format(&self) -> Format {
        Format::Lance
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn take(&self, indices: &[u64]) -> anyhow::Result<RandomAccessorRet> {
        let indices = indices
            .iter()
            .map(|&idx| u32::try_from(idx))
            .collect::<Result<UInt32Array, _>>()?;
        let batch_size = u32::try_from(indices.len())?.max(1);
        let batches: Vec<_> = self
            .reader
            .read_stream(
                ReadBatchParams::Indices(indices),
                batch_size,
                1,
                FilterExpression::no_filter(),
            )
            .await?
            .try_collect()
            .await?;
        let schema = Arc::new(ArrowSchema::from(self.reader.schema().as_ref()));
        let batch = concat_batches(&schema, &batches)?;
        Ok(RandomAccessorRet::Native(Box::new(batch)))
    }
}
