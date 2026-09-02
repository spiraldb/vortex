// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::collections::BTreeMap;
use std::fs::File as StdFile;
use std::iter::once;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::OnceLock;
use std::time::SystemTime;

use anyhow::Context;
use arrow_array::PrimitiveArray;
use arrow_array::types::Int64Type;
use arrow_ipc::reader::FileReader;
use arrow_select::concat::concat_batches;
use arrow_select::take::take_record_batch;
use async_trait::async_trait;
use futures::stream;
use itertools::Itertools;
use moka::sync::Cache;
use parking_lot::Mutex;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::metadata::PageIndexPolicy;
use stream::StreamExt;
use tokio::fs::File;
use vortex::array::Canonical;
use vortex::array::IntoArray;
use vortex::array::VortexSessionExecute;
use vortex::array::arrays::ChunkedArray;
use vortex::array::dtype::DType;
use vortex::array::stream::ArrayStreamExt;
use vortex::buffer::Buffer;
use vortex::file::OpenOptionsSessionExt;
use vortex::file::VortexFile;
use vortex::io::session::RuntimeSessionExt;
use vortex::scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex::utils::aliases::hash_map::HashMap;
use vortex::utils::parallelism::get_available_parallelism;
use vortex_mask::Mask;
use vortex_morsel::ExecPlan;
use vortex_morsel::MorselExecutor;
use vortex_morsel::MorselScan;
use vortex_morsel::SegmentSourceDriver;
use vortex_morsel::build_plan_for_ranges;
use vortex_morsel::natural_morsels_for;
use vortex_morsel::nodes::ConjunctMode;

use crate::Format;
use crate::SESSION;
use crate::random_access::ARROW_ROW_OFFSETS_METADATA_KEY;
use crate::random_access::RandomAccessor;
use crate::random_access::RandomAccessorRet;

const DEFAULT_RANDOM_ACCESS_MORSEL_ROWS: u64 = 131_072;
const MORSEL_RANDOM_ACCESS_CACHE_ENTRIES: u64 = 16;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct MorselRandomAccessCacheKey {
    path: PathBuf,
    format: Format,
    len: u64,
    modified: Option<SystemTime>,
}

#[derive(Default)]
struct MorselRandomAccessCacheEntry {
    natural_morsels: OnceLock<Arc<[Range<u64>]>>,
    state: Mutex<Option<MorselRandomAccessState>>,
}

static MORSEL_RANDOM_ACCESS_CACHE: LazyLock<
    Cache<MorselRandomAccessCacheKey, Arc<MorselRandomAccessCacheEntry>>,
> = LazyLock::new(|| {
    Cache::builder()
        .max_capacity(MORSEL_RANDOM_ACCESS_CACHE_ENTRIES)
        .build()
});

fn index_morsels(indices: &[u64], row_count: u64) -> anyhow::Result<Vec<Range<u64>>> {
    let mut ranges: Vec<Range<u64>> = Vec::new();
    for &index in indices {
        anyhow::ensure!(
            index < row_count,
            "Vortex row index {index} is out of bounds"
        );
        match ranges.last_mut() {
            Some(range) if range.end == index => range.end += 1,
            Some(range) => {
                anyhow::ensure!(
                    range.end < index,
                    "morsel random access requires strictly sorted row indices"
                );
                ranges.push(index..index + 1);
            }
            None => ranges.push(index..index + 1),
        }
    }
    Ok(ranges)
}

fn split_morsels_for_parallelism(mut ranges: Vec<Range<u64>>, target: usize) -> Vec<Range<u64>> {
    while ranges.len() < target {
        let Some((idx, range)) = ranges
            .iter()
            .enumerate()
            .filter(|(_, range)| range.end - range.start > 1)
            .max_by_key(|(_, range)| range.end - range.start)
            .map(|(idx, range)| (idx, range.clone()))
        else {
            break;
        };
        let middle = range.start + (range.end - range.start) / 2;
        ranges[idx] = range.start..middle;
        ranges.insert(idx + 1, middle..range.end);
    }
    ranges
}

fn index_morsel_demands(
    indices: &[u64],
    row_count: u64,
    natural_morsels: &[Range<u64>],
) -> anyhow::Result<Vec<(Range<u64>, Mask)>> {
    index_morsels(indices, row_count)?;

    let mut selected_count = 0;
    let mut selected_morsels = Vec::new();
    for natural in natural_morsels {
        let start = indices.partition_point(|&index| index < natural.start);
        let end = indices.partition_point(|&index| index < natural.end);
        let selected = &indices[start..end];
        if selected.is_empty() {
            continue;
        }
        selected_count += selected.len();
        selected_morsels.push(morsel_demand(selected)?);
    }
    anyhow::ensure!(
        selected_count == indices.len(),
        "natural morsels do not cover all selected row indices"
    );
    Ok(selected_morsels)
}

fn fixed_row_morsel_demands(
    indices: &[u64],
    row_count: u64,
    morsel_rows: u64,
    target_morsels: usize,
) -> anyhow::Result<Vec<(Range<u64>, Mask)>> {
    anyhow::ensure!(morsel_rows > 0, "fixed morsel row count must be positive");
    index_morsels(indices, row_count)?;

    let mut groups = Vec::new();
    let mut start = 0;
    while start < indices.len() {
        let bucket_start = indices[start] / morsel_rows * morsel_rows;
        let bucket_end = bucket_start.saturating_add(morsel_rows).min(row_count);
        let end = indices.partition_point(|&index| index < bucket_end);
        groups.push(start..end);
        start = end;
    }

    while groups.len() < target_morsels {
        let Some((idx, group)) = groups
            .iter()
            .enumerate()
            .filter(|(_, group)| group.len() > 1)
            .max_by_key(|(_, group)| group.len())
            .map(|(idx, group)| (idx, group.clone()))
        else {
            break;
        };
        let middle = group.start + group.len() / 2;
        groups[idx] = group.start..middle;
        groups.insert(idx + 1, middle..group.end);
    }

    groups
        .into_iter()
        .map(|group| morsel_demand(&indices[group]))
        .collect()
}

fn balanced_index_morsel_demands(
    indices: &[u64],
    row_count: u64,
    partitions: usize,
) -> anyhow::Result<Vec<(Range<u64>, Mask)>> {
    anyhow::ensure!(partitions > 0, "balanced morsel count must be positive");
    index_morsels(indices, row_count)?;
    if indices.is_empty() {
        return Ok(Vec::new());
    }

    let selected_per_morsel = indices.len().div_ceil(partitions);
    indices
        .chunks(selected_per_morsel)
        .map(morsel_demand)
        .collect()
}

fn row_partition_morsel_demands(
    indices: &[u64],
    row_count: u64,
    partitions: usize,
) -> anyhow::Result<Vec<(Range<u64>, Mask)>> {
    anyhow::ensure!(partitions > 0, "row partition count must be positive");
    let partitions = u64::try_from(partitions).context("row partition count exceeds u64")?;
    fixed_row_morsel_demands(indices, row_count, row_count.div_ceil(partitions), 1)
}

fn morsel_demand(indices: &[u64]) -> anyhow::Result<(Range<u64>, Mask)> {
    let (&first, &last) = indices
        .first()
        .zip(indices.last())
        .context("cannot create a morsel from no selected rows")?;
    let range = first..last + 1;
    let len = usize::try_from(range.end - range.start)
        .context("selected morsel row count exceeds usize")?;
    let relative_indices = indices
        .iter()
        .map(|&index| {
            usize::try_from(index - range.start).context("selected row offset exceeds usize")
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    Ok((range, Mask::from_indices(len, relative_indices)))
}

fn contains_variable_list(dtype: &DType) -> bool {
    match dtype {
        DType::List(..) => true,
        DType::FixedSizeList(element, ..) => contains_variable_list(element),
        DType::Struct(fields, _) => fields.fields().any(|dtype| contains_variable_list(&dtype)),
        _ => false,
    }
}

/// Random accessor for uncompressed Arrow IPC files.
pub struct ArrowIpcRandomAccessor {
    name: String,
    reader: Mutex<FileReader<StdFile>>,
    row_offsets: Vec<u64>,
    schema: arrow_schema::SchemaRef,
}

impl ArrowIpcRandomAccessor {
    pub fn open(path: PathBuf, name: impl Into<String>) -> anyhow::Result<Self> {
        let reader = FileReader::try_new(StdFile::open(path)?, None)?;
        let row_offsets = reader
            .custom_metadata()
            .get(ARROW_ROW_OFFSETS_METADATA_KEY)
            .context("Arrow IPC file is missing row-offset metadata")?
            .split(',')
            .map(str::parse)
            .collect::<Result<Vec<u64>, _>>()?;
        anyhow::ensure!(
            row_offsets.len() == reader.num_batches() + 1,
            "Arrow IPC row-offset metadata does not match the record batches"
        );
        anyhow::ensure!(
            row_offsets.first() == Some(&0),
            "Arrow IPC row offsets must start at zero"
        );
        anyhow::ensure!(
            row_offsets.windows(2).all(|window| window[0] <= window[1]),
            "Arrow IPC row offsets must be sorted"
        );
        let schema = reader.schema();
        Ok(Self {
            name: name.into(),
            reader: Mutex::new(reader),
            row_offsets,
            schema,
        })
    }
}

#[async_trait]
impl RandomAccessor for ArrowIpcRandomAccessor {
    fn format(&self) -> Format {
        Format::ArrowIpc
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn take(&self, indices: &[u64]) -> anyhow::Result<RandomAccessorRet> {
        let mut by_batch = BTreeMap::<usize, Vec<i64>>::new();
        for &index in indices {
            let batch_idx = self.row_offsets.partition_point(|offset| *offset <= index);
            anyhow::ensure!(
                batch_idx > 0 && batch_idx < self.row_offsets.len(),
                "Arrow row index {index} is out of bounds"
            );
            let batch_idx = batch_idx - 1;
            by_batch
                .entry(batch_idx)
                .or_default()
                .push(i64::try_from(index - self.row_offsets[batch_idx])?);
        }

        let mut reader = self.reader.lock();
        let mut batches = Vec::with_capacity(by_batch.len());
        for (batch_idx, local_indices) in by_batch {
            reader.set_index(batch_idx)?;
            let batch = reader
                .next()
                .context("Arrow IPC record batch is missing")??;
            let indices = PrimitiveArray::<Int64Type>::from(local_indices);
            batches.push(take_record_batch(&batch, &indices)?);
        }

        Ok(RandomAccessorRet::RecordBatch(concat_batches(
            &self.schema,
            &batches,
        )?))
    }
}

/// Random accessor for Vortex format files.
///
/// The file handle is opened at construction time and reused across `take()` calls.
pub struct VortexRandomAccessor {
    name: String,
    format: Format,
    file: VortexFile,
    morsel_cache: Arc<MorselRandomAccessCacheEntry>,
}

struct MorselRandomAccessState {
    plan: Arc<ExecPlan>,
    executor: MorselExecutor,
}

impl VortexRandomAccessor {
    /// Open a Vortex file and return a ready-to-use accessor.
    pub async fn open(
        path: impl AsRef<std::path::Path>,
        name: impl Into<String>,
        format: Format,
    ) -> anyhow::Result<Self> {
        let path = std::fs::canonicalize(path.as_ref())?;
        let metadata = std::fs::metadata(&path)?;
        let cache_key = MorselRandomAccessCacheKey {
            path: path.clone(),
            format,
            len: metadata.len(),
            modified: metadata.modified().ok(),
        };
        let file = SESSION
            .open_options()
            .with_layout_reader_cache()
            .open_path(&path)
            .await?;
        let morsel_cache = MORSEL_RANDOM_ACCESS_CACHE.get_with(cache_key, || {
            Arc::new(MorselRandomAccessCacheEntry::default())
        });
        Ok(Self {
            name: name.into(),
            format,
            file,
            morsel_cache,
        })
    }
}

#[async_trait]
impl RandomAccessor for VortexRandomAccessor {
    fn format(&self) -> Format {
        self.format
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn take(&self, indices: &[u64]) -> anyhow::Result<RandomAccessorRet> {
        if std::env::var("VORTEX_RANDOM_ACCESS_MORSEL").is_ok_and(|value| value == "1") {
            return self.take_morsels(indices);
        }

        let indices_buf: Buffer<u64> = Buffer::from(indices.to_vec());
        let array = self
            .file
            .scan()?
            .with_row_indices(StrictSortedBuffer::try_new(indices_buf)?)
            .into_array_stream()?
            .read_all()
            .await?;

        // We canonicalize / decompress for equivalence to Arrow's `RecordBatch`es.
        let mut ctx = SESSION.create_execution_ctx();
        let canonical = array.execute::<Canonical>(&mut ctx)?.into_array();
        Ok(RandomAccessorRet::ArrayRef(canonical))
    }
}

impl VortexRandomAccessor {
    fn take_morsels(&self, indices: &[u64]) -> anyhow::Result<RandomAccessorRet> {
        let mut state = self.morsel_cache.state.lock();
        let exact_ranges = index_morsels(indices, self.file.row_count())?;
        // Keep executor width independent from morsel sizing and do not impose an arbitrary cap.
        // V1 can drive its split tasks across the full runtime; restricting only the morsel path
        // hides available parallelism on sparse scans. Dispatch below still bounds the worker
        // count by the number of runnable morsels.
        let available_threads = get_available_parallelism().unwrap_or(1);
        let configured_threads = std::env::var("VORTEX_RANDOM_ACCESS_MORSEL_THREADS")
            .ok()
            .map(|value| {
                let threads = value
                    .parse::<usize>()
                    .context("VORTEX_RANDOM_ACCESS_MORSEL_THREADS must be a positive integer")?;
                anyhow::ensure!(
                    threads > 0,
                    "VORTEX_RANDOM_ACCESS_MORSEL_THREADS must be greater than zero"
                );
                Ok::<_, anyhow::Error>(threads)
            })
            .transpose()?;
        let fixed_rows = std::env::var("VORTEX_RANDOM_ACCESS_MORSEL_ROWS")
            .ok()
            .map(|value| {
                let rows = value
                    .parse::<u64>()
                    .context("VORTEX_RANDOM_ACCESS_MORSEL_ROWS must be a positive integer")?;
                anyhow::ensure!(
                    rows > 0,
                    "VORTEX_RANDOM_ACCESS_MORSEL_ROWS must be greater than zero"
                );
                Ok::<_, anyhow::Error>(rows)
            })
            .transpose()?;
        let balanced_partitions = std::env::var("VORTEX_RANDOM_ACCESS_MORSEL_PARTITIONS")
            .ok()
            .map(|value| {
                let partitions = value
                    .parse::<usize>()
                    .context("VORTEX_RANDOM_ACCESS_MORSEL_PARTITIONS must be a positive integer")?;
                anyhow::ensure!(
                    partitions > 0,
                    "VORTEX_RANDOM_ACCESS_MORSEL_PARTITIONS must be greater than zero"
                );
                Ok::<_, anyhow::Error>(partitions)
            })
            .transpose()?;
        let row_partitions = std::env::var("VORTEX_RANDOM_ACCESS_MORSEL_ROW_PARTITIONS")
            .ok()
            .map(|value| {
                let partitions = value.parse::<usize>().context(
                    "VORTEX_RANDOM_ACCESS_MORSEL_ROW_PARTITIONS must be a positive integer",
                )?;
                anyhow::ensure!(
                    partitions > 0,
                    "VORTEX_RANDOM_ACCESS_MORSEL_ROW_PARTITIONS must be greater than zero"
                );
                Ok::<_, anyhow::Error>(partitions)
            })
            .transpose()?;
        let natural_morsels = std::env::var("VORTEX_RANDOM_ACCESS_MORSEL_NATURAL")
            .ok()
            .map(|value| {
                anyhow::ensure!(
                    value == "1",
                    "VORTEX_RANDOM_ACCESS_MORSEL_NATURAL must be 1"
                );
                Ok::<_, anyhow::Error>(true)
            })
            .transpose()?
            .unwrap_or(false);
        anyhow::ensure!(
            usize::from(fixed_rows.is_some())
                + usize::from(balanced_partitions.is_some())
                + usize::from(row_partitions.is_some())
                + usize::from(natural_morsels)
                <= 1,
            "set only one morsel sizing strategy"
        );
        let cut = std::env::var("VORTEX_RANDOM_ACCESS_MORSEL_CUT").unwrap_or_default();
        let use_exact = match cut.as_str() {
            "" => exact_ranges.len().saturating_mul(4) <= indices.len(),
            "exact" => true,
            "sparse" => false,
            other => anyhow::bail!(
                "VORTEX_RANDOM_ACCESS_MORSEL_CUT must be exact or sparse, got {other}"
            ),
        };
        let use_natural_morsels = natural_morsels
            || (fixed_rows.is_none()
                && balanced_partitions.is_none()
                && row_partitions.is_none()
                && contains_variable_list(self.file.dtype()));
        let sparse_strategy = if fixed_rows.is_some() {
            "fixed-rows"
        } else if balanced_partitions.is_some() {
            "balanced-selection"
        } else if row_partitions.is_some() {
            "row-partitions"
        } else if use_natural_morsels {
            if natural_morsels {
                "natural"
            } else {
                "natural-variable-list"
            }
        } else {
            "fixed-rows"
        };
        let sparse_target_morsels = configured_threads.unwrap_or(available_threads);
        let mut selected_morsels = if use_exact {
            exact_ranges
                .iter()
                .cloned()
                .map(|range| {
                    let len = usize::try_from(range.end - range.start)
                        .context("exact morsel row count exceeds usize")?;
                    Ok((range, Mask::new_true(len)))
                })
                .collect::<anyhow::Result<Vec<_>>>()?
        } else if let Some(rows) = fixed_rows {
            fixed_row_morsel_demands(indices, self.file.row_count(), rows, sparse_target_morsels)?
        } else if let Some(partitions) = balanced_partitions {
            balanced_index_morsel_demands(indices, self.file.row_count(), partitions)?
        } else if let Some(partitions) = row_partitions {
            row_partition_morsel_demands(indices, self.file.row_count(), partitions)?
        } else if use_natural_morsels {
            if self.morsel_cache.natural_morsels.get().is_none() {
                let natural = natural_morsels_for(
                    self.file.footer().layout(),
                    &vortex::expr::root(),
                    None,
                    0,
                )?;
                self.morsel_cache
                    .natural_morsels
                    .set(Arc::from(natural))
                    .map_err(|_| {
                        anyhow::anyhow!("natural morsels were initialized concurrently")
                    })?;
            }
            let natural = self
                .morsel_cache
                .natural_morsels
                .get()
                .context("natural morsels were not initialized")?;
            index_morsel_demands(indices, self.file.row_count(), natural)?
        } else {
            fixed_row_morsel_demands(
                indices,
                self.file.row_count(),
                DEFAULT_RANDOM_ACCESS_MORSEL_ROWS,
                sparse_target_morsels,
            )?
        };
        let planned_ranges = selected_morsels
            .iter()
            .map(|(range, _)| range.clone())
            .collect::<Vec<_>>();
        let reuse_plan = state
            .as_ref()
            .is_some_and(|state| state.plan.supports_ranges(&planned_ranges));
        let plan = if reuse_plan {
            Arc::clone(
                &state
                    .as_ref()
                    .ok_or_else(|| anyhow::anyhow!("reusable morsel plan is missing"))?
                    .plan,
            )
        } else {
            Arc::new(build_plan_for_ranges(
                self.file.footer().layout(),
                &vortex::expr::root(),
                None,
                ConjunctMode::Cascade,
                &planned_ranges,
            )?)
        };
        let nodes_per_morsel = plan.len().div_ceil(selected_morsels.len().max(1));
        let target_threads = configured_threads.unwrap_or_else(|| {
            default_morsel_threads(nodes_per_morsel, use_exact, available_threads)
        });
        if use_exact {
            let ranges = split_morsels_for_parallelism(
                selected_morsels
                    .iter()
                    .map(|(range, _)| range.clone())
                    .collect(),
                target_threads,
            );
            selected_morsels = ranges
                .into_iter()
                .map(|range| {
                    let len = usize::try_from(range.end - range.start)
                        .context("exact morsel row count exceeds usize")?;
                    Ok((range, Mask::new_true(len)))
                })
                .collect::<anyhow::Result<Vec<_>>>()?;
        }
        let threads = target_threads.min(selected_morsels.len().max(1));
        if std::env::var("VORTEX_RANDOM_ACCESS_MORSEL_DIAGNOSTICS").is_ok_and(|value| value == "1")
        {
            eprintln!(
                "morsel random access: strategy={}, nodes={}, morsels={}, nodes_per_morsel={}, target_threads={}, actual_threads={}",
                if use_exact { "exact" } else { sparse_strategy },
                plan.len(),
                selected_morsels.len(),
                nodes_per_morsel,
                target_threads,
                threads,
            );
        }
        if !reuse_plan
            || state
                .as_ref()
                .is_some_and(|state| state.executor.threads() != threads)
        {
            *state = Some(MorselRandomAccessState {
                executor: MorselExecutor::shared(Arc::clone(&plan), threads)?,
                plan: Arc::clone(&plan),
            });
        }
        let observe =
            std::env::var("VORTEX_RANDOM_ACCESS_MORSEL_OBSERVE").is_ok_and(|value| value == "1");
        let scan = MorselScan::new(Arc::clone(&plan), SESSION.clone())
            .with_morsel_demands(selected_morsels)?
            .with_observability(observe);
        let scan = SegmentSourceDriver::new(self.file.segment_source())
            .connect(scan, &SESSION.handle())?;
        let (batches, stats) = state
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("morsel executor was not initialized"))?
            .executor
            .run(&scan)?;
        if std::env::var("VORTEX_RANDOM_ACCESS_MORSEL_DIAGNOSTICS").is_ok_and(|value| value == "1")
        {
            eprintln!(
                "morsel random access stats: morsels={}, io_uses={}, io_requests={}, io_bytes={}, decodes={}, decode_reuses={}, execute_io_blocks={}",
                stats.morsels,
                stats.io_uses,
                stats.io_requests,
                stats.io_bytes,
                stats.decodes,
                stats.decode_reuses,
                stats.execute_io_blocks,
            );
        }
        let array = match batches.len() {
            0 => Canonical::empty(plan.output_dtype()).into_array(),
            1 => batches
                .into_iter()
                .next()
                .ok_or_else(|| anyhow::anyhow!("morsel scan returned no batch"))?,
            _ => ChunkedArray::try_new(batches, plan.output_dtype().clone())?.into_array(),
        };

        let mut ctx = SESSION.create_execution_ctx();
        let canonical = array.execute::<Canonical>(&mut ctx)?.into_array();
        Ok(RandomAccessorRet::ArrayRef(canonical))
    }
}

/// Random accessor for Parquet format files.
///
/// Parquet footer and row group offsets are parsed at construction time and
/// reused to map indices to row groups in each `take()` call.
pub struct ParquetRandomAccessor {
    name: String,
    /// Cumulative row offsets per row group (length = num_row_groups + 1).
    row_group_offsets: Vec<i64>,
    /// Cached Arrow reader metadata (footer) to avoid re-parsing on each take.
    arrow_metadata: ArrowReaderMetadata,
    /// Path to the Parquet file (for re-opening on each take).
    path: PathBuf,
}

impl ParquetRandomAccessor {
    /// Open a Parquet file, parse the footer, and return a ready-to-use accessor.
    pub async fn open(path: PathBuf, name: impl Into<String>) -> anyhow::Result<Self> {
        let mut file = File::open(&path).await?;
        let options = ArrowReaderOptions::new().with_page_index_policy(PageIndexPolicy::Required);
        let arrow_metadata = ArrowReaderMetadata::load_async(&mut file, options).await?;

        let row_group_offsets = once(0)
            .chain(
                arrow_metadata
                    .metadata()
                    .row_groups()
                    .iter()
                    .map(|rg| rg.num_rows()),
            )
            .scan(0i64, |acc, x| {
                *acc += x;
                Some(*acc)
            })
            .collect::<Vec<_>>();

        Ok(Self {
            name: name.into(),
            row_group_offsets,
            arrow_metadata,
            path,
        })
    }
}

#[async_trait]
impl RandomAccessor for ParquetRandomAccessor {
    fn format(&self) -> Format {
        Format::Parquet
    }

    fn name(&self) -> &str {
        &self.name
    }

    async fn take(&self, indices: &[u64]) -> anyhow::Result<RandomAccessorRet> {
        // Map indices to row groups.
        let mut row_groups = HashMap::new();
        for &idx in indices {
            let row_group_idx = self
                .row_group_offsets
                .binary_search(&(idx as i64))
                .unwrap_or_else(|e| e - 1);
            row_groups
                .entry(row_group_idx)
                .or_insert_with(Vec::new)
                .push((idx as i64) - self.row_group_offsets[row_group_idx]);
        }

        let sorted_row_group_keys = row_groups.keys().copied().sorted().collect_vec();
        let row_group_indices = sorted_row_group_keys
            .iter()
            .map(|i| row_groups[i].clone())
            .collect_vec();

        // Re-open the file but reuse cached metadata (avoids re-parsing the footer).
        let file = File::open(&self.path).await?;
        let builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(file, self.arrow_metadata.clone());

        let reader = builder
            .with_row_groups(sorted_row_group_keys)
            // FIXME(ngates): our indices code assumes the batch size == the row group sizes
            .with_batch_size(10_000_000)
            .build()?;

        let schema = Arc::clone(reader.schema());

        let batches = reader
            .enumerate()
            .map(|(idx, batch)| {
                let batch = batch.unwrap();
                let indices = PrimitiveArray::<Int64Type>::from(row_group_indices[idx].clone());
                take_record_batch(&batch, &indices).unwrap()
            })
            .collect::<Vec<_>>()
            .await;

        let result = concat_batches(&schema, &batches)?;
        Ok(RandomAccessorRet::RecordBatch(result))
    }
}

fn default_morsel_threads(
    nodes_per_morsel: usize,
    use_exact: bool,
    available_threads: usize,
) -> usize {
    if use_exact && nodes_per_morsel > 4 {
        1
    } else {
        available_threads
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use arrow_array::RecordBatch;
    use arrow_ipc::writer::FileWriter;
    use arrow_schema::DataType as ArrowDataType;
    use arrow_schema::Field;
    use arrow_schema::Schema;
    use vortex::array::arrays::DictArray;
    use vortex::array::arrays::PrimitiveArray as VortexPrimitiveArray;
    use vortex::array::arrays::StructArray;
    use vortex::array::arrays::VarBinViewArray;
    use vortex::array::dtype::Nullability;
    use vortex::file::WriteOptionsSessionExt;

    use super::*;

    #[test]
    fn sparse_morsels_use_all_available_workers() {
        assert_eq!(default_morsel_threads(15, false, 16), 16);
        assert_eq!(default_morsel_threads(15, false, 48), 48);
        assert_eq!(default_morsel_threads(15, true, 16), 1);
        assert_eq!(default_morsel_threads(4, true, 48), 48);
    }

    #[test]
    fn variable_lists_prefer_natural_boundaries() {
        let dtype = DType::List(Arc::new(DType::Null), Nullability::NonNullable);
        assert!(contains_variable_list(&dtype));
        assert!(!contains_variable_list(&DType::Null));
    }

    #[test]
    fn coalesces_adjacent_indices_into_morsels() -> anyhow::Result<()> {
        assert_eq!(
            index_morsels(&[1, 2, 3, 8, 10, 11], 12)?,
            vec![1..4, 8..9, 10..12]
        );
        Ok(())
    }

    #[test]
    fn splits_large_morsels_to_fill_workers() {
        assert_eq!(
            split_morsels_for_parallelism(vec![0..20, 100..120], 4),
            vec![0..10, 10..20, 100..110, 110..120]
        );
    }

    #[test]
    fn rejects_duplicate_or_unsorted_indices() {
        assert!(index_morsels(&[1, 1], 2).is_err());
        assert!(index_morsels(&[1, 0], 2).is_err());
    }

    #[test]
    fn groups_sparse_indices_with_demands() -> anyhow::Result<()> {
        let morsels = index_morsel_demands(&[1, 3, 12, 13], 20, &[0..10, 10..20])?;
        assert_eq!(morsels.len(), 2);
        assert_eq!(morsels[0].0, 1..4);
        assert_eq!(
            morsels[0]
                .1
                .values()
                .context("expected a sparse mask")?
                .indices(),
            &[0, 2]
        );
        assert_eq!(morsels[1].0, 12..14);
        assert!(morsels[1].1.all_true());
        Ok(())
    }

    #[test]
    fn groups_sparse_indices_by_fixed_rows() -> anyhow::Result<()> {
        let morsels = fixed_row_morsel_demands(&[1, 3, 12, 13], 20, 10, 1)?;
        assert_eq!(morsels.len(), 2);
        assert_eq!(morsels[0].0, 1..4);
        assert_eq!(morsels[1].0, 12..14);
        Ok(())
    }

    #[test]
    fn splits_nonempty_fixed_morsels_to_fill_workers() -> anyhow::Result<()> {
        let morsels = fixed_row_morsel_demands(&[1, 3, 5, 12, 13], 20, 10, 4)?;
        assert_eq!(morsels.len(), 4);
        assert_eq!(morsels[0].0, 1..2);
        assert_eq!(morsels[1].0, 3..6);
        assert_eq!(morsels[1].1.true_count(), 2);
        assert_eq!(morsels[2].0, 12..13);
        assert_eq!(morsels[3].0, 13..14);
        Ok(())
    }

    #[test]
    fn cannot_create_more_sparse_morsels_than_selected_rows() -> anyhow::Result<()> {
        let morsels = fixed_row_morsel_demands(&[1, 12], 20, 10, 16)?;
        assert_eq!(morsels.len(), 2);
        Ok(())
    }

    #[test]
    fn balances_selected_rows_across_morsels() -> anyhow::Result<()> {
        let morsels = balanced_index_morsel_demands(&[1, 3, 12, 13, 20], 21, 2)?;
        assert_eq!(morsels.len(), 2);
        assert_eq!(morsels[0].0, 1..13);
        assert_eq!(morsels[0].1.true_count(), 3);
        assert_eq!(morsels[1].0, 13..21);
        assert_eq!(morsels[1].1.true_count(), 2);
        Ok(())
    }

    #[test]
    fn partitions_the_complete_row_domain_evenly() -> anyhow::Result<()> {
        let morsels = row_partition_morsel_demands(&[1, 3, 12, 13, 20], 21, 2)?;
        assert_eq!(morsels.len(), 2);
        assert_eq!(morsels[0].0, 1..4);
        assert_eq!(morsels[1].0, 12..21);
        assert_eq!(morsels[1].1.true_count(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn morsel_random_accessor_takes_disjoint_rows() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let values = VortexPrimitiveArray::from_iter(0i64..8).into_array();
        let array = StructArray::from_fields(&[("id", values)])?.into_array();
        let mut output = File::create(file.path()).await?;
        SESSION
            .write_options()
            .write(&mut output, array.to_array_stream())
            .await?;
        drop(output);

        let accessor =
            VortexRandomAccessor::open(file.path(), "vortex-morsel-test", Format::OnDiskVortex)
                .await?;
        let RandomAccessorRet::ArrayRef(actual) = accessor.take_morsels(&[1, 2, 6])? else {
            anyhow::bail!("Vortex accessor returned a non-Vortex result")
        };
        let RandomAccessorRet::ArrayRef(v1) = accessor.take(&[1, 2, 6]).await? else {
            anyhow::bail!("Vortex V1 accessor returned a non-Vortex result")
        };
        let expected = StructArray::from_fields(&[(
            "id",
            VortexPrimitiveArray::from_iter([1i64, 2, 6]).into_array(),
        )])?
        .into_array();
        let mut ctx = SESSION.create_execution_ctx();
        vortex_array::assert_arrays_eq!(actual, v1, &mut ctx);
        vortex_array::assert_arrays_eq!(actual, expected, &mut ctx);
        Ok(())
    }

    #[tokio::test]
    async fn reopened_morsel_accessor_reuses_plan_and_executor() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let values = VortexPrimitiveArray::from_iter(0i64..8).into_array();
        let array = StructArray::from_fields(&[("id", values)])?.into_array();
        let mut output = File::create(file.path()).await?;
        SESSION
            .write_options()
            .write(&mut output, array.to_array_stream())
            .await?;
        drop(output);

        let first = VortexRandomAccessor::open(file.path(), "first", Format::OnDiskVortex).await?;
        first.take_morsels(&[1, 2, 6])?;
        let first_plan = Arc::clone(
            &first
                .morsel_cache
                .state
                .lock()
                .as_ref()
                .context("first accessor did not initialize a morsel plan")?
                .plan,
        );

        let reopened =
            VortexRandomAccessor::open(file.path(), "reopened", Format::OnDiskVortex).await?;
        assert!(Arc::ptr_eq(&first.morsel_cache, &reopened.morsel_cache));
        reopened.take_morsels(&[1, 2, 6])?;
        let reopened_plan = Arc::clone(
            &reopened
                .morsel_cache
                .state
                .lock()
                .as_ref()
                .context("reopened accessor did not retain the morsel plan")?
                .plan,
        );
        assert!(Arc::ptr_eq(&first_plan, &reopened_plan));
        Ok(())
    }

    #[tokio::test]
    async fn morsel_random_accessor_takes_dictionary_rows() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let codes = VortexPrimitiveArray::from_iter([0u8, 1, 0, 1, 1]).into_array();
        let values = VarBinViewArray::from_iter_str(["red", "blue"]).into_array();
        let dictionary = DictArray::new(codes, values).into_array();
        let array = StructArray::from_fields(&[("color", dictionary)])?.into_array();
        let mut output = File::create(file.path()).await?;
        SESSION
            .write_options()
            .write(&mut output, array.to_array_stream())
            .await?;
        drop(output);

        let accessor = VortexRandomAccessor::open(
            file.path(),
            "vortex-morsel-dict-test",
            Format::OnDiskVortex,
        )
        .await?;
        let RandomAccessorRet::ArrayRef(actual) = accessor.take_morsels(&[1, 4])? else {
            anyhow::bail!("Vortex accessor returned a non-Vortex result")
        };
        let expected = StructArray::from_fields(&[(
            "color",
            VarBinViewArray::from_iter_str(["blue", "blue"]).into_array(),
        )])?
        .into_array();
        let mut ctx = SESSION.create_execution_ctx();
        vortex_array::assert_arrays_eq!(actual, expected, &mut ctx);
        Ok(())
    }

    #[tokio::test]
    async fn morsel_random_accessor_takes_nested_struct_rows() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let nested = StructArray::from_fields(&[
            (
                "x",
                VortexPrimitiveArray::from_iter([10i32, 20, 30, 40]).into_array(),
            ),
            (
                "y",
                VortexPrimitiveArray::from_iter([1i64, 2, 3, 4]).into_array(),
            ),
        ])?
        .into_array();
        let array = StructArray::from_fields(&[("nested", nested)])?.into_array();
        let mut output = File::create(file.path()).await?;
        SESSION
            .write_options()
            .write(&mut output, array.to_array_stream())
            .await?;
        drop(output);

        let accessor = VortexRandomAccessor::open(
            file.path(),
            "vortex-morsel-nested-struct-test",
            Format::OnDiskVortex,
        )
        .await?;
        let RandomAccessorRet::ArrayRef(actual) = accessor.take_morsels(&[0, 3])? else {
            anyhow::bail!("Vortex accessor returned a non-Vortex result")
        };
        let expected_nested = StructArray::from_fields(&[
            (
                "x",
                VortexPrimitiveArray::from_iter([10i32, 40]).into_array(),
            ),
            ("y", VortexPrimitiveArray::from_iter([1i64, 4]).into_array()),
        ])?
        .into_array();
        let expected = StructArray::from_fields(&[("nested", expected_nested)])?.into_array();
        let mut ctx = SESSION.create_execution_ctx();
        vortex_array::assert_arrays_eq!(actual, expected, &mut ctx);
        Ok(())
    }

    #[tokio::test]
    async fn arrow_ipc_random_accessor_takes_rows_across_record_batches() -> anyhow::Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "id",
            ArrowDataType::Int64,
            false,
        )]));
        {
            let mut writer = FileWriter::try_new(file.reopen()?, schema.as_ref())?;
            writer.write(&RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![0, 1]))],
            )?)?;
            writer.write(&RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![2, 3, 4]))],
            )?)?;
            writer.write_metadata(ARROW_ROW_OFFSETS_METADATA_KEY, "0,2,5");
            writer.finish()?;
        }

        let accessor = ArrowIpcRandomAccessor::open(file.path().to_path_buf(), "arrow-ipc")?;
        let RandomAccessorRet::RecordBatch(actual) = accessor.take(&[1, 3, 4]).await? else {
            anyhow::bail!("Arrow accessor returned a Vortex array")
        };
        let expected =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 3, 4]))])?;
        assert_eq!(actual, expected);
        Ok(())
    }
}
