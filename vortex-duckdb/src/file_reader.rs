// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use futures::FutureExt;
use memmap2::Mmap;
use object_store::registry::ObjectStoreRegistry;
use url::Url;
use vortex::array::VortexSessionExecute as _;
use vortex::array::arrays::struct_::StructArrayExt as _;
use vortex::buffer::ByteBuffer;
use vortex::dtype::DType;
use vortex::error::VortexExpect;
use vortex::error::VortexResult;
use vortex::error::vortex_err;
use vortex::error::vortex_panic;
use vortex::file::Footer;
use vortex::file::VortexFile;
use vortex::file::multi::MultiFileSession;
use vortex::file::multi::open_cached;
use vortex::file::multi::parse_uri_or_path;
use vortex::file::v2::FileStatsLayoutReader;
use vortex::io::compat::Compat;
use vortex::io::filesystem::FileSystemRef;
use vortex::io::object_store::ObjectStoreFileSystem;
use vortex::io::runtime::BlockingRuntime as _;
use vortex::io::std_file::StdFileSystem;
use vortex::layout::LayoutReaderRef;
use vortex::layout::scan::scan_builder::ScanBuilder;
use vortex::mask::Mask;
use vortex::session::SessionExt as _;

use crate::REGISTRY;
use crate::RUNTIME;
use crate::SESSION;
use crate::column_statistics::ColumnStatistics;
use crate::column_statistics::ColumnStatisticsAggregate;
use crate::duckdb::BindResultRef;
use crate::duckdb::DataChunkRef;
use crate::exporter::ArrayExporter;
use crate::exporter::ConversionCache;
use crate::projection::Filter;
use crate::projection::extract_schema_from_dtype;
use crate::scan_order::order_splits;
use crate::table_function::BindState;
use crate::table_function::GlobalState;
use crate::table_function::LocalState;
use crate::table_function::Split;
use crate::table_function::convert_result;

// See src/table_function.rs for definition of table function state machine.
// Duckdb's inner state machine, called by table function state machine, is
// file reader state machine. It opens Vortex files, reads their contents
// and populates output data chunks with data from these files.
//
// Definitions:
// "global lock" - lock over all threads. Only one thread may access some
//   section.
// "file-local lock" - multiple threads may access different File's in
//   parallel, but only one thread can access a single File at a time.
//
// First there's bind phase in planning, called by one thread on first
// file expanded from glob, to get scan schema and estimate overall scan
// cardinality.
//
// `reader_open` -> `reader_bind` -> `reader_get_statistics`
//
// After this, still in planning phase, one thread calls the following chain on
// all files to try and replace scanning with metadata answer:
//
// `can_get_partition_stats` -> `footer_open` -> `footer_get_statistics`.
//
// Then there's query runtime phase, called for all files in scan:
//
// `reader_open` -> `reader_initialize` ->
// `reader_try_initialize_scan` -> `reader_scan`
//
// `reader_get_progress_in_file` is called during `reader_scan` calls from a
// separate thread.

fn resolve_filesystem(url: &Url) -> VortexResult<(FileSystemRef, String)> {
    // Compat makes us use tokio which is very bad for local reads on
    // high-core machines because reads go into blocking pool
    if url.scheme() == "file" {
        return Ok((
            Arc::new(StdFileSystem::new(RUNTIME.handle())),
            url.path().to_string(),
        ));
    }

    let (object_store, path) = REGISTRY.resolve(url)?;

    Ok((
        Arc::new(ObjectStoreFileSystem::new(
            Arc::new(Compat::new(object_store)),
            RUNTIME.handle(),
        )),
        path.to_string(),
    ))
}

pub struct OpenFileReader {
    /// The layout reader and conversion cache of the file. `None` after the file is skipped or
    /// all its splits are taken.
    ///
    /// DuckDB releases its last reference to a file while it holds the global lock of the
    /// multi-file scan, and dropping this state takes long. So every local state that scans the
    /// file holds a reference too, and the file gives its own reference to a local state before
    /// DuckDB releases the file. The last local state drops the state outside the lock.
    scan_state: Option<Arc<FileScanState>>,
    /// File splits stored in inverse order
    pub splits: Vec<Split>,
    total_splits: usize,
}

/// The state of an open file that the threads scanning it share.
pub struct FileScanState {
    reader: LayoutReaderRef,
    file: VortexFile,
    cache: ConversionCache,
}

impl OpenFileReader {
    /// Releases a skipped file's scan state while only its file-local lock is held.
    pub(crate) fn discard(&mut self) {
        self.scan_state = None;
        self.splits.clear();
    }

    async fn open(path: String) -> VortexResult<Self> {
        let url = parse_uri_or_path(&path)?;
        let file = if url.scheme() == "file"
            && std::env::var_os("VORTEX_DUCKDB_MEMORY_MAP").is_none_or(|value| value != "0")
        {
            open_memory_mapped(&url, &path).await?
        } else {
            let (fs, fs_path) = resolve_filesystem(&url)?;
            let source = fs.open_read(&fs_path).await?;
            open_cached(&SESSION, Some(&path), source, None, &|options| options).await?
        };
        Ok(OpenFileReader {
            scan_state: Some(Arc::new(FileScanState {
                reader: file.layout_reader()?,
                file,
                cache: ConversionCache::default(),
            })),
            splits: vec![],
            total_splits: 0,
        })
    }

    fn reader(&self) -> &LayoutReaderRef {
        &self
            .scan_state
            .as_ref()
            .vortex_expect("file used after its scan state was released")
            .reader
    }

    fn can_skip(&self, filter: &Filter) -> VortexResult<bool> {
        let Some(filter) = &filter.filter else {
            return Ok(false);
        };
        let row_count = self.reader().row_count();
        let row_range = 0..row_count;
        let mask = Mask::new_true(usize::try_from(row_count).unwrap_or(usize::MAX));
        let evaluation = self.reader().pruning_evaluation(&row_range, filter, mask)?;
        match evaluation.now_or_never() {
            Some(mask) => mask.map(|mask| mask.all_false()),
            None => Ok(false),
        }
    }
}

async fn open_memory_mapped(url: &Url, key: &str) -> VortexResult<VortexFile> {
    let path = url
        .to_file_path()
        .map_err(|()| vortex_err!("Invalid local file URL: {url}"))?;
    let file = std::fs::File::open(path)?;
    // SAFETY: Vortex files are immutable once written. The buffer owns the mapping, so every
    // segment slice keeps it alive even after DuckDB releases its file-local scan state.
    let buffer = ByteBuffer::from(unsafe { Mmap::map(&file)? });
    let size = buffer.len() as u64;
    open_cached(
        &SESSION,
        Some(key),
        Arc::new(buffer),
        Some(size),
        &|options| options,
    )
    .await
}

/// Called once per file while initializing the scan under file-local lock.
/// Files are opened lazily.
pub fn reader_open(file_path: &str) -> VortexResult<OpenFileReader> {
    RUNTIME.block_on(OpenFileReader::open(file_path.to_owned()))
}

/// Called once per scan with first file without locks. Populates "result"
/// with first file schema which is the scan schema. Unlike Parquet, we don't
/// support schema evolution, so if any file schema doesn't match first schema,
/// we break.
pub fn reader_bind(file: &OpenFileReader, result: &mut BindResultRef) -> VortexResult<BindState> {
    let dtype = file.reader().dtype().clone();
    let columns = extract_schema_from_dtype(&dtype)?;

    for column in &columns {
        result.add_result_column(&column.name, &column.logical_type);
    }

    Ok(BindState {
        dtype,
        first_file_row_count: file.reader().row_count(),
        filters: vec![],
        columns,
        has_non_optional_filter: AtomicBool::new(false),
        aggregates: vec![],
        no_footer_caches: false,
        scan_order: None,
    })
}

/// Called once per file by one thread under file-local lock. Determines
/// whether the opened file should be skipped. If this function returns false,
/// duckdb closes the file and doesn't call reader_try_initialize_scan on it.
pub fn reader_initialize(file: &mut OpenFileReader, global: &GlobalState) -> VortexResult<bool> {
    if file.can_skip(&global.filter)? {
        // DuckDB releases a skipped file under the global lock. Here only the
        // file-local lock is held, so drop the scan state now.
        file.scan_state = None;
        return Ok(true);
    }

    // Getting splits is non-trivial work so we prefer doing it here under file
    // lock and not in reader_try_initialize_scan under global lock.
    let reader = Arc::clone(file.reader());
    let filter = &global.filter;
    let builder = ScanBuilder::new(SESSION.clone(), reader)
        .with_projection(global.projection.clone())
        .with_some_filter(filter.filter.clone())
        .with_selection(filter.row_selection.clone());
    let scan = builder.prepare()?;
    let mut splits = scan.execute(filter.row_range.clone())?;
    if let Some(order) = &global.scan_order {
        let file_state = file
            .scan_state
            .as_ref()
            .vortex_expect("open file has scan state");
        RUNTIME.block_on(order_splits(
            &file_state.file,
            order,
            scan.split_ranges(filter.row_range.clone()),
            &mut splits,
        ))?;
    }

    // threads take last element of file.splits so we need to reverse
    splits.reverse();
    file.total_splits = splits.len();
    file.splits = splits;
    Ok(false)
}

/// Called by all threads under global lock. If this function returns true,
/// thread calls reader_scan on this file. If this function returns false,
/// duckdb thinks file is exhausted, closes the file, and the first thread to
/// get "false" switches to next file.
///
/// This function must not drop a file's scan state, because the caller holds
/// the global lock. It only moves references between the file and the local
/// state, and reader_scan drops the retired ones.
pub fn reader_try_initialize_scan(file: &mut OpenFileReader, local: &mut LocalState) -> bool {
    let Some(split) = file.splits.pop() else {
        // DuckDB releases the file after this call, under the global lock.
        // Give the reference of the file to this thread so it drops outside.
        local.retired_files.extend(file.scan_state.take());
        return false;
    };

    let scan_state = file
        .scan_state
        .as_ref()
        .vortex_expect("file has splits but no scan state");
    if !local
        .file
        .as_ref()
        .is_some_and(|current| Arc::ptr_eq(current, scan_state))
    {
        let previous = local.file.replace(Arc::clone(scan_state));
        local.retired_files.extend(previous);
    }

    local.split = Some(split);
    true
}

/// Called by all threads operating on a file without locks. If this function
/// returns false, duckdb closes the file, and first thread to get "false"
/// switches to next file.
pub fn reader_scan(
    global: &GlobalState,
    local: &mut LocalState,
    chunk: &mut DataChunkRef,
) -> VortexResult<bool> {
    // No lock is held here, so drop the scan states of files this thread left.
    local.retired_files.clear();

    if !global.aggregates.is_empty() {
        return reader_scan_aggregate(global, local);
    }

    if local.exporter.is_none() {
        let Some(split) = local.split.take() else {
            return Ok(false);
        };
        let Some(array) = RUNTIME.block_on(split)? else {
            // split is filtered
            return Ok(true);
        };
        let mut ctx = SESSION.create_execution_ctx();
        let array = convert_result(array, &mut ctx)?;
        let file = local.file.as_ref().vortex_expect("split without a file");
        local.exporter = Some(ArrayExporter::try_new(&array, &file.cache, ctx)?);
    }
    let exporter = local.exporter.as_mut().vortex_expect("no exporter");

    let has_more_data = exporter.export(chunk, global.file_row_number_column_pos)?;
    if !has_more_data {
        local.exporter = None;
    }
    Ok(true)
}

fn reader_scan_aggregate(global: &GlobalState, local: &mut LocalState) -> VortexResult<bool> {
    let Some(split) = local.split.take() else {
        return Ok(false);
    };
    let Some(array) = RUNTIME.block_on(split)? else {
        // split is filtered
        return Ok(true);
    };

    let mut ctx = SESSION.create_execution_ctx();
    let array = convert_result(array, &mut ctx)?;

    for (position, partial) in local.partials.iter_mut() {
        partial.accumulate(array.unmasked_field(*position), &mut ctx)?;
    }

    if global.has_count_star {
        let len = array.len() as u64;
        global.row_count.fetch_add(len, Ordering::Relaxed);
    }

    Ok(true)
}

/// Called by one thread in plan phase without locks only on the first file
/// after calling reader_open and reader_bind on it.
pub fn reader_get_statistics(
    file: &OpenFileReader,
    bind: &BindState,
    column: &str,
) -> Option<ColumnStatistics> {
    if !bind.aggregates.is_empty() {
        return None;
    }

    let reader = file
        .reader()
        .as_any()
        .downcast_ref::<FileStatsLayoutReader>()?;

    let DType::Struct(fields, _) = &file.reader().dtype() else {
        return None;
    };
    let index = fields.find(column)?;
    let stats_sets = reader.file_stats().stats_sets();

    let dtype = fields.field_by_index(index)?;

    let mut stats =
        ColumnStatisticsAggregate::new(stats_sets.get(index)?, &dtype, file.reader().row_count());
    if bind
        .columns
        .get(index)
        .is_some_and(|column| column.projection_expr.is_some())
    {
        stats.distinct_count = None;
    }
    match ColumnStatistics::try_from(stats, dtype) {
        Ok(stats) => Some(stats),
        Err(e) => vortex_panic!(e),
    }
}

/// Called from a separate thread (not related to threads for Vortex
/// table function) under global lock.
pub fn reader_get_progress_in_file(file: &OpenFileReader) -> f64 {
    let total = file.total_splits;
    let left = file.splits.len();
    let denom = total + (total == 0) as usize;
    100.0 * (total - left) as f64 / denom as f64
}

/// Called by one thread in planning phase
pub fn can_get_partition_stats(bind: &BindState) -> bool {
    // Re-reading footers on every request is costly so we mimic DuckDB's
    // TryLoadCaches for Parquet and load them once per file
    !bind.no_footer_caches
    // This function is called during planning where we haven't read data yet.
    // If there's a filter, we need to read the data to evaluate stats.
    // Even if we could do this fast, we report some filters here as not pushed
    // (see table_function.rs) and stats would be incorrect.
        && bind.filters.is_empty()
        && bind.columns.iter().all(|column| column.projection_expr.is_none())
}

/// Called by one thread for every file. When we open files, we cache footers
/// in MultiFileSession so this function retrieves the cached data if present.
/// If any footer is not present, it sets a flag in BindState so we won't try
/// again.
pub fn footer_get_cached(bind: &mut BindState, path: &str) -> VortexResult<Option<Footer>> {
    let footer = SESSION
        .get_opt::<MultiFileSession>()
        .vortex_expect("MultiFileSession not found")
        .get_footer(path);
    bind.no_footer_caches |= footer.is_none();
    Ok(footer)
}

/// Called by one thread for every footer in planning phase
pub fn footer_get_statistics(footer: &Footer, index: usize) -> Option<ColumnStatistics> {
    let DType::Struct(fields, _) = footer.dtype() else {
        return None;
    };
    let stats = footer.statistics()?;
    let dtype = fields.field_by_index(index)?;
    let stats = stats.stats_sets().get(index)?;
    let stats = ColumnStatisticsAggregate::new(stats, &dtype, footer.row_count());
    match ColumnStatistics::try_from(stats, dtype) {
        Ok(stats) => Some(stats),
        Err(e) => vortex_panic!(e),
    }
}

#[cfg(test)]
mod tests {
    use url::Url;
    use vortex::array::IntoArray;
    use vortex::array::VortexSessionExecute;
    use vortex::array::arrays::PrimitiveArray;
    use vortex::array::assert_arrays_eq;
    use vortex::array::stream::ArrayStreamExt;
    use vortex::error::VortexResult;
    use vortex::error::vortex_err;
    use vortex::file::WriteOptionsSessionExt;
    use vortex::io::runtime::BlockingRuntime;

    use super::open_memory_mapped;
    use crate::RUNTIME;
    use crate::SESSION;
    use crate::duckdb::Database;

    #[test]
    fn mapped_arrays_outlive_the_file_reader() -> VortexResult<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("space % 日本語.vortex");
        let expected = PrimitiveArray::from_iter(0i64..10_000).into_array();
        let url = Url::from_file_path(&path)
            .map_err(|()| vortex_err!("temporary path is not absolute"))?;
        let actual = RUNTIME.block_on(async {
            let mut writer = async_fs::File::create(&path).await?;
            SESSION
                .write_options()
                .write(&mut writer, expected.clone().to_array_stream())
                .await?;
            let mapped = open_memory_mapped(&url, url.as_str()).await?;
            let array = mapped.scan()?.into_array_stream()?.read_all().await?;
            drop(mapped);
            VortexResult::Ok(array)
        })?;
        assert_arrays_eq!(actual, expected, &mut SESSION.create_execution_ctx());
        Ok(())
    }

    #[test]
    fn local_scans_reopen_paths_with_reserved_characters() -> VortexResult<()> {
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("space % 日本語.vortex");
        let db = Database::open_in_memory()?;
        crate::initialize(&db)?;
        let connection = db.connect()?;
        connection.query(&format!(
            "COPY (SELECT i FROM range(10000) t(i)) TO '{}' (FORMAT vortex)",
            path.display()
        ))?;
        let path = path.to_string_lossy();
        for _ in 0..3 {
            let result = connection.query(&format!(
                "SELECT sum(i)::BIGINT FROM read_vortex('{path}') WHERE i % 2 = 0"
            ))?;
            let chunk = result
                .into_iter()
                .next()
                .ok_or_else(|| vortex_err!("missing sum result"))?;
            assert_eq!(
                chunk.get_vector(0).as_slice_with_len::<i64>(1),
                &[24_995_000]
            );
        }
        Ok(())
    }
}
