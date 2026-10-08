// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::cmp;
use std::iter;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::task::ready;

use futures::Future;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::future;
use futures::future::BoxFuture;
use futures::stream;
use futures::stream::BoxStream;
use itertools::Either;
use itertools::Itertools;
use vortex_array::ArrayRef;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldMask;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::analysis::referenced_field_paths;
use vortex_array::expr::transform::find_between;
use vortex_array::iter::ArrayIterator;
use vortex_array::iter::ArrayIteratorAdapter;
use vortex_array::stream::ArrayStream;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_io::runtime::BlockingRuntime;
use vortex_io::runtime::Task;
use vortex_io::session::RuntimeSessionExt;
use vortex_metrics::MetricsRegistry;
use vortex_scan::row_mask::RowMask;
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_session::VortexSession;
use vortex_utils::parallelism::get_available_parallelism;

use crate::LayoutReader;
use crate::LayoutReaderRef;
use crate::layouts::row_idx::RowIdx;
use crate::layouts::row_idx::RowIdxLayoutReader;
use crate::scan::filter::FilterExpr;
use crate::scan::limit::RowLimit;
use crate::scan::limit::ScanLimit;
use crate::scan::split_by::SplitBy;
use crate::scan::splits::Splits;
use crate::scan::splits::attempt_split_ranges;
use crate::scan::tasks::SplitFuture;
use crate::scan::tasks::TaskContext;
use crate::scan::tasks::split_exec;
use crate::scan::tasks::split_projection;

/// Builder for scanning a [`LayoutReader`] into arrays, streams, iterators, or mapped outputs.
///
/// A scan has three independent row restriction mechanisms:
///
/// - [`with_row_range`](Self::with_row_range) selects a contiguous range before scanning.
/// - [`with_selection`](Self::with_selection) applies a [`Selection`] inside that range.
/// - [`with_filter`](Self::with_filter) evaluates an expression predicate during execution.
///
/// Projection and filter expressions must be bound against the reader dtype. Work is divided by
/// the configured [`SplitBy`] strategy or by explicit selection ranges.
pub struct ScanBuilder<A> {
    session: VortexSession,
    layout_reader: LayoutReaderRef,
    projection: BoundExpression,
    filter: Option<BoundExpression>,
    /// Whether the scan needs to return splits in the order they appear in the file.
    ordered: bool,
    /// Optionally read a subset of the rows in the file.
    row_range: Option<Range<u64>>,
    /// The selection mask to apply to the selected row range.
    // TODO(joe): replace this is usage of row_id selection, see
    selection: Selection,
    /// How to split the file for concurrent processing.
    split_by: SplitBy,
    /// Precomputed full-file natural split boundaries; when set, [`build`](Self::build)
    /// uses them instead of walking the layout.
    natural_splits: Option<Arc<[u64]>>,
    /// The number of splits to make progress on concurrently **per-thread**.
    concurrency: usize,
    /// Function to apply to each [`ArrayRef`] within the spawned split tasks.
    map_fn: MapFn<A>,
    metrics_registry: Option<Arc<dyn MetricsRegistry>>,
    /// Maximal number of rows to read after filtering.
    limit: Option<ScanLimit>,
    /// The row-offset assigned to the first row of the file. Used by the `row_idx` expression,
    /// but not by the scan [`Selection`] which remains relative.
    row_offset: u64,
}

impl ScanBuilder<ArrayRef> {
    /// Create a scan builder over `layout_reader` using `session` for runtime and execution state.
    pub fn new(session: VortexSession, layout_reader: Arc<dyn LayoutReader>) -> Self {
        let projection = BoundExpression::new_root(layout_reader.dtype().clone());
        Self {
            session,
            layout_reader,
            projection,
            filter: None,
            ordered: true,
            row_range: None,
            selection: Default::default(),
            split_by: SplitBy::default(),
            natural_splits: None,
            // We default to four tasks per worker thread, which allows for some I/O lookahead
            // without too much impact on work-stealing.
            concurrency: 4,
            map_fn: Arc::new(Ok),
            metrics_registry: None,
            limit: None,
            row_offset: 0,
        }
    }

    /// Returns an [`ArrayStream`] with tasks spawned onto the session's runtime handle.
    ///
    /// See [`ScanBuilder::into_stream`] for more details.
    pub fn into_array_stream(self) -> VortexResult<impl ArrayStream + Send + 'static> {
        let dtype = self.dtype()?;
        let stream = self.into_stream()?;
        Ok(ArrayStreamAdapter::new(dtype, stream))
    }

    /// Returns an [`ArrayIterator`] using the given blocking runtime.
    pub fn into_array_iter<B: BlockingRuntime>(
        self,
        runtime: &B,
    ) -> VortexResult<impl ArrayIterator + 'static> {
        let stream = self.into_array_stream()?;
        let dtype = stream.dtype().clone();
        Ok(ArrayIteratorAdapter::new(
            dtype,
            runtime.block_on_stream(stream),
        ))
    }
}

impl<A: 'static + Send> ScanBuilder<A> {
    /// Add a filter expression bound against the reader dtype.
    pub fn with_filter(mut self, filter: BoundExpression) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Add or clear a filter expression bound against the reader dtype.
    pub fn with_some_filter(mut self, filter: Option<BoundExpression>) -> Self {
        self.filter = filter;
        self
    }

    /// Set a projection expression bound against the reader dtype.
    pub fn with_projection(mut self, projection: BoundExpression) -> Self {
        self.projection = projection;
        self
    }

    /// Returns whether output chunks are yielded in file order.
    pub fn ordered(&self) -> bool {
        self.ordered
    }

    /// Configure whether output chunks must be yielded in file order.
    pub fn with_ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    /// Restrict scanning to a contiguous row range.
    pub fn with_row_range(mut self, row_range: Range<u64>) -> Self {
        self.row_range = Some(row_range);
        self
    }

    /// Apply a row selection to the selected row range.
    pub fn with_selection(mut self, selection: Selection) -> Self {
        self.selection = selection;
        self
    }

    /// Select rows by strictly sorted absolute indices relative to the scan input.
    pub fn with_row_indices(mut self, row_indices: StrictSortedBuffer<u64>) -> Self {
        self.selection = Selection::IncludeByIndex(row_indices);
        self
    }

    /// Set the root row offset used by row-index expressions.
    pub fn with_row_offset(mut self, row_offset: u64) -> Self {
        self.row_offset = row_offset;
        self
    }

    /// Configure how natural scan work is split for concurrency.
    pub fn with_split_by(mut self, split_by: SplitBy) -> Self {
        self.split_by = split_by;
        self
    }

    /// Supply precomputed full-file natural split boundaries (see
    /// [`full_file_splits`](Self::full_file_splits)) so [`build`](Self::build) reuses them
    /// instead of walking the layout. Callers translating external partitions into row ranges
    /// can compute the boundaries once per file and share them across partitions.
    ///
    /// Takes precedence over [`with_split_by`](Self::with_split_by); boundaries outside the
    /// scan's row range are clamped during execution. Boundaries must be strictly increasing.
    pub fn with_natural_splits(mut self, boundaries: Arc<[u64]>) -> Self {
        debug_assert!(
            boundaries.windows(2).all(|w| w[0] < w[1]),
            "natural split boundaries must be strictly increasing"
        );
        self.natural_splits = Some(boundaries);
        self
    }

    /// Compute the full-file natural split boundaries for the fields referenced by this scan's
    /// projection and filter, ignoring any configured row range.
    ///
    /// These are the boundaries [`build`](Self::build) derives for a whole-file scan; hand
    /// them back via [`with_natural_splits`](Self::with_natural_splits) to skip the layout walk
    /// when building.
    pub fn full_file_splits(&self) -> VortexResult<Vec<u64>> {
        let field_mask = referenced_field_masks(&self.projection, self.filter.as_ref())?;
        self.split_by.splits(
            self.layout_reader.as_ref(),
            &(0..self.layout_reader.row_count()),
            &field_mask,
        )
    }

    /// Returns the per-worker row-split concurrency.
    pub fn concurrency(&self) -> usize {
        self.concurrency
    }

    /// The number of row splits to make progress on concurrently per-thread, must
    /// be greater than 0.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0);
        self.concurrency = concurrency;
        self
    }

    /// Add or clear the metrics registry used by scan execution.
    pub fn with_some_metrics_registry(mut self, metrics: Option<Arc<dyn MetricsRegistry>>) -> Self {
        self.metrics_registry = metrics;
        self
    }

    /// Set the metrics registry used by scan execution.
    pub fn with_metrics_registry(mut self, metrics: Arc<dyn MetricsRegistry>) -> Self {
        self.metrics_registry = Some(metrics);
        self
    }

    /// Add or clear the maximum number of rows returned after filtering.
    pub fn with_some_limit(mut self, limit: Option<u64>) -> Self {
        self.limit = limit.map(ScanLimit::PerExecution);
        self
    }

    /// Set the maximum number of rows returned after filtering.
    pub fn with_limit(self, limit: u64) -> Self {
        self.with_some_limit(Some(limit))
    }

    /// Draw rows from a budget shared with sibling partitions instead of a limit of its own.
    pub(crate) fn with_shared_limit(mut self, limit: RowLimit) -> Self {
        self.limit = Some(ScanLimit::Shared(limit));
        self
    }

    /// The [`DType`] returned by the scan, after applying the projection.
    pub fn dtype(&self) -> VortexResult<DType> {
        Ok(self.projection.dtype().clone())
    }

    /// The session used by the scan.
    pub fn session(&self) -> &VortexSession {
        &self.session
    }

    /// Map each split of the scan. The function will be run on the spawned task.
    pub fn map<B: 'static>(
        self,
        map_fn: impl Fn(A) -> VortexResult<B> + 'static + Send + Sync,
    ) -> ScanBuilder<B> {
        let old_map_fn = self.map_fn;
        ScanBuilder {
            session: self.session,
            layout_reader: self.layout_reader,
            projection: self.projection,
            filter: self.filter,
            ordered: self.ordered,
            row_range: self.row_range,
            selection: self.selection,
            split_by: self.split_by,
            natural_splits: self.natural_splits,
            concurrency: self.concurrency,
            metrics_registry: self.metrics_registry,
            limit: self.limit,
            row_offset: self.row_offset,
            map_fn: Arc::new(move |a| old_map_fn(a).and_then(&map_fn)),
        }
    }

    /// The context shared by the scan's split tasks, and the row ranges of its splits.
    ///
    /// Range comparisons in the filter are rewritten into betweens.
    fn split_plan(&self) -> VortexResult<(Arc<TaskContext>, Vec<Range<u64>>)> {
        let mut layout_reader = Arc::clone(&self.layout_reader);

        // Enrich the layout reader to support RowIdx expressions if scan uses #row_idx.
        // Note that this is applied below the filter layout reader since it can perform
        // better over individual conjunctions.
        let mut found_row_idx = self.projection.contains::<RowIdx>()?;
        if !found_row_idx && let Some(filter) = self.filter.as_ref() {
            found_row_idx = filter.contains::<RowIdx>()?;
        }
        if found_row_idx {
            layout_reader = Arc::new(RowIdxLayoutReader::new(
                self.row_offset,
                layout_reader,
                self.session.clone(),
            ));
        }

        let filter = self.filter.clone().map(find_between).transpose()?;

        let splits =
            if let Some(ranges) = attempt_split_ranges(&self.selection, self.row_range.as_ref()) {
                Splits::Ranges(ranges)
            } else if let Some(boundaries) = &self.natural_splits {
                // Caller-supplied full-file boundaries; they are clamped to the row range below.
                Splits::Natural(Arc::clone(boundaries))
            } else {
                let field_mask = referenced_field_masks(&self.projection, filter.as_ref())?;
                let split_range = self
                    .row_range
                    .clone()
                    .unwrap_or_else(|| 0..layout_reader.row_count());
                Splits::Natural(
                    self.split_by
                        .splits(layout_reader.as_ref(), &split_range, &field_mask)?
                        .into(),
                )
            };
        let row_range = intersect_ranges(self.row_range.as_ref(), selection_range(&self.selection));

        let ctx = Arc::new(TaskContext {
            filter: filter.map(|filter| Arc::new(FilterExpr::new(filter))),
            reader: layout_reader,
            projection: self.projection.clone(),
        });
        Ok((ctx, split_ranges(&splits, row_range)))
    }

    /// Constructs a task per row split of the scan, returned as a vector of futures.
    ///
    /// No tasks are spawned. Each future returns the mapped split, or `None` when its split is
    /// filtered out. The futures may be polled independently in any order.
    ///
    /// Scans with row limits must use [`Self::into_stream`] or [`Self::into_iter`] so the scan
    /// can coordinate the limit across splits.
    pub fn build(self) -> VortexResult<Vec<BoxFuture<'static, VortexResult<Option<A>>>>> {
        if self.limit.is_some() {
            vortex_bail!("Split futures do not support row limits; use a scan stream or iterator");
        }

        let (ctx, ranges) = self.split_plan()?;
        split_masks(&self.selection, ranges)
            .map(|row_mask| Ok(map_split(split_exec(&ctx, row_mask)?, &self.map_fn)))
            .collect()
    }

    /// Execute the scan as a stream of the mapped split arrays, spawning split tasks onto the
    /// session's runtime handle.
    ///
    /// The stream ends after yielding its first error.
    fn split_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        let concurrency = self.concurrency * get_available_parallelism().unwrap_or(1);
        let handle = self.session.handle();
        let ordered = self.ordered;
        let (ctx, ranges) = self.split_plan()?;

        let map_fn = &self.map_fn;

        let arrays = match self.limit.as_ref().map(ScanLimit::budget) {
            // With a filter, a split's output row count is unknown until its filter has run.
            // Splits therefore filter and project ahead of the consumer, and the rows they
            // return are taken from the limit as they are emitted, discarding the excess. Split
            // tasks are built lazily so that no new splits start once the budget is spent. The
            // map function needs the trimmed arrays, so it runs in a second stage of CPU tasks.
            Some(limit) if ctx.filter.is_some() => {
                let gate = limit.clone();
                let selection = self.selection.clone();
                let split_handle = handle.clone();
                let tasks = stream::iter(ranges)
                    .take_while(move |_| future::ready(!gate.is_exhausted()))
                    .map(move |row_range| {
                        let ctx = Arc::clone(&ctx);
                        let selection = selection.clone();
                        split_handle.spawn_nested(move |handle| async move {
                            let task = handle
                                .spawn_cpu(move || {
                                    let row_mask = selection.row_mask(&row_range);
                                    if row_mask.mask().all_false() {
                                        return Ok(None);
                                    }
                                    split_exec(&ctx, row_mask).map(Some)
                                })
                                .await?;
                            match task {
                                Some(task) => task.await,
                                None => Ok(None),
                            }
                        })
                    });
                let map_fn = Arc::clone(map_fn);
                let arrays = buffer(tasks, ordered, concurrency)
                    .filter_map(|array| future::ready(array.transpose()))
                    .boxed();
                let mapped = limit_rows(arrays, limit).map(move |array| {
                    let map_fn = Arc::clone(&map_fn);
                    handle.spawn_cpu(move || map_fn(array?))
                });
                buffer(mapped, ordered, concurrency)
            }
            limit => {
                // Build eagerly to register I/O up front. Without a filter, selected row counts
                // are exact, so a limit can also be reserved here in split order.
                let mut tasks = Vec::new();
                for row_mask in split_masks(&self.selection, ranges) {
                    let task = if let Some(limit) = &limit {
                        if limit.is_exhausted() {
                            break;
                        }
                        let mask = limit.limit(row_mask.mask().clone());
                        split_projection(&ctx, &row_mask.row_range(), mask)?
                    } else {
                        split_exec(&ctx, row_mask)?
                    };
                    tasks.push(map_split(task, map_fn));
                }
                buffer(
                    stream::iter(tasks).map(move |task| handle.spawn(task)),
                    ordered,
                    concurrency,
                )
                .filter_map(|array| future::ready(array.transpose()))
                .boxed()
            }
        };

        Ok(stop_on_error(arrays))
    }

    /// Returns a [`Stream`] with tasks spawned onto the session's runtime handle.
    ///
    /// Preparation and initial stream construction begin on the first poll. Errors from either
    /// step are returned as the stream's next item.
    pub fn into_stream(
        self,
    ) -> VortexResult<impl Stream<Item = VortexResult<A>> + Send + 'static + use<A>> {
        Ok(LazyScanStream::new(self))
    }

    /// Returns an [`Iterator`] using the session's runtime.
    pub fn into_iter<B: BlockingRuntime>(
        self,
        runtime: &B,
    ) -> VortexResult<impl Iterator<Item = VortexResult<A>> + 'static> {
        let stream = self.into_stream()?;
        Ok(runtime.block_on_stream(stream))
    }
}

enum LazyScanState<A: 'static + Send> {
    Builder(Option<Box<ScanBuilder<A>>>),
    Preparing(Task<VortexResult<BoxStream<'static, VortexResult<A>>>>),
    Stream(BoxStream<'static, VortexResult<A>>),
    Error(Option<vortex_error::VortexError>),
}

struct LazyScanStream<A: 'static + Send> {
    state: LazyScanState<A>,
}

impl<A: 'static + Send> LazyScanStream<A> {
    fn new(builder: ScanBuilder<A>) -> Self {
        Self {
            state: LazyScanState::Builder(Some(Box::new(builder))),
        }
    }
}

impl<A: 'static + Send> Unpin for LazyScanStream<A> {}

impl<A: 'static + Send> Stream for LazyScanStream<A> {
    type Item = VortexResult<A>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                LazyScanState::Builder(builder) => {
                    let builder = builder.take().vortex_expect("polled after completion");
                    let handle = builder.session.handle();
                    // IMPORTANT: Building the stream can synchronously walk the layout and
                    // register I/O for every split. Keep it with preparation in this CPU
                    // task: poll_next must only wait for and poll an already-constructed stream.
                    // This also keeps construction errors on the Preparing -> Error path rather
                    // than running construction on the caller's executor.
                    let task = handle.spawn_cpu(move || builder.split_stream());
                    self.state = LazyScanState::Preparing(task);
                }
                LazyScanState::Preparing(task) => match ready!(Pin::new(task).poll(cx)) {
                    Ok(stream) => self.state = LazyScanState::Stream(stream),
                    Err(err) => self.state = LazyScanState::Error(Some(err)),
                },
                LazyScanState::Stream(stream) => return stream.as_mut().poll_next(cx),
                LazyScanState::Error(err) => return Poll::Ready(err.take().map(Err)),
            }
        }
    }
}

/// A function applied to each projected split array.
type MapFn<A> = Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>;

/// The smallest row range covering an index selection.
fn selection_range(selection: &Selection) -> Option<Range<u64>> {
    match selection {
        Selection::IncludeByIndex(buf) if !buf.is_empty() => Some(buf[0]..buf[buf.len() - 1] + 1),
        Selection::IncludeRoaring(roaring) if !roaring.is_empty() => {
            Some(roaring.min().vortex_expect("empty")..roaring.max().vortex_expect("empty") + 1)
        }
        _ => None,
    }
}

/// The ranges of `splits`, clamped to `row_range`.
fn split_ranges(splits: &Splits, row_range: Option<Range<u64>>) -> Vec<Range<u64>> {
    match splits {
        Splits::Natural(vec) => {
            debug_assert!(vec.is_sorted());
            let splits_iter = match row_range {
                None => Either::Left(vec.iter().copied()),
                Some(range) => {
                    if range.is_empty() {
                        return Vec::new();
                    }
                    let lo = vec.partition_point(|&x| x <= range.start);
                    let hi = vec.partition_point(|&x| x < range.end);
                    Either::Right(
                        iter::once(range.start)
                            .chain(vec[lo..hi].iter().copied())
                            .chain(iter::once(range.end)),
                    )
                }
            };

            splits_iter
                .tuple_windows()
                .map(|(start, end)| start..end)
                .collect()
        }
        Splits::Ranges(ranges) => match row_range {
            None => ranges.to_vec(),
            Some(range) => {
                if range.is_empty() {
                    return Vec::new();
                }
                ranges
                    .iter()
                    .filter_map(move |r| {
                        let start = cmp::max(r.start, range.start);
                        let end = cmp::min(r.end, range.end);
                        (start < end).then_some(start..end)
                    })
                    .collect()
            }
        },
    }
}

/// The non-empty row masks of the split ranges, in split order.
fn split_masks(
    selection: &Selection,
    ranges: Vec<Range<u64>>,
) -> impl Iterator<Item = RowMask> + '_ {
    ranges
        .into_iter()
        .filter(|range| range.start < range.end)
        .map(|range| selection.row_mask(&range))
        .filter(|row_mask| !row_mask.mask().all_false())
}

/// Apply `map_fn` to a split's array within the split's task.
fn map_split<A: 'static + Send>(
    task: SplitFuture,
    map_fn: &MapFn<A>,
) -> BoxFuture<'static, VortexResult<Option<A>>> {
    let map_fn = Arc::clone(map_fn);
    task.map(move |array| array?.map(|array| map_fn(array)).transpose())
        .boxed()
}

/// Trim arrays to the budget and drop upstream work as soon as it is exhausted.
fn limit_rows(
    arrays: BoxStream<'static, VortexResult<ArrayRef>>,
    limit: RowLimit,
) -> BoxStream<'static, VortexResult<ArrayRef>> {
    let mut arrays = Some(arrays);
    stream::poll_fn(move |cx| {
        loop {
            if limit.is_exhausted() {
                arrays = None;
            }
            let Some(stream) = arrays.as_mut() else {
                return Poll::Ready(None);
            };
            let array = match ready!(stream.poll_next_unpin(cx)) {
                Some(Ok(array)) => array,
                end => {
                    arrays = None;
                    return Poll::Ready(end);
                }
            };
            let granted = limit.take(array.len());
            if limit.is_exhausted() {
                arrays = None;
            }
            if granted != 0 {
                return Poll::Ready(Some(if granted < array.len() {
                    array.slice(0..granted)
                } else {
                    Ok(array)
                }));
            }
        }
    })
    .boxed()
}

/// Yield the first error and drop upstream without polling it again.
fn stop_on_error<T: Send + 'static>(
    stream: BoxStream<'static, VortexResult<T>>,
) -> BoxStream<'static, VortexResult<T>> {
    let mut stream = Some(stream);
    stream::poll_fn(move |cx| {
        let Some(input) = stream.as_mut() else {
            return Poll::Ready(None);
        };
        let item = ready!(input.poll_next_unpin(cx));
        if !matches!(item, Some(Ok(_))) {
            stream = None;
        }
        Poll::Ready(item)
    })
    .boxed()
}

/// Run up to `concurrency` futures of `tasks` at once, in order when `ordered` is set.
fn buffer<T: Send + 'static>(
    tasks: impl Stream<Item = impl Future<Output = T> + Send + 'static> + Send + 'static,
    ordered: bool,
    concurrency: usize,
) -> BoxStream<'static, T> {
    if ordered {
        tasks.buffered(concurrency).boxed()
    } else {
        tasks.buffer_unordered(concurrency).boxed()
    }
}

fn intersect_ranges(left: Option<&Range<u64>>, right: Option<Range<u64>>) -> Option<Range<u64>> {
    match (left, right) {
        (None, None) => None,
        (None, Some(r)) => Some(r),
        (Some(l), None) => Some(l.clone()),
        (Some(l), Some(r)) => Some(cmp::max(l.start, r.start)..cmp::min(l.end, r.end)),
    }
}

/// Compute masks of field paths referenced by the projection and filter in the scan.
///
/// Projection and filter must be pre-simplified and bound against the scan dtype.
pub fn referenced_field_masks(
    projection: &BoundExpression,
    filter: Option<&BoundExpression>,
) -> VortexResult<Vec<FieldMask>> {
    let mut field_paths = referenced_field_paths(projection)?;
    if let Some(filter) = filter {
        field_paths.extend(referenced_field_paths(filter)?);
    }
    Ok(field_paths
        .into_iter()
        .map(|path| {
            if path.is_root() {
                FieldMask::All
            } else {
                FieldMask::Prefix(path)
            }
        })
        .collect_vec())
}

#[cfg(test)]
mod test {
    use std::ops::Range;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::mpsc;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use futures::FutureExt;
    use futures::Stream;
    use futures::StreamExt;
    use futures::stream;
    use futures::task::noop_waker_ref;
    use parking_lot::Mutex;
    use rstest::rstest;
    use vortex_array::IntoArray;
    use vortex_array::MaskFuture;
    use vortex_array::VortexSessionExecute;
    use vortex_array::array_session;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::dtype::DType;
    use vortex_array::dtype::FieldMask;
    use vortex_array::dtype::FieldPath;
    use vortex_array::dtype::Nullability;
    use vortex_array::dtype::PType;
    use vortex_array::dtype::StructFields;
    use vortex_array::expr::BoundExpression;
    use vortex_array::expr::ExactBoundExpr;
    use vortex_array::expr::and;
    use vortex_array::expr::between;
    use vortex_array::expr::col;
    use vortex_array::expr::eq;
    use vortex_array::expr::get_item;
    use vortex_array::expr::gt_eq;
    use vortex_array::expr::is_not_null;
    use vortex_array::expr::lit;
    use vortex_array::expr::lt;
    use vortex_array::expr::root;
    use vortex_array::scalar_fn::fns::between::BetweenOptions;
    use vortex_array::scalar_fn::fns::between::StrictComparison;
    use vortex_error::VortexResult;
    use vortex_error::vortex_bail;
    use vortex_error::vortex_err;
    use vortex_io::runtime::BlockingRuntime;
    use vortex_io::runtime::single::SingleThreadRuntime;
    use vortex_io::runtime::tokio::TokioRuntime;
    use vortex_mask::Mask;

    use super::ScanBuilder;
    use super::limit_rows;
    use super::referenced_field_masks;
    use super::stop_on_error;
    use crate::ArrayFuture;
    use crate::LayoutReader;
    use crate::RowSplits;
    use crate::SplitRange;
    use crate::scan::limit::RowLimit;
    use crate::scan::test::SCAN_SESSION;
    use crate::scan::test::TestLayoutReader;
    use crate::scan::test::collect_scan_values;
    use crate::scan::test::drain_runtime;
    use crate::scan::test::keep_all;
    use crate::scan::test::keep_odd;
    use crate::scan::test::session_with_handle;

    fn nested_dtype() -> DType {
        DType::Struct(
            StructFields::from_iter([
                (
                    "a",
                    DType::Struct(
                        StructFields::from_iter([
                            ("1", DType::Primitive(PType::I32, Nullability::NonNullable)),
                            ("2", DType::Primitive(PType::I32, Nullability::NonNullable)),
                        ]),
                        Nullability::NonNullable,
                    ),
                ),
                ("b", DType::Primitive(PType::I32, Nullability::NonNullable)),
            ]),
            Nullability::NonNullable,
        )
    }

    #[test]
    fn bound_setters_preserve_identity() -> VortexResult<()> {
        let dtype = DType::Primitive(PType::I32, Nullability::NonNullable);
        let projection = eq(root(), lit(1_i32)).bind(&dtype)?;
        let filter = eq(root(), lit(2_i32)).bind(&dtype)?;
        let expected_projection = ExactBoundExpr(projection.clone());
        let expected_filter = ExactBoundExpr(filter.clone());
        let reader = Arc::new(CountingLayoutReader::new(Arc::new(AtomicUsize::new(0))));

        let builder = ScanBuilder::new(SCAN_SESSION.clone(), reader)
            .with_projection(projection)
            .with_filter(filter);

        assert_eq!(ExactBoundExpr(builder.projection), expected_projection);
        assert_eq!(builder.filter.map(ExactBoundExpr), Some(expected_filter));
        Ok(())
    }

    #[test]
    fn root_projection_produces_all_mask() -> VortexResult<()> {
        let dtype = DType::Primitive(PType::I32, Nullability::NonNullable);
        let projection = root().bind(&dtype)?;

        assert_eq!(referenced_field_masks(&projection, None)?, [FieldMask::All]);
        Ok(())
    }

    #[test]
    fn nested_projection_preserves_field_path_in_split_mask() -> VortexResult<()> {
        let dtype = nested_dtype();
        let projection = get_item("1", get_item("a", root())).bind(&dtype)?;
        let filter = eq(get_item("2", get_item("a", root())), lit(0_i32)).bind(&dtype)?;

        let field_masks = referenced_field_masks(&projection, Some(&filter))?;

        assert_eq!(field_masks.len(), 2);
        assert!(field_masks.contains(&FieldMask::Prefix(FieldPath::from_name("a").push("1"))));
        assert!(field_masks.contains(&FieldMask::Prefix(FieldPath::from_name("a").push("2"))));
        Ok(())
    }

    #[test]
    fn filter_path_covers_nested_projection_path() -> VortexResult<()> {
        let dtype = nested_dtype();
        let projection = get_item("1", get_item("a", root())).bind(&dtype)?;
        let filter = is_not_null(get_item("a", root())).bind(&dtype)?;

        let field_masks = referenced_field_masks(&projection, Some(&filter))?;

        assert_eq!(field_masks, [FieldMask::Prefix(FieldPath::from_name("a"))]);
        Ok(())
    }

    #[test]
    fn parent_projection_path_covers_nested_filter_path() -> VortexResult<()> {
        let dtype = nested_dtype();
        let projection = get_item("a", root()).bind(&dtype)?;
        let filter = is_not_null(get_item("1", get_item("a", root()))).bind(&dtype)?;

        let field_masks = referenced_field_masks(&projection, Some(&filter))?;

        assert_eq!(field_masks, [FieldMask::Prefix(FieldPath::from_name("a"))]);
        Ok(())
    }

    #[derive(Debug)]
    struct CountingLayoutReader {
        name: Arc<str>,
        dtype: DType,
        row_count: u64,
        register_splits_calls: Arc<AtomicUsize>,
    }

    impl CountingLayoutReader {
        fn new(register_splits_calls: Arc<AtomicUsize>) -> Self {
            Self {
                name: Arc::from("counting"),
                dtype: DType::Primitive(PType::I32, Nullability::NonNullable),
                row_count: 1,
                register_splits_calls,
            }
        }
    }

    impl LayoutReader for CountingLayoutReader {
        fn name(&self) -> &Arc<str> {
            &self.name
        }

        fn dtype(&self) -> &DType {
            &self.dtype
        }

        fn row_count(&self) -> u64 {
            self.row_count
        }

        fn register_splits(
            &self,
            _field_mask: &[FieldMask],
            split_range: &SplitRange,
            splits: &mut RowSplits,
        ) -> VortexResult<()> {
            self.register_splits_calls.fetch_add(1, Ordering::Relaxed);
            splits.push(split_range.root_row_range().end);
            Ok(())
        }

        fn pruning_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &BoundExpression,
            _mask: Mask,
        ) -> VortexResult<MaskFuture> {
            vortex_bail!("not needed for this test");
        }

        fn filter_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &BoundExpression,
            _mask: MaskFuture,
        ) -> VortexResult<MaskFuture> {
            vortex_bail!("not needed for this test");
        }

        fn projection_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &BoundExpression,
            _mask: MaskFuture,
        ) -> VortexResult<ArrayFuture> {
            Ok(Box::pin(async move {
                unreachable!("scan should not be polled in this test")
            }))
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn into_stream_is_lazy() {
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(CountingLayoutReader::new(Arc::clone(&calls)));

        let session = SCAN_SESSION.clone();

        let _stream = ScanBuilder::new(session, reader).into_stream().unwrap();

        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[derive(Debug)]
    struct SplittingLayoutReader {
        name: Arc<str>,
        dtype: DType,
        row_count: u64,
        register_splits_calls: Arc<AtomicUsize>,
        blocking_projection: Option<BlockingProjection>,
    }

    #[derive(Debug)]
    struct BlockingProjection {
        started: mpsc::Sender<()>,
        gate: Arc<Mutex<()>>,
    }

    impl SplittingLayoutReader {
        fn new(register_splits_calls: Arc<AtomicUsize>) -> Self {
            Self {
                name: Arc::from("splitting"),
                dtype: DType::Primitive(PType::I32, Nullability::NonNullable),
                row_count: 4,
                register_splits_calls,
                blocking_projection: None,
            }
        }

        fn with_blocking_projection(
            register_splits_calls: Arc<AtomicUsize>,
            gate: Arc<Mutex<()>>,
            started: mpsc::Sender<()>,
        ) -> Self {
            let mut reader = Self::new(register_splits_calls);
            reader.blocking_projection = Some(BlockingProjection { started, gate });
            reader
        }
    }

    impl LayoutReader for SplittingLayoutReader {
        fn name(&self) -> &Arc<str> {
            &self.name
        }

        fn dtype(&self) -> &DType {
            &self.dtype
        }

        fn row_count(&self) -> u64 {
            self.row_count
        }

        fn register_splits(
            &self,
            _field_mask: &[FieldMask],
            split_range: &SplitRange,
            splits: &mut RowSplits,
        ) -> VortexResult<()> {
            self.register_splits_calls.fetch_add(1, Ordering::Relaxed);
            for split in (split_range.row_range().start + 1)..=split_range.row_range().end {
                splits.push(split_range.row_offset() + split);
            }
            Ok(())
        }

        fn pruning_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &BoundExpression,
            mask: Mask,
        ) -> VortexResult<MaskFuture> {
            Ok(MaskFuture::ready(mask))
        }

        fn filter_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &BoundExpression,
            mask: MaskFuture,
        ) -> VortexResult<MaskFuture> {
            Ok(mask)
        }

        fn projection_evaluation(
            &self,
            row_range: &Range<u64>,
            _expr: &BoundExpression,
            _mask: MaskFuture,
        ) -> VortexResult<ArrayFuture> {
            if let Some(blocking_projection) = &self.blocking_projection {
                blocking_projection
                    .started
                    .send(())
                    .map_err(|_| vortex_err!("test projection-start receiver dropped"))?;
                let _guard = blocking_projection.gate.lock();
            }

            let start = usize::try_from(row_range.start)
                .map_err(|_| vortex_err!("row_range.start must fit in usize"))?;
            let end = usize::try_from(row_range.end)
                .map_err(|_| vortex_err!("row_range.end must fit in usize"))?;

            let values: VortexResult<Vec<i32>> = (start..end)
                .map(|v| i32::try_from(v).map_err(|_| vortex_err!("split value must fit in i32")))
                .collect();

            let array = PrimitiveArray::from_iter(values?).into_array();
            Ok(Box::pin(async move { Ok(array) }))
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn into_stream_executes_after_prepare() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(SplittingLayoutReader::new(Arc::clone(&calls)));

        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());

        let stream = ScanBuilder::new(session, reader).into_stream()?;
        let mut iter = runtime.block_on_stream(stream);

        let mut values = Vec::new();
        for chunk in &mut iter {
            let prim = chunk?.execute::<PrimitiveArray>(&mut ctx)?;
            values.push(prim.into_buffer::<i32>()[0]);
        }

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(values.as_ref(), [0, 1, 2, 3]);

        Ok(())
    }

    #[test]
    fn supplied_natural_splits_skip_layout_walk() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(SplittingLayoutReader::new(Arc::clone(&calls)));

        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());

        let stream = ScanBuilder::new(session, reader)
            .with_natural_splits(vec![0u64, 2, 4].into())
            .with_row_range(1..4)
            .into_stream()?;
        let mut iter = runtime.block_on_stream(stream);

        let mut chunks = Vec::new();
        for chunk in &mut iter {
            let prim = chunk?.execute::<PrimitiveArray>(&mut ctx)?;
            chunks.push(prim.into_buffer::<i32>().to_vec());
        }

        assert_eq!(calls.load(Ordering::Relaxed), 0);
        // Supplied full-file boundaries [0, 2, 4] clamped to rows 1..4.
        assert_eq!(chunks, [vec![1], vec![2, 3]]);

        Ok(())
    }

    #[rstest]
    #[case::eager(false)]
    #[case::filtered_limit(true)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn scan_constructs_tasks_off_the_poller(#[case] filtered: bool) -> VortexResult<()> {
        let gate = Arc::new(Mutex::new(()));
        let guard = gate.lock();
        let calls = Arc::new(AtomicUsize::new(0));
        let (started_send, started_recv) = mpsc::channel();
        let reader = Arc::new(SplittingLayoutReader::with_blocking_projection(
            Arc::clone(&calls),
            Arc::clone(&gate),
            started_send,
        ));

        let runtime = TokioRuntime::new(tokio::runtime::Handle::current());
        let session = session_with_handle(runtime.handle());
        let filter = is_not_null(root()).bind(reader.dtype())?;
        let builder = ScanBuilder::new(session, reader);
        let mut stream = if filtered {
            // Drive the prepared stream directly so its first poll must construct a split.
            builder.with_filter(filter).with_limit(1).split_stream()?
        } else {
            builder.into_stream()?.boxed()
        };

        let (poll_send, poll_recv) = mpsc::channel();
        let (release_send, release_recv) = mpsc::channel();
        let join = std::thread::spawn(move || {
            let waker = noop_waker_ref();
            let mut cx = Context::from_waker(waker);
            let poll = Pin::new(&mut stream).poll_next(&mut cx);
            let _ = poll_send.send(matches!(poll, Poll::Pending));
            let _ = release_recv.recv();
        });

        let poll_result = poll_recv.recv_timeout(Duration::from_secs(1));
        let projection_started = started_recv.recv_timeout(Duration::from_secs(1));

        // Release the task and join its caller before reporting a failed assertion.
        drop(guard);
        let _ = release_send.send(());
        drop(join.join());

        assert!(
            poll_result.is_ok_and(|poll_pending| poll_pending),
            "first poll must return while scan task construction is blocked"
        );
        projection_started
            .map_err(|_| vortex_err!("stream construction did not begin in the background"))?;
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        Ok(())
    }

    #[tokio::test]
    async fn into_stream_reports_stream_construction_errors() -> VortexResult<()> {
        let range_start = i32::MAX as u64 + 1;
        let reader = Arc::new(SplittingLayoutReader::new(Arc::new(AtomicUsize::new(0))));
        let session = session_with_handle(TokioRuntime::current());
        let mut stream = ScanBuilder::new(session, reader)
            .with_row_range(range_start..range_start + 1)
            .into_stream()?;

        assert!(matches!(stream.next().await, Some(Err(_))));
        assert!(stream.next().await.is_none());

        Ok(())
    }

    #[rstest]
    #[case::limit_below_matches(8, keep_all, 3, &[0, 1, 2])]
    #[case::limit_zero(8, keep_all, 0, &[])]
    #[case::limit_exceeds_matches(8, keep_odd, 100, &[1, 3, 5, 7])]
    #[case::empty_input(0, keep_all, 3, &[])]
    fn filtered_limit_yields_expected_rows(
        #[case] row_count: u64,
        #[case] keep_row: fn(u64) -> bool,
        #[case] limit: u64,
        #[case] expected: &[i32],
    ) -> VortexResult<()> {
        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());
        let reader = Arc::new(
            TestLayoutReader::new(row_count)
                .with_split_size(2)
                .with_keep_row(keep_row),
        );
        let filter = root().bind(reader.dtype())?;

        let stream = ScanBuilder::new(session, reader)
            .with_filter(filter)
            .with_limit(limit)
            .into_stream()?;
        let values = collect_scan_values(runtime.block_on_stream(stream))?;
        drain_runtime(&runtime);

        assert_eq!(values.as_slice(), expected);
        Ok(())
    }

    /// An ordered filtered limit takes rows in split order, so it returns the earliest matching
    /// rows even when later splits finish first.
    #[test]
    fn ordered_filtered_limit_returns_the_earliest_rows() -> VortexResult<()> {
        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());
        let reader = Arc::new(TestLayoutReader::new(12).with_split_size(4));
        let filter = root().bind(reader.dtype())?;

        let stream = ScanBuilder::new(session, reader)
            .with_filter(filter)
            .with_limit(6)
            .into_stream()?;
        let values = collect_scan_values(runtime.block_on_stream(stream))?;
        drain_runtime(&runtime);

        assert_eq!(values, [0, 1, 2, 3, 4, 5]);
        Ok(())
    }

    /// `map` applies to every emitted split, including the trimmed output of a filtered limit.
    #[rstest]
    #[case::unlimited(None, 8)]
    #[case::limited(Some(3), 3)]
    fn map_applies_to_every_emitted_split(
        #[case] limit: Option<u64>,
        #[case] expected_rows: usize,
    ) -> VortexResult<()> {
        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());
        let reader = Arc::new(TestLayoutReader::new(8).with_split_size(2));
        let filter = root().bind(reader.dtype())?;

        let rows = ScanBuilder::new(session, reader)
            .with_filter(filter)
            .with_some_limit(limit)
            .map(|array| Ok(array.len()))
            .into_iter(&runtime)?
            .sum::<VortexResult<usize>>()?;
        drain_runtime(&runtime);

        assert_eq!(rows, expected_rows);
        Ok(())
    }

    #[rstest]
    #[case::filter_error(true, None)]
    #[case::filter_error_with_limit(true, Some(2))]
    #[case::projection_error(false, None)]
    #[case::projection_error_with_limit(false, Some(2))]
    fn errors_end_the_scan(
        #[case] fail_filter: bool,
        #[case] limit: Option<u64>,
    ) -> VortexResult<()> {
        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());
        let reader = TestLayoutReader::new(2).with_split_size(1);
        let reader = Arc::new(if fail_filter {
            reader.with_fail_first_filter()
        } else {
            reader.with_fail_first_projection()
        });
        let filter = root().bind(reader.dtype())?;
        let stream = ScanBuilder::new(session, reader)
            .with_filter(filter)
            .with_some_limit(limit)
            .into_stream()?;
        let mut iter = runtime.block_on_stream(stream);

        assert!(matches!(iter.next(), Some(Err(_))));
        assert!(iter.next().is_none());
        drop(iter);
        drain_runtime(&runtime);
        Ok(())
    }

    #[test]
    fn full_file_splits_ignore_row_range() -> VortexResult<()> {
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(SplittingLayoutReader::new(Arc::clone(&calls)));

        let splits = ScanBuilder::new(SCAN_SESSION.clone(), reader)
            .with_row_range(1..3)
            .full_file_splits()?;

        assert_eq!(splits, [0, 1, 2, 3, 4]);

        Ok(())
    }

    #[test]
    fn projection_errors_are_stream_items() -> VortexResult<()> {
        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());
        let projection_masks = Arc::new(Mutex::new(Vec::new()));
        let reader = Arc::new(
            TestLayoutReader::new(1)
                .with_projection_masks(Arc::clone(&projection_masks))
                .with_projection_error(),
        );
        let filter = root().bind(reader.dtype())?;
        let stream = ScanBuilder::new(session, reader)
            .with_filter(filter)
            .with_limit(1)
            .into_stream()?;
        let mut iter = runtime.block_on_stream(stream);

        assert!(matches!(iter.next(), Some(Err(_))));
        assert!(iter.next().is_none());
        assert_eq!(projection_masks.lock().as_slice(), [1]);
        Ok(())
    }

    #[test]
    fn scan_limits_filtered_results() -> VortexResult<()> {
        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());
        let reader = Arc::new(
            TestLayoutReader::new(8)
                .with_split_size(2)
                .with_keep_row(keep_odd),
        );
        let filter = root().bind(reader.dtype())?;

        let iter = ScanBuilder::new(session, reader)
            .with_filter(filter)
            .with_limit(3)
            .into_array_iter(&runtime)?;
        let values = collect_scan_values(iter)?;
        drain_runtime(&runtime);

        assert_eq!(values, [1, 3, 5]);
        Ok(())
    }

    #[test]
    fn scan_evaluates_filter_ranges_as_between() -> VortexResult<()> {
        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());
        let filter_exprs = Arc::new(Mutex::new(Vec::new()));
        let dtype = nested_dtype();
        let reader = Arc::new(
            TestLayoutReader::new(4)
                .with_dtype(dtype.clone())
                .with_filter_exprs(Arc::clone(&filter_exprs)),
        );
        let filter = and(gt_eq(col("b"), lit(2_i32)), lt(col("b"), lit(5_i32))).bind(&dtype)?;

        let values = collect_scan_values(
            ScanBuilder::new(session, reader)
                .with_projection(col("b").bind(&dtype)?)
                .with_filter(filter)
                .into_array_iter(&runtime)?,
        )?;
        drain_runtime(&runtime);

        assert_eq!(values, [0, 1, 2, 3]);
        let expected = between(
            col("b"),
            lit(2_i32),
            lit(5_i32),
            BetweenOptions {
                lower_strict: StrictComparison::NonStrict,
                upper_strict: StrictComparison::Strict,
            },
        )
        .bind(&dtype)?;
        assert_eq!(filter_exprs.lock().as_slice(), [expected]);
        Ok(())
    }

    #[test]
    fn split_futures_can_be_polled_in_reverse() -> VortexResult<()> {
        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());
        let projection_masks = Arc::new(Mutex::new(Vec::new()));
        let reader = Arc::new(
            TestLayoutReader::new(8)
                .with_split_size(2)
                .with_keep_row(keep_odd)
                .with_projection_masks(Arc::clone(&projection_masks)),
        );
        let filter = root().bind(reader.dtype())?;
        let tasks = ScanBuilder::new(session, reader)
            .with_filter(filter)
            .with_row_range(1..7)
            .build()?;
        assert!(projection_masks.lock().is_empty());

        let mut arrays = Vec::new();
        for task in tasks.into_iter().rev() {
            if let Some(array) = runtime.block_on(task)? {
                arrays.push(Ok(array));
            }
        }
        assert_eq!(collect_scan_values(arrays)?, [5, 3, 1]);
        Ok(())
    }

    #[rstest]
    #[case(false)]
    #[case(true)]
    fn split_futures_reject_limits(#[case] shared: bool) -> VortexResult<()> {
        let reader = Arc::new(TestLayoutReader::new(8).with_split_size(2));
        let builder = ScanBuilder::new(SCAN_SESSION.clone(), reader);
        let builder = if shared {
            builder.with_shared_limit(RowLimit::new(1))
        } else {
            builder.with_limit(1)
        };

        assert!(builder.build().is_err());
        Ok(())
    }

    #[derive(Debug)]
    struct BlockingSplitsLayoutReader {
        name: Arc<str>,
        dtype: DType,
        row_count: u64,
        register_splits_calls: Arc<AtomicUsize>,
        gate: Arc<Mutex<()>>,
    }

    impl BlockingSplitsLayoutReader {
        fn new(gate: Arc<Mutex<()>>, register_splits_calls: Arc<AtomicUsize>) -> Self {
            Self {
                name: Arc::from("blocking-splits"),
                dtype: DType::Primitive(PType::I32, Nullability::NonNullable),
                row_count: 1,
                register_splits_calls,
                gate,
            }
        }
    }

    impl LayoutReader for BlockingSplitsLayoutReader {
        fn name(&self) -> &Arc<str> {
            &self.name
        }

        fn dtype(&self) -> &DType {
            &self.dtype
        }

        fn row_count(&self) -> u64 {
            self.row_count
        }

        fn register_splits(
            &self,
            _field_mask: &[FieldMask],
            split_range: &SplitRange,
            splits: &mut RowSplits,
        ) -> VortexResult<()> {
            self.register_splits_calls.fetch_add(1, Ordering::Relaxed);
            let _guard = self.gate.lock();
            splits.push(split_range.root_row_range().end);
            Ok(())
        }

        fn pruning_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &BoundExpression,
            _mask: Mask,
        ) -> VortexResult<MaskFuture> {
            vortex_bail!("not needed for this test");
        }

        fn filter_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &BoundExpression,
            _mask: MaskFuture,
        ) -> VortexResult<MaskFuture> {
            vortex_bail!("not needed for this test");
        }

        fn projection_evaluation(
            &self,
            _row_range: &Range<u64>,
            _expr: &BoundExpression,
            _mask: MaskFuture,
        ) -> VortexResult<ArrayFuture> {
            Ok(Box::pin(async move {
                unreachable!("scan should not be polled in this test")
            }))
        }

        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }

    #[test]
    fn into_stream_first_poll_does_not_block() {
        let gate = Arc::new(Mutex::new(()));
        let guard = gate.lock();

        let calls = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(BlockingSplitsLayoutReader::new(
            Arc::clone(&gate),
            Arc::clone(&calls),
        ));

        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());

        let mut stream = ScanBuilder::new(session, reader).into_stream().unwrap();

        let (send, recv) = mpsc::channel::<bool>();
        let join = std::thread::spawn(move || {
            let waker = noop_waker_ref();
            let mut cx = Context::from_waker(waker);
            let poll = Pin::new(&mut stream).poll_next(&mut cx);
            let _ = send.send(matches!(poll, Poll::Pending));
        });

        let polled_pending = recv.recv_timeout(Duration::from_secs(1)).ok();

        // Always release the gate and join the thread so failures don't hang the test process.
        drop(guard);
        drop(join.join());

        let polled_pending = polled_pending.expect("poll_next blocked; expected quick return");
        assert!(
            polled_pending,
            "expected Poll::Pending while prepare is blocked"
        );
        assert_eq!(calls.load(Ordering::Relaxed), 0);

        drop(runtime);
    }

    #[test]
    fn into_stream_with_row_range() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let calls = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(SplittingLayoutReader::new(Arc::clone(&calls)));

        let runtime = SingleThreadRuntime::default();
        let session = session_with_handle(runtime.handle());

        let stream = ScanBuilder::new(session, reader)
            .with_row_range(1..3)
            .into_stream()?;
        let mut iter = runtime.block_on_stream(stream);

        let mut values = Vec::new();
        for chunk in &mut iter {
            let prim = chunk?.execute::<PrimitiveArray>(&mut ctx)?;
            values.extend(prim.into_buffer::<i32>().iter().copied());
        }

        assert_eq!(calls.load(Ordering::Relaxed), 1);
        assert_eq!(values.as_ref(), [1, 2]);

        Ok(())
    }

    #[rstest]
    #[case::whole_array(4)]
    #[case::trimmed_array(2)]
    fn limit_ends_before_pending_work(#[case] rows: usize) -> VortexResult<()> {
        let array = PrimitiveArray::from_iter(0..4i32).into_array();
        let alive = Arc::new(());
        let dropped = Arc::downgrade(&alive);
        let pending = stream::poll_fn(move |_| {
            let _ = &alive;
            Poll::Pending
        });
        let arrays = stream::iter([Ok(array)]).chain(pending).boxed();
        let mut limited = limit_rows(arrays, RowLimit::new(rows as u64));

        let array = limited
            .next()
            .now_or_never()
            .expect("ready")
            .expect("array")?;
        assert_eq!(array.len(), rows);
        assert!(dropped.upgrade().is_none());
        assert!(matches!(limited.next().now_or_never(), Some(None)));
        Ok(())
    }

    #[test]
    fn exhausted_shared_limit_ends_before_pending_work() {
        let limit = RowLimit::new(1);
        let mut limited = limit_rows(stream::pending().boxed(), limit.clone());
        assert!(limited.next().now_or_never().is_none());

        assert_eq!(limit.take(1), 1);
        assert!(matches!(limited.next().now_or_never(), Some(None)));
    }

    #[test]
    fn zero_limit_does_not_poll_input() {
        let arrays = stream::poll_fn(|_| panic!("zero limit must not poll input")).boxed();
        let mut limited = limit_rows(arrays, RowLimit::new(0));
        assert!(matches!(limited.next().now_or_never(), Some(None)));
    }

    #[test]
    fn error_ends_before_pending_work() {
        let arrays = stream::iter([Err::<(), _>(vortex_err!("scan failed"))])
            .chain(stream::pending())
            .boxed();
        let mut stream = stop_on_error(arrays);

        assert!(matches!(stream.next().now_or_never(), Some(Some(Err(_)))));
        assert!(matches!(stream.next().now_or_never(), Some(None)));
    }
}
