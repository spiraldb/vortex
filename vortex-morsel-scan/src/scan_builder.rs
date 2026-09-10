// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;

use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::BoxStream;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::dtype::DType;
use vortex_array::expr::BoundExpression;
use vortex_array::stream::ArrayStream;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::LayoutRef;
use vortex_layout::segments::SegmentSource;
use vortex_metrics::MetricsRegistry;
use vortex_morsel_push::PushMorselScanExecutor;
use vortex_scan::selection::Selection;
use vortex_session::VortexSession;
use vortex_utils::parallelism::get_available_parallelism;

use crate::ScanBackend;
use crate::ScanExecutorOptions;

/// Builder for push scans over a raw layout and segment source.
///
/// Unlike [`vortex_layout::scan::scan_builder::ScanBuilder`], this builder never constructs or
/// accepts a layout reader. Layout planning and predicate execution are owned entirely by the
/// selected morsel implementation.
pub struct MorselScanBuilder<A> {
    session: VortexSession,
    executor: PushMorselScanExecutor,
    projection: BoundExpression,
    filter: Option<BoundExpression>,
    ordered: bool,
    row_range: Option<Range<u64>>,
    selection: Selection,
    concurrency: usize,
    map_fn: Arc<dyn Fn(ArrayRef) -> VortexResult<A> + Send + Sync>,
    _metrics_registry: Option<Arc<dyn MetricsRegistry>>,
    limit: Option<u64>,
    row_offset: u64,
}

impl MorselScanBuilder<ArrayRef> {
    /// Create a dedicated push builder over raw file layout state.
    pub fn new(
        session: VortexSession,
        backend: ScanBackend,
        layout: LayoutRef,
        segments: Arc<dyn SegmentSource>,
        options: &ScanExecutorOptions,
    ) -> VortexResult<Self> {
        let projection = BoundExpression::new_root(layout.dtype().clone());
        let executor = match backend {
            ScanBackend::V1 => {
                vortex_bail!("MorselScanBuilder only supports the push backend")
            }
            ScanBackend::Push => {
                let mut executor =
                    PushMorselScanExecutor::new(layout, segments).with_threads(options.threads);
                if let Some(driver) = &options.external_driver {
                    executor = executor.with_external_threads(Arc::clone(driver));
                }
                executor
            }
        };
        Ok(Self {
            session,
            executor,
            projection,
            filter: None,
            ordered: true,
            row_range: None,
            selection: Selection::All,
            concurrency: 4,
            map_fn: Arc::new(Ok),
            _metrics_registry: None,
            limit: None,
            row_offset: 0,
        })
    }

    /// Return an array stream for this scan.
    pub fn into_array_stream(self) -> VortexResult<impl ArrayStream + Send + 'static> {
        let dtype = self.dtype();
        Ok(ArrayStreamAdapter::new(dtype, self.into_stream()?))
    }
}

impl<A: 'static + Send> MorselScanBuilder<A> {
    /// Set the bound filter expression.
    pub fn with_filter(mut self, filter: BoundExpression) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set or clear the bound filter expression.
    pub fn with_some_filter(mut self, filter: Option<BoundExpression>) -> Self {
        self.filter = filter;
        self
    }

    /// Set the bound output projection.
    pub fn with_projection(mut self, projection: BoundExpression) -> Self {
        self.projection = projection;
        self
    }

    /// Configure whether output futures are consumed in file order.
    pub fn with_ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    /// Restrict scanning to a contiguous root row range.
    pub fn with_row_range(mut self, row_range: Range<u64>) -> Self {
        self.row_range = Some(row_range);
        self
    }

    /// Apply a row selection inside the configured row range.
    pub fn with_selection(mut self, selection: Selection) -> Self {
        self.selection = selection;
        self
    }

    /// Configure the number of output futures driven concurrently per runtime worker.
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        assert!(concurrency > 0);
        self.concurrency = concurrency;
        self
    }

    /// Retain the metrics registry for scan-builder API parity.
    ///
    /// Morsel executor metrics are not yet published through this registry.
    pub fn with_metrics_registry(mut self, metrics: Arc<dyn MetricsRegistry>) -> Self {
        self._metrics_registry = Some(metrics);
        self
    }

    /// Set the maximum number of output rows.
    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Set the root offset used by row-index expressions.
    pub fn with_row_offset(mut self, row_offset: u64) -> Self {
        self.row_offset = row_offset;
        self
    }

    /// Return the projected output dtype.
    pub fn dtype(&self) -> DType {
        self.projection.dtype().clone()
    }

    /// Compute full-file natural boundaries using the selected morsel planner.
    pub fn full_file_splits(&self) -> VortexResult<Vec<u64>> {
        self.executor
            .full_file_splits(&self.projection, self.filter.as_ref())
    }

    /// Map every non-empty output array.
    pub fn map<B: 'static + Send>(
        self,
        map_fn: impl Fn(A) -> VortexResult<B> + 'static + Send + Sync,
    ) -> MorselScanBuilder<B> {
        let old_map_fn = self.map_fn;
        MorselScanBuilder {
            session: self.session,
            executor: self.executor,
            projection: self.projection,
            filter: self.filter,
            ordered: self.ordered,
            row_range: self.row_range,
            selection: self.selection,
            concurrency: self.concurrency,
            map_fn: Arc::new(move |array| old_map_fn(array).and_then(&map_fn)),
            _metrics_registry: self._metrics_registry,
            limit: self.limit,
            row_offset: self.row_offset,
        }
    }

    /// Build one independently awaitable task per output unit.
    pub fn build(self) -> VortexResult<Vec<BoxFuture<'static, VortexResult<Option<A>>>>> {
        if self.limit == Some(0) {
            return Ok(Vec::new());
        }
        let limit = self.limit;
        let map_fn = Arc::clone(&self.map_fn);
        let tasks = self.executor.build(
            self.session,
            self.projection,
            self.filter,
            self.row_range,
            self.selection,
            limit,
            self.row_offset,
        )?;
        let tasks = match limit {
            Some(limit) => limit_tasks(tasks, limit),
            None => tasks,
        };
        Ok(tasks
            .into_iter()
            .map(move |task| {
                let map_fn = Arc::clone(&map_fn);
                Box::pin(async move { task.await?.map(|array| map_fn(array)).transpose() })
                    as BoxFuture<'static, VortexResult<Option<A>>>
            })
            .collect())
    }

    /// Return a runtime-driven stream of non-empty scan outputs.
    pub fn into_stream(self) -> VortexResult<BoxStream<'static, VortexResult<A>>> {
        let ordered = self.ordered;
        let concurrency = self.concurrency * get_available_parallelism().unwrap_or(1);
        let handle = self.session.handle();
        let tasks = self.build()?;
        let stream = futures::stream::iter(tasks).map(move |task| handle.spawn(task));
        let stream = if ordered {
            stream.buffered(concurrency).boxed()
        } else {
            stream.buffer_unordered(concurrency).boxed()
        };
        Ok(stream
            .filter_map(|result| async move { result.transpose() })
            .boxed())
    }
}

fn limit_tasks(
    tasks: Vec<BoxFuture<'static, VortexResult<Option<ArrayRef>>>>,
    limit: u64,
) -> Vec<BoxFuture<'static, VortexResult<Option<ArrayRef>>>> {
    vec![Box::pin(async move {
        let mut remaining = limit;
        let mut batches = Vec::new();
        for task in tasks {
            if remaining == 0 {
                break;
            }
            let Some(batch) = task.await? else {
                continue;
            };
            let take = batch
                .len()
                .min(usize::try_from(remaining).unwrap_or(usize::MAX));
            remaining -= take as u64;
            if take == batch.len() {
                batches.push(batch);
            } else {
                batches.push(batch.slice(0..take)?);
            }
        }
        match batches.len() {
            0 => Ok(None),
            1 => Ok(batches.pop()),
            _ => {
                let dtype = batches[0].dtype().clone();
                ChunkedArray::try_new(batches, dtype).map(|array| Some(array.into_array()))
            }
        }
    })]
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use futures::future;
    use vortex_array::IntoArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_error::vortex_err;

    use super::*;

    #[test]
    fn limit_spans_ordered_output_tasks() -> VortexResult<()> {
        let tasks = [[1i32, 2], [3, 4]]
            .into_iter()
            .map(|values| {
                Box::pin(future::ready(Ok(Some(
                    PrimitiveArray::from_iter(values).into_array(),
                )))) as BoxFuture<'static, VortexResult<Option<ArrayRef>>>
            })
            .collect();

        let mut limited = limit_tasks(tasks, 3);
        let task = limited
            .pop()
            .ok_or_else(|| vortex_err!("limit did not return an output task"))?;
        let output = block_on(task)?.ok_or_else(|| vortex_err!("limit returned no rows"))?;
        assert_eq!(output.len(), 3);
        Ok(())
    }
}
