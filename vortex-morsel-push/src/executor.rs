// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Integration with the dedicated morsel scan builder.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::channel::oneshot;
use futures::future::BoxFuture;
use futures::future::try_join_all;
use parking_lot::Mutex;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::dtype::DType;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::Expression;
use vortex_array::scalar_fn::fns::binary::Binary;
use vortex_array::scalar_fn::fns::dynamic::DynamicComparison;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::LayoutReaderContext;
use vortex_layout::LayoutReaderRef;
use vortex_layout::LayoutRef;
use vortex_layout::layouts::row_idx::RowIdx;
use vortex_layout::segments::SegmentSource;
use vortex_mask::AllOr;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;

use crate::MorselScan;
use crate::build::ExecPlan;
use crate::build::build_plan_with_row_offset;
use crate::driver::StreamCancellation;
use crate::driver::morsels;
use crate::io::IoService;
use crate::nodes::ConjunctMode;
use crate::source::SegmentSourceDriver;

type PlanCacheKey = (String, Option<String>, ConjunctMode, u64);

/// Morsels kept visible to background I/O ahead of the active workers in shared scans, so the
/// file driver sees enough adjacent segments to coalesce and cold reads overlap execution.
const SHARED_LOOKAHEAD_MORSELS: usize = 16;

/// Keep stats evaluation bounded while exposing enough adjacent morsels for stats reads to batch.
const PRUNING_LOOKAHEAD_MORSELS: usize = 16;

/// Push-morsel execution backend over a raw layout and segment source.
pub struct PushMorselScanExecutor {
    layout: LayoutRef,
    segments: Arc<dyn SegmentSource>,
    target_rows: u64,
    conjunct_mode: ConjunctMode,
    threads: usize,
    external_driver: Option<Arc<dyn Fn() -> bool + Send + Sync>>,
    plan_cache: Mutex<HashMap<PlanCacheKey, Arc<ExecPlan>>>,
}

impl PushMorselScanExecutor {
    /// Create an executor over a raw layout and its segment source.
    pub fn new(layout: LayoutRef, segments: Arc<dyn SegmentSource>) -> Self {
        Self {
            layout,
            segments,
            target_rows: 128 * 1024,
            conjunct_mode: ConjunctMode::Cascade,
            threads: 4,
            external_driver: None,
            plan_cache: Mutex::default(),
        }
    }

    /// Set the target number of rows per morsel.
    pub fn with_target_rows(mut self, target_rows: u64) -> Self {
        self.target_rows = target_rows;
        self
    }

    /// Set the conjunct evaluation policy.
    pub fn with_conjunct_mode(mut self, conjunct_mode: ConjunctMode) -> Self {
        self.conjunct_mode = conjunct_mode;
        self
    }

    /// Set the number of affinity workers used by one shared scan run.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads.max(1);
        self
    }

    /// Run each returned morsel future on the thread that polls it.
    pub fn with_external_threads(mut self, driver: Arc<dyn Fn() -> bool + Send + Sync>) -> Self {
        self.external_driver = Some(driver);
        self
    }

    /// Return the natural full-file row boundaries for this projection and filter.
    pub fn full_file_splits(
        &self,
        projection: &BoundExpression,
        filter: Option<&BoundExpression>,
    ) -> VortexResult<Vec<u64>> {
        let plan = self.plan(projection, filter, 0)?;
        let mut boundaries = Vec::with_capacity(plan.natural_splits().len() + 1);
        boundaries.push(0);
        boundaries.extend(
            plan.natural_splits()
                .iter()
                .copied()
                .filter(|boundary| *boundary != 0),
        );
        Ok(boundaries)
    }

    /// Build independently awaitable output tasks without constructing a layout reader.
    ///
    /// `limit` is honoured exactly on unfiltered scans: morsels past it are never read and the
    /// last one is capped. A filtered scan cannot know where the limit falls, so it returns every
    /// matching row and the caller trims. Dropping every returned future stops the scan.
    #[expect(clippy::too_many_arguments)]
    pub fn build(
        &self,
        session: VortexSession,
        projection: BoundExpression,
        filter: Option<BoundExpression>,
        row_range: Option<Range<u64>>,
        selection: vortex_scan::selection::Selection,
        limit: Option<u64>,
        row_offset: u64,
    ) -> VortexResult<Vec<BoxFuture<'static, VortexResult<Option<ArrayRef>>>>> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }

        let plan = self.plan(&projection, filter.as_ref(), row_offset)?;
        let full_range = row_range.unwrap_or_else(|| 0..plan.row_count());
        let mut morsels =
            selected_morsels(morsels(&plan, self.target_rows), &full_range, &selection);
        // Without a filter every selected row is an output row, so the morsels past the limit
        // can be dropped before any I/O and the last one capped exactly. A filtered scan cannot
        // know where the limit falls.
        let mut row_caps = None;
        if let Some(limit) = limit
            && filter.is_none()
        {
            let mut remaining = limit;
            let mut caps = Vec::with_capacity(morsels.len());
            for morsel in &morsels {
                if remaining == 0 {
                    break;
                }
                let rows = morsel
                    .selected_ranges
                    .iter()
                    .map(|range| range.end - range.start)
                    .sum::<u64>();
                caps.push(usize::try_from(rows.min(remaining)).unwrap_or(usize::MAX));
                remaining = remaining.saturating_sub(rows);
            }
            morsels.truncate(caps.len());
            row_caps = Some(caps);
        }

        // Build a fresh pruning reader for this scan. Its zone-map state is shared only by the
        // morsels in this invocation; nothing survives into a later scan. Dynamic filters are
        // intentionally excluded because their bounds can change after this one-shot prepass.
        let pruner = filter
            .as_ref()
            .map(static_conjuncts)
            .transpose()?
            .filter(|conjuncts| !conjuncts.is_empty())
            .map(|conjuncts| {
                Ok::<_, vortex_error::VortexError>(StaticPruner {
                    reader: self.layout.new_reader(
                        "morsel-pruning".into(),
                        Arc::clone(&self.segments),
                        &session,
                        &LayoutReaderContext::new(),
                    )?,
                    conjuncts: conjuncts.into(),
                })
            })
            .transpose()?;

        if let Some(driver) = &self.external_driver {
            return build_external_outputs(
                session,
                plan,
                Arc::clone(&self.segments),
                morsels,
                row_caps,
                Arc::clone(driver),
                pruner,
            );
        }

        // Each output future carries a guard; when the last guard drops, whether because its
        // future was consumed or discarded, there is nobody left to deliver to and the scan is
        // cancelled.
        let cancellation = StreamCancellation::new();
        let undelivered = Arc::new(AtomicUsize::new(morsels.len()));
        let mut senders = Vec::with_capacity(morsels.len());
        let mut outputs = Vec::with_capacity(morsels.len());
        for _ in 0..morsels.len() {
            let (sender, receiver) = oneshot::channel();
            senders.push(sender);
            let guard = DeliveryGuard {
                undelivered: Arc::clone(&undelivered),
                cancellation: Arc::clone(&cancellation),
            };
            outputs.push(Box::pin(async move {
                let _guard = guard;
                receiver
                    .await
                    .map_err(|_| vortex_err!("shared morsel scan coordinator stopped"))?
            })
                as BoxFuture<'static, VortexResult<Option<ArrayRef>>>);
        }

        let driver = SegmentSourceDriver::new(Arc::clone(&self.segments));
        let handle = session.handle();
        let coordinator_handle = handle.clone();
        let driver_handle = handle.clone();
        let max_threads = self.threads;
        handle
            .spawn(async move {
                let morsels = match prune_morsels(pruner.as_ref(), morsels).await {
                    Ok(morsels) => morsels,
                    Err(err) => {
                        let message = err.to_string();
                        fail_senders(senders, &message);
                        return;
                    }
                };

                let mut ranges = Vec::new();
                let mut targets = Vec::new();
                let mut groups = Vec::with_capacity(morsels.len());
                for (morsel_index, (morsel, sender)) in morsels.into_iter().zip(senders).enumerate()
                {
                    if morsel.selected_ranges.is_empty() {
                        drop(sender.send(Ok(None)));
                        continue;
                    }
                    let group = Arc::new(OutputGroup::new(
                        morsel.selected_ranges.len(),
                        plan.output_dtype().clone(),
                        sender,
                        row_caps.as_ref().map(|caps| caps[morsel_index]),
                    ));
                    for (local_index, range) in morsel.selected_ranges.into_iter().enumerate() {
                        ranges.push(range);
                        targets.push(CompletionTarget {
                            group: Arc::clone(&group),
                            local_index,
                        });
                    }
                    groups.push(group);
                }

                if ranges.is_empty() {
                    return;
                }
                let threads = ranges.len().min(max_threads);
                let result = coordinator_handle
                    .spawn_blocking(move || {
                        let scan = MorselScan::new(plan, session)
                            .with_threads(threads)
                            .with_morsels(ranges)
                            .with_sparse_morsels(true)
                            .with_lookahead_morsels(SHARED_LOOKAHEAD_MORSELS)
                            .with_eager_lookahead(true)
                            .with_cancellation(cancellation)
                            .with_completion_sink(move |index, batch| {
                                targets[index].complete(batch);
                            });
                        driver.connect(scan, &driver_handle)?.run().map(|_| ())
                    })
                    .await;
                if let Err(err) = result {
                    let message = err.to_string();
                    for group in groups {
                        group.fail(&message);
                    }
                }
            })
            .detach();

        Ok(outputs)
    }

    fn plan(
        &self,
        projection: &BoundExpression,
        filter: Option<&BoundExpression>,
        row_offset: u64,
    ) -> VortexResult<Arc<ExecPlan>> {
        let projection = unbind(projection)?;
        let filter = filter.map(unbind).transpose()?;
        let plan_key = (
            projection.to_string(),
            filter.as_ref().map(ToString::to_string),
            self.conjunct_mode,
            row_offset,
        );
        let mut cache = self.plan_cache.lock();
        match cache.get(&plan_key) {
            Some(plan) => Ok(Arc::clone(plan)),
            None => {
                let plan = Arc::new(build_plan_with_row_offset(
                    &self.layout,
                    &projection,
                    filter.as_ref(),
                    self.conjunct_mode,
                    row_offset,
                )?);
                cache.insert(plan_key, Arc::clone(&plan));
                Ok(plan)
            }
        }
    }
}

fn build_external_outputs(
    session: VortexSession,
    plan: Arc<ExecPlan>,
    segments: Arc<dyn SegmentSource>,
    morsels: Vec<SelectedMorsel>,
    row_caps: Option<Vec<usize>>,
    driver: Arc<dyn Fn() -> bool + Send + Sync>,
    pruner: Option<StaticPruner>,
) -> VortexResult<Vec<BoxFuture<'static, VortexResult<Option<ArrayRef>>>>> {
    // One I/O service, and therefore one demand stream, spans every morsel of this file so
    // reads dedupe across them. The engine's threads advance the runtime the driver runs on.
    let source = SegmentSourceDriver::new(segments);
    let (io, demand) = IoService::new();
    io.set_background_reads(source.prefers_background_reads());
    io.set_probe(Some(source.nowait_probe()));
    session
        .handle()
        .spawn(source.drive(demand, io.completions()))
        .detach();
    let mut outputs = Vec::with_capacity(morsels.len());
    for (morsel_index, morsel) in morsels.into_iter().enumerate() {
        let row_cap = row_caps.as_ref().map(|caps| caps[morsel_index]);
        let plan = Arc::clone(&plan);
        let io = Arc::clone(&io);
        let driver = Arc::clone(&driver);
        let session = session.clone();
        let pruner = pruner.clone();
        outputs.push(Box::pin(async move {
            let ranges = prune_morsel(pruner.as_ref(), morsel).await?.selected_ranges;
            if ranges.is_empty() {
                return Ok(None);
            }
            let (batches, _) = MorselScan::new_with_morsels(plan, session, ranges)
                .with_io_service(io)
                .with_external_driver(driver)
                .with_share_decodes(false)
                .with_sparse_morsels(true)
                .with_eager_lookahead(true)
                .run_on_current_thread()?;
            match (combine_batches(batches)?, row_cap) {
                (Some(array), Some(cap)) if array.len() > cap => Ok(Some(array.slice(0..cap)?)),
                (batch, _) => Ok(batch),
            }
        })
            as BoxFuture<'static, VortexResult<Option<ArrayRef>>>);
    }
    Ok(outputs)
}

#[derive(Clone)]
struct StaticPruner {
    reader: LayoutReaderRef,
    conjuncts: Arc<[BoundExpression]>,
}

async fn prune_morsels(
    pruner: Option<&StaticPruner>,
    morsels: Vec<SelectedMorsel>,
) -> VortexResult<Vec<SelectedMorsel>> {
    let mut pruned = Vec::with_capacity(morsels.len());
    let mut morsels = morsels.into_iter();
    loop {
        let pending = morsels
            .by_ref()
            .take(PRUNING_LOOKAHEAD_MORSELS)
            .map(|morsel| prune_morsel(pruner, morsel))
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(pruned);
        }
        pruned.extend(try_join_all(pending).await?);
    }
}

async fn prune_morsel(
    pruner: Option<&StaticPruner>,
    morsel: SelectedMorsel,
) -> VortexResult<SelectedMorsel> {
    let Some(pruner) = pruner else {
        return Ok(morsel);
    };

    let mut selected_ranges = Vec::new();
    for range in morsel.selected_ranges {
        let len = usize::try_from(range.end - range.start).unwrap_or(usize::MAX);
        // Construct every independent stats future before awaiting any of them. Besides avoiding
        // an artificial conjunct-by-conjunct dependency, this exposes all auxiliary segments to
        // the file source together so adjacent stats reads can coalesce just as they do in V1.
        let futures = pruner
            .conjuncts
            .iter()
            .map(|conjunct| {
                pruner
                    .reader
                    .pruning_evaluation(&range, conjunct, Mask::new_true(len))
            })
            .collect::<VortexResult<Vec<_>>>()?;
        let mask = Mask::intersect_owned(try_join_all(futures).await?);
        selected_ranges.extend(mask_ranges(&range, &mask));
    }
    Ok(SelectedMorsel { selected_ranges })
}

fn static_conjuncts(filter: &BoundExpression) -> VortexResult<Vec<BoundExpression>> {
    let mut conjuncts = Vec::new();
    let mut pending = vec![filter];
    while let Some(expr) = pending.pop() {
        let is_and = expr
            .as_scalar()
            .and_then(|scalar_fn| scalar_fn.as_opt::<Binary>())
            .is_some_and(|operator| *operator == Operator::And);
        if is_and {
            pending.extend(expr.children().iter().rev());
        } else if !expr.contains::<DynamicComparison>()? && !expr.contains::<RowIdx>()? {
            conjuncts.push(expr.clone());
        }
    }
    Ok(conjuncts)
}

fn fail_senders(senders: Vec<oneshot::Sender<VortexResult<Option<ArrayRef>>>>, message: &str) {
    for sender in senders {
        drop(sender.send(Err(vortex_err!(
            "shared morsel scan pruning failed: {message}"
        ))));
    }
}

fn combine_batches(mut batches: Vec<ArrayRef>) -> VortexResult<Option<ArrayRef>> {
    match batches.len() {
        0 => Ok(None),
        1 => Ok(batches.pop()),
        _ => {
            let dtype = batches[0].dtype().clone();
            ChunkedArray::try_new(batches, dtype).map(|array| Some(array.into_array()))
        }
    }
}

struct CompletionTarget {
    group: Arc<OutputGroup>,
    local_index: usize,
}

impl CompletionTarget {
    fn complete(&self, batch: VortexResult<Option<ArrayRef>>) {
        match batch {
            Ok(batch) => self.group.complete(self.local_index, batch),
            Err(err) => self.group.fail(&err.to_string()),
        }
    }
}

struct OutputGroup {
    remaining: AtomicUsize,
    dtype: DType,
    batches: Mutex<Vec<(usize, ArrayRef)>>,
    sender: Mutex<Option<oneshot::Sender<VortexResult<Option<ArrayRef>>>>>,
    /// Exact output rows for this morsel under an unfiltered limit.
    row_cap: Option<usize>,
}

impl OutputGroup {
    fn new(
        remaining: usize,
        dtype: DType,
        sender: oneshot::Sender<VortexResult<Option<ArrayRef>>>,
        row_cap: Option<usize>,
    ) -> Self {
        Self {
            remaining: AtomicUsize::new(remaining),
            dtype,
            batches: Mutex::new(Vec::new()),
            sender: Mutex::new(Some(sender)),
            row_cap,
        }
    }

    fn complete(&self, index: usize, batch: Option<ArrayRef>) {
        if let Some(batch) = batch {
            self.batches.lock().push((index, batch));
        }
        if self.remaining.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        let mut batches = std::mem::take(&mut *self.batches.lock());
        batches.sort_unstable_by_key(|(index, _)| *index);
        let result = match batches.len() {
            0 => Ok(None),
            1 => Ok(batches.pop().map(|(_, batch)| batch)),
            _ => ChunkedArray::try_new(
                batches.into_iter().map(|(_, batch)| batch),
                self.dtype.clone(),
            )
            .map(|array| Some(array.into_array())),
        };
        let result = match (result, self.row_cap) {
            (Ok(Some(array)), Some(cap)) if array.len() > cap => array.slice(0..cap).map(Some),
            (result, _) => result,
        };
        if let Some(sender) = self.sender.lock().take() {
            drop(sender.send(result));
        }
    }

    fn fail(&self, message: &str) {
        if let Some(sender) = self.sender.lock().take() {
            drop(sender.send(Err(vortex_err!("shared morsel scan failed: {message}"))));
        }
    }
}

/// Cancels the scan when the last output future is consumed or dropped.
struct DeliveryGuard {
    undelivered: Arc<AtomicUsize>,
    cancellation: Arc<StreamCancellation>,
}

impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        if self.undelivered.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.cancellation.cancel();
        }
    }
}

fn mask_ranges(range: &Range<u64>, mask: &Mask) -> Vec<Range<u64>> {
    match mask.slices() {
        AllOr::All => vec![range.clone()],
        AllOr::None => Vec::new(),
        AllOr::Some(slices) => slices
            .iter()
            .map(|&(start, end)| range.start + start as u64..range.start + end as u64)
            .collect(),
    }
}

fn unbind(expr: &BoundExpression) -> VortexResult<Expression> {
    let Some(scalar_fn) = expr.as_scalar() else {
        return Ok(Expression::Root);
    };
    Expression::try_new(
        scalar_fn.clone(),
        expr.children()
            .iter()
            .map(unbind)
            .collect::<VortexResult<Vec<_>>>()?,
    )
}

struct SelectedMorsel {
    selected_ranges: Vec<Range<u64>>,
}

fn selected_morsels(
    morsels: Vec<Range<u64>>,
    row_range: &Range<u64>,
    selection: &vortex_scan::selection::Selection,
) -> Vec<SelectedMorsel> {
    morsels
        .into_iter()
        .filter_map(|range| {
            let start = range.start.max(row_range.start);
            let end = range.end.min(row_range.end);
            (start < end).then_some(start..end)
        })
        .filter_map(|range| {
            let mask = selection.row_mask(&range);
            let selection_mask = mask.mask().clone();
            let selected_ranges = mask_ranges(&range, &selection_mask);
            (!selected_ranges.is_empty()).then_some(SelectedMorsel { selected_ranges })
        })
        .collect()
}
