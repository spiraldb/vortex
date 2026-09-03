// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Integration with the dedicated morsel scan builder.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use futures::channel::oneshot;
use futures::future::BoxFuture;
use parking_lot::Mutex;
use vortex_array::ArrayRef;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::Expression;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::LayoutRef;
use vortex_layout::segments::SegmentSource;
use vortex_mask::AllOr;
use vortex_mask::Mask;
use vortex_session::VortexSession;
use vortex_utils::aliases::hash_map::HashMap;

use crate::ExecPlan;
use crate::MorselExecutor;
use crate::MorselScan;
use crate::build::LayoutPlanners;
use crate::driver::ScanCancellation;
use crate::morsels;
use crate::nodes::ConjunctMode;
use crate::source::SegmentSourceDriver;

type PlanCacheKey = (String, Option<String>, ConjunctMode);
type OutputSender = Mutex<Option<oneshot::Sender<VortexResult<Option<ArrayRef>>>>>;

/// Morsels kept visible to background I/O ahead of the active workers, so the file driver sees
/// enough adjacent segments to coalesce and cold reads overlap execution.
const DEFAULT_LOOKAHEAD_MORSELS: usize = 16;

/// Pull-morsel execution backend over a raw layout and segment source.
pub struct MorselScanExecutor {
    layout: LayoutRef,
    segments: Arc<dyn SegmentSource>,
    planners: LayoutPlanners,
    target_rows: u64,
    conjunct_mode: ConjunctMode,
    threads: usize,
    lookahead_morsels: usize,
    plan_cache: Mutex<HashMap<PlanCacheKey, Arc<ExecPlan>>>,
}

impl MorselScanExecutor {
    /// Create an executor over a raw layout and its segment source.
    pub fn new(layout: LayoutRef, segments: Arc<dyn SegmentSource>) -> Self {
        Self {
            layout,
            segments,
            planners: LayoutPlanners::default(),
            target_rows: 128 * 1024,
            conjunct_mode: ConjunctMode::Cascade,
            threads: 4,
            lookahead_morsels: DEFAULT_LOOKAHEAD_MORSELS,
            plan_cache: Mutex::default(),
        }
    }

    /// Set how many morsels beyond the active window stay visible to background I/O.
    pub fn with_lookahead_morsels(mut self, morsels: usize) -> Self {
        self.lookahead_morsels = morsels;
        self
    }

    /// Plan with `planners` instead of the built-in layout planners.
    pub fn with_planners(mut self, planners: LayoutPlanners) -> Self {
        self.planners = planners;
        self
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

    /// Set the number of affinity workers used by one scan.
    pub fn with_threads(mut self, threads: usize) -> Self {
        self.threads = threads.max(1);
        self
    }

    /// Return the natural full-file row boundaries for this projection and filter.
    pub fn full_file_splits(
        &self,
        projection: &BoundExpression,
        filter: Option<&BoundExpression>,
    ) -> VortexResult<Vec<u64>> {
        let plan = self.plan(projection, filter)?;
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
        if row_offset != 0 {
            vortex_bail!("the morsel scan executor does not support row offsets");
        }
        if limit == Some(0) {
            return Ok(Vec::new());
        }

        let plan = self.plan(&projection, filter.as_ref())?;
        let full_range = row_range.unwrap_or_else(|| 0..plan.row_count());
        let mut demands =
            selected_morsels(morsels(&plan, self.target_rows), &full_range, &selection);
        // Without a filter every selected row is an output row, so the morsels past the limit
        // can be dropped before any I/O and the last one capped exactly. A filtered scan cannot
        // know where the limit falls.
        let mut row_caps = None;
        if let Some(limit) = limit
            && filter.is_none()
        {
            let mut remaining = limit;
            let mut caps = Vec::with_capacity(demands.len());
            for (_, demand) in &demands {
                if remaining == 0 {
                    break;
                }
                let rows = demand.true_count() as u64;
                caps.push(usize::try_from(rows.min(remaining)).unwrap_or(usize::MAX));
                remaining = remaining.saturating_sub(rows);
            }
            demands.truncate(caps.len());
            row_caps = Some(caps);
        }
        if demands.is_empty() {
            return Ok(Vec::new());
        }

        // One output future per morsel, completed by the scan as soon as that morsel retires, so
        // the engine can consume and parallelize over morsels instead of one whole-file result.
        // Each future carries a guard; when the last guard drops, whether because its future was
        // consumed or discarded, there is nobody left to deliver to and the scan is cancelled.
        let cancellation = ScanCancellation::new();
        let undelivered = Arc::new(AtomicUsize::new(demands.len()));
        let mut senders = Vec::with_capacity(demands.len());
        let mut outputs = Vec::with_capacity(demands.len());
        for _ in 0..demands.len() {
            let (sender, receiver) = oneshot::channel();
            senders.push(Mutex::new(Some(sender)));
            let guard = DeliveryGuard {
                undelivered: Arc::clone(&undelivered),
                cancellation: Arc::clone(&cancellation),
            };
            outputs.push(Box::pin(async move {
                let _guard = guard;
                receiver
                    .await
                    .map_err(|_| vortex_err!("morsel scan coordinator stopped"))?
            })
                as BoxFuture<'static, VortexResult<Option<ArrayRef>>>);
        }
        let senders: Arc<[OutputSender]> = Arc::from(senders);

        let driver = SegmentSourceDriver::new(Arc::clone(&self.segments));
        let handle = session.handle();
        let coordinator_handle = handle.clone();
        let driver_handle = handle.clone();
        let threads = demands.len().min(self.threads);
        let lookahead_morsels = self.lookahead_morsels;
        let sink_senders = Arc::clone(&senders);
        handle
            .spawn(async move {
                let result = coordinator_handle
                    .spawn_blocking(move || {
                        let executor = MorselExecutor::shared(Arc::clone(&plan), threads)?;
                        let scan = MorselScan::new(plan, session)
                            .with_threads(threads)
                            .with_lookahead_morsels(lookahead_morsels)
                            .with_cancellation(cancellation);
                        let scan = scan.connect(&driver, &driver_handle)?;
                        // All-true demands are a dense scan; keep them off the sparse
                        // random-access path, which localizes I/O polling per worker.
                        let scan = if demands.iter().all(|(_, demand)| demand.all_true()) {
                            scan.with_morsels(demands.into_iter().map(|(range, _)| range).collect())
                        } else {
                            scan.with_morsel_demands(demands)?
                        };
                        let scan = scan.with_completion_sink(move |index, batch| {
                            let Some(sender) = sink_senders[index].lock().take() else {
                                return;
                            };
                            let batch = match (batch, row_caps.as_ref().map(|caps| caps[index])) {
                                (Some(batch), Some(cap)) if batch.len() > cap => {
                                    batch.slice(0..cap).map(Some)
                                }
                                (batch, _) => Ok(batch),
                            };
                            drop(sender.send(batch));
                        });
                        executor.run(&scan).map(|_| ())
                    })
                    .await;
                if let Err(err) = result {
                    let message = err.to_string();
                    for sender in senders.iter() {
                        if let Some(sender) = sender.lock().take() {
                            drop(sender.send(Err(vortex_err!("morsel scan failed: {message}"))));
                        }
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
    ) -> VortexResult<Arc<ExecPlan>> {
        let projection = unbind(projection)?;
        let filter = filter.map(unbind).transpose()?;
        let key = (
            projection.to_string(),
            filter.as_ref().map(ToString::to_string),
            self.conjunct_mode,
        );
        let mut cache = self.plan_cache.lock();
        match cache.get(&key) {
            Some(plan) => Ok(Arc::clone(plan)),
            None => {
                let plan = Arc::new(self.planners.build_plan(
                    &self.layout,
                    &projection,
                    filter.as_ref(),
                    self.conjunct_mode,
                )?);
                cache.insert(key, Arc::clone(&plan));
                Ok(plan)
            }
        }
    }
}

fn selected_morsels(
    morsels: Vec<Range<u64>>,
    row_range: &Range<u64>,
    selection: &vortex_scan::selection::Selection,
) -> Vec<(Range<u64>, Mask)> {
    morsels
        .into_iter()
        .filter_map(|range| {
            let range = range.start.max(row_range.start)..range.end.min(row_range.end);
            (range.start < range.end).then_some(range)
        })
        .filter_map(|range| {
            let mask = selection.row_mask(&range).mask().clone();
            (!matches!(mask.slices(), AllOr::None)).then_some((range, mask))
        })
        .collect()
}

/// Cancels the scan when the last output future is consumed or dropped.
struct DeliveryGuard {
    undelivered: Arc<AtomicUsize>,
    cancellation: Arc<ScanCancellation>,
}

impl Drop for DeliveryGuard {
    fn drop(&mut self) {
        if self.undelivered.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.cancellation.cancel();
        }
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
