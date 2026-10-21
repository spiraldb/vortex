// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;

use bit_vec::BitVec;
use vortex_array::ArrayRef;
use vortex_array::VortexSessionExecute;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::expr::BoundExpression;
use vortex_array::expr::transform::find_between;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure_eq;
use vortex_io::request::IoBatch;
use vortex_io::request::IoConsumer;
use vortex_io::request::IoIntent;
use vortex_io::request::IoRequest;
use vortex_io::request::IoRequestId;
use vortex_io::request::IoResult;
use vortex_mask::Mask;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_session::VortexSession;

use super::ScanPlans;
use super::SegmentLocation;
use super::SelectedRows;
use super::adaptor::AdaptorStep;
use super::adaptor::ScanAdaptor;
use super::projection::ProjectionPlanner;
use crate::plan::physical::FilterPlan;
use crate::plan::physical::PlanRef;
use crate::plan::physical::SegmentScan;
use crate::plan::physical::Zoned;
use crate::plan::physical::optimize;
use crate::plan::physical::pipeline::Scan;
use crate::plan::physical::pipeline::Split;
use crate::plan::physical::plan_row_idx_expression;
use crate::plan::physical::visit_segments;
use crate::scan::filter::FilterExpr;
use crate::segments::SegmentId;

/// A predicate a scan cannot express as a Vortex expression, such as a join's filter on keys
/// it has seen, evaluated on the rows the scan's own conjuncts kept and before the projection
/// decodes them.
pub trait RowFilter: Send + Sync + 'static {
    /// The columns the filter reads, bound to the scanned file's dtype.
    fn projection(&self) -> &BoundExpression;

    /// Whether the filter is still worth evaluating. A split that starts once it is not reads
    /// none of the filter's columns. Every row passes a filter that is not evaluated, so a
    /// filter may stop once it rarely removes rows.
    fn is_active(&self) -> bool {
        true
    }

    /// Selects rows of `array`, the filter's projection over rows the scan kept, returning a
    /// mask as long as `array`.
    fn evaluate(&self, array: ArrayRef) -> VortexResult<Mask>;
}

/// Plans and adaptive conjunct ordering shared by all splits of a scan.
pub(in crate::scan) struct FilterPlans {
    expression: Option<FilterExpr>,
    predicates: Vec<PlanRef>,
    pruning: Vec<Option<PlanRef>>,
    /// Row filters and the plans of the columns they read, run after the conjuncts.
    row_filters: Vec<(Arc<dyn RowFilter>, PlanRef)>,
}

impl FilterPlans {
    pub(in crate::scan) fn new(
        expression: Option<BoundExpression>,
        row_filters: Vec<Arc<dyn RowFilter>>,
        root: &PlanRef,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let expression = expression
            .map(|expression| VortexResult::Ok(FilterExpr::new(find_between(expression)?)))
            .transpose()?;
        let conjuncts = expression.as_ref().map_or(&[][..], FilterExpr::conjuncts);
        let predicates = conjuncts
            .iter()
            .map(|expr| {
                let plan = optimize(plan_row_idx_expression(expr.clone(), root.clone())?)?;
                Ok(FilterPlan::new(plan).into_plan())
            })
            .collect::<VortexResult<_>>()?;
        let pruning = conjuncts
            .iter()
            .map(|expr| {
                expr.falsify(session)?
                    .map(|proof| optimize(plan_row_idx_expression(proof, root.clone())?))
                    .transpose()
            })
            .map(|plan| {
                let plan = plan?;
                match plan {
                    Some(plan) if metadata_only(&plan)? => Ok(Some(plan)),
                    _ => Ok(None),
                }
            })
            .collect::<VortexResult<_>>()?;
        let row_filters = row_filters
            .into_iter()
            .map(|filter| {
                let plan = optimize(plan_row_idx_expression(
                    filter.projection().clone(),
                    root.clone(),
                )?)?;
                Ok((filter, plan))
            })
            .collect::<VortexResult<_>>()?;
        Ok(Self {
            expression,
            predicates,
            pruning,
            row_filters,
        })
    }
}

/// A pruning proof must not fetch data when its requested statistics are unavailable.
fn metadata_only(plan: &PlanRef) -> VortexResult<bool> {
    if plan
        .as_opt::<Zoned>()
        .is_some_and(|zoned| zoned.is_pruning())
    {
        return Ok(true);
    }
    if plan.is::<SegmentScan>() {
        return Ok(false);
    }
    for child in plan.children().iter() {
        if !metadata_only(&child?)? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Proves zone exclusions, evaluates conjuncts over survivors, runs the row filters over what
/// the conjuncts kept, then hands the mask to projection.
///
/// Reads are started ahead of the stage that needs them: zone maps on the first compute, and
/// the conjuncts' segments for the rows pruning kept once the proofs are in. Stages then run
/// back to back instead of each waiting a round trip for its reads.
pub(super) struct FilterPlanner {
    plans: ScanPlans,
    selected: Option<SelectedRows>,
    filter: Arc<FilterPlans>,
    remaining: BitVec,
    prune_index: usize,
    row_filter_index: usize,
    active: Option<(Stage, ScanAdaptor)>,
    pieces: Vec<Mask>,
    announced: bool,
    prefetched: bool,
    /// Ids for hints, counting down from `u32::MAX` so they never meet an adaptor's fetch ids.
    next_hint: u32,
}

/// The filter stage a [`FilterPlanner`] is running.
#[derive(Clone, Copy, Debug)]
enum Stage {
    /// A pruning proof, whose rows are excluded.
    Pruning,
    /// Conjunct `index`.
    Conjunct(usize),
    /// Row filter `index`.
    Row(usize),
}

/// The segments `plans` read over `rows`, under `mask`, each once, in plan order.
fn segments_read<'a>(
    plans: impl Iterator<Item = &'a PlanRef>,
    rows: Range<u64>,
    mask: Option<&Mask>,
) -> VortexResult<Vec<SegmentId>> {
    let mut segments = Vec::new();
    for plan in plans {
        visit_segments(plan, rows.clone(), mask, &mut |id, _| segments.push(id))?;
    }
    segments.sort_unstable();
    segments.dedup();
    Ok(segments)
}

impl FilterPlanner {
    pub(super) fn new(plans: ScanPlans, selected: SelectedRows) -> Self {
        let filter = Arc::clone(plans.filter.as_ref().vortex_expect("filter plans exist"));
        Self {
            remaining: BitVec::from_elem(filter.predicates.len(), true),
            plans,
            selected: Some(selected),
            filter,
            prune_index: 0,
            row_filter_index: 0,
            active: None,
            pieces: Vec::new(),
            announced: false,
            prefetched: false,
            next_hint: u32::MAX,
        }
    }
}

/// Publishes a hint for each of `segments` not already in `batch`.
fn hint(
    locations: &[SegmentLocation],
    next_hint: &mut u32,
    intent: IoIntent,
    segments: &[SegmentId],
    batch: &mut IoBatch,
) {
    for segment in segments {
        let target = locations[**segment as usize].target();
        if batch.iter().any(|request| request.target == target) {
            continue;
        }
        batch.push(IoRequest {
            intent,
            request: IoRequestId(*next_hint),
            target,
        });
        *next_hint -= 1;
    }
}

impl IoConsumer for FilterPlanner {
    fn set_io_result(&mut self, request: IoRequestId, result: IoResult) {
        if let Some((_, adaptor)) = &mut self.active {
            adaptor.set_io_result(request, result);
        }
    }
}

impl Planner for FilterPlanner {
    fn state(&self) -> State {
        if self.selected.is_none() {
            return State::Done;
        }
        match &self.active {
            Some((_, adaptor)) if adaptor.state() != State::Done => adaptor.state(),
            _ => State::NeedsCompute,
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        let Some(selected) = &mut self.selected else {
            return Ok(PlannerOutput::Done);
        };
        if !self.announced {
            self.announced = true;
            let rows = selected.scope.rows.clone();
            // Pruning proofs read only zone maps and run first: start those reads now. The rest
            // is announced so the proofs' reads can coalesce with it.
            let proofs = segments_read(self.filter.pruning.iter().flatten(), rows.clone(), None)?;
            let rest = segments_read(
                self.filter
                    .predicates
                    .iter()
                    .chain(self.filter.row_filters.iter().map(|(_, plan)| plan))
                    .chain(std::iter::once(&self.plans.projection)),
                rows,
                None,
            )?;
            let mut batch = Vec::new();
            let locations = &self.plans.locations;
            hint(
                locations,
                &mut self.next_hint,
                IoIntent::Prefetch,
                &proofs,
                &mut batch,
            );
            hint(
                locations,
                &mut self.next_hint,
                IoIntent::Announce,
                &rest,
                &mut batch,
            );
            if !batch.is_empty() {
                return Ok(PlannerOutput::NeedsIO(batch));
            }
        }
        if let Some((stage, adaptor)) = &mut self.active {
            if adaptor.state() != State::Done {
                return Ok(match adaptor.compute()? {
                    AdaptorStep::Yield => PlannerOutput::Continue,
                    AdaptorStep::NeedsIO(batch) => PlannerOutput::NeedsIO(batch),
                    AdaptorStep::Piece(piece) => {
                        let mask = match *stage {
                            Stage::Row(index) => {
                                self.filter.row_filters[index].0.evaluate(piece)?
                            }
                            Stage::Pruning | Stage::Conjunct(_) => {
                                let mut ctx = self.plans.session.create_execution_ctx();
                                piece.fill_null(false)?.execute::<Mask>(&mut ctx)?
                            }
                        };
                        self.pieces.push(mask);
                        PlannerOutput::Continue
                    }
                });
            }
            let input = selected.mask.true_count();
            let result = Mask::concat(self.pieces.iter())?;
            vortex_ensure_eq!(result.len(), input);
            let result = if matches!(stage, Stage::Pruning) {
                !result
            } else {
                result
            };
            selected.mask = selected.mask.intersect_by_rank(&result);
            tracing::debug!(
                row_start = selected.scope.rows.start,
                row_end = selected.scope.rows.end,
                input_rows = input,
                output_rows = selected.mask.true_count(),
                stage = ?stage,
                "v2 filter stage evaluated"
            );
            if let (Stage::Conjunct(index), Some(expression)) = (*stage, &self.filter.expression) {
                expression
                    .report_selectivity(index, selected.mask.true_count() as f64 / input as f64);
            }
            self.active = None;
            self.pieces.clear();
        }
        if selected.mask.all_false() {
            self.selected = None;
            return Ok(PlannerOutput::Done);
        }
        if !self.prefetched
            && !self.filter.pruning[self.prune_index..]
                .iter()
                .any(Option::is_some)
        {
            // Every proof is in: start the conjuncts' reads over the chunks that survived, so
            // each conjunct finds its bytes ready. The projection's reads wait for its morsels,
            // which fetch together once the conjuncts have settled which chunks they need.
            self.prefetched = true;
            let rows = selected.scope.rows.clone();
            let mask = (!selected.mask.all_true()).then_some(&selected.mask);
            let predicates = segments_read(
                self.filter
                    .predicates
                    .iter()
                    .enumerate()
                    .filter(|(index, _)| self.remaining[*index])
                    .map(|(_, plan)| plan)
                    .chain(
                        self.filter
                            .row_filters
                            .iter()
                            .filter(|(filter, _)| filter.is_active())
                            .map(|(_, plan)| plan),
                    ),
                rows,
                mask,
            )?;
            let mut batch = Vec::new();
            hint(
                &self.plans.locations,
                &mut self.next_hint,
                IoIntent::Prefetch,
                &predicates,
                &mut batch,
            );
            if !batch.is_empty() {
                return Ok(PlannerOutput::NeedsIO(batch));
            }
        }

        let mut next = None;
        while self.prune_index < self.filter.pruning.len() {
            let index = self.prune_index;
            self.prune_index += 1;
            if let Some(plan) = &self.filter.pruning[index] {
                next = Some((Stage::Pruning, plan.clone()));
                break;
            }
        }
        if next.is_none()
            && let Some(index) = self
                .filter
                .expression
                .as_ref()
                .and_then(|expression| expression.next_conjunct(&self.remaining))
        {
            self.remaining.set(index, false);
            next = Some((
                Stage::Conjunct(index),
                self.filter.predicates[index].clone(),
            ));
        }
        while next.is_none() && self.row_filter_index < self.filter.row_filters.len() {
            let index = self.row_filter_index;
            self.row_filter_index += 1;
            let (filter, plan) = &self.filter.row_filters[index];
            if filter.is_active() {
                next = Some((Stage::Row(index), plan.clone()));
            }
        }
        if let Some((stage, plan)) = next {
            let scan = Scan::try_new(
                self.plans.session.clone(),
                plan,
                vec![Split {
                    rows: selected.scope.rows.clone(),
                    mask: selected.mask.clone(),
                }],
            )?
            .with_row_offset(self.plans.row_offset)
            .with_value_cache(self.plans.values.clone());
            self.active = Some((
                stage,
                ScanAdaptor::new(scan, Arc::clone(&self.plans.locations)),
            ));
            return Ok(PlannerOutput::Continue);
        }
        let selected = self.selected.take().vortex_expect("selected rows exist");
        Ok(PlannerOutput::Planner(
            selected.scope.clone(),
            Box::new(ProjectionPlanner::new(self.plans.clone(), selected)),
        ))
    }
}
