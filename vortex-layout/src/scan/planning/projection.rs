// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;
use std::sync::LazyLock;

use vortex_array::IntoArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::buffer::BufferHandle;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_io::segments::IoIntent;
use vortex_io::segments::SegmentKey;
use vortex_io::segments::SegmentRequest;
use vortex_scan::planning::morsel::Morsel;
use vortex_scan::planning::morsel::MorselOutput;
use vortex_scan::planning::planner::Planner;
use vortex_scan::planning::planner::PlannerOutput;
use vortex_scan::planning::planner::State;
use vortex_scan::planning::planner::WorkScope;
use vortex_utils::aliases::hash_set::HashSet;

use crate::scan::planning::ScanPlans;
use crate::scan::planning::SelectedRows;
use crate::scan::planning::graph::GraphStep;
use crate::scan::planning::graph::Piece;
use crate::scan::planning::graph::ProtocolGraph;
use crate::scan::planning::graph::ScanGraph;
use crate::scan::v2::prefetch::plan_segments;
use crate::scan::v2::splits::projection_splits;
use crate::segments::SegmentId;

/// Turns a filter split's selected rows into the morsels that project them.
///
/// The rows are cut into projection splits where the projection's chunks start, and each split
/// with a selected row becomes one [`ProjectionMorsel`], so a morsel reads at most one chunk of
/// every column. The planner finishes once it has handed out every morsel.
///
/// When there are multiple morsels, the planner first prefetches every segment the projection
/// reads over the selected rows, so their reads start together. Nothing asks for projection
/// segments earlier by default. Opt-in projection read-ahead can start a bounded set during
/// filtering instead.
/// Early announcements can share neighboring reads; wide projections withdraw interests
/// belonging only to empty cuts once the final mask is known.
pub struct ProjectionPlanner {
    plans: ScanPlans,
    /// The filter split's selected rows, until the first compute cuts them.
    selected: Option<SelectedRows>,
    /// The projection splits not yet handed out, last first.
    pending: Vec<SelectedRows>,
    pruned_cuts: bool,
}

impl ProjectionPlanner {
    /// Creates a planner for `selected`.
    pub fn new(plans: ScanPlans, selected: SelectedRows) -> Self {
        Self {
            plans,
            selected: Some(selected),
            pending: Vec::new(),
            pruned_cuts: false,
        }
    }

    /// Prefetches the nonempty projection chunks, which are already cut for execution.
    fn prefetch(&self) -> VortexResult<Vec<SegmentRequest>> {
        if self.pending.len() == 1 {
            // One morsel publishes its own reads before waiting; there are no later morsels
            // whose reads need to start ahead of it.
            return Ok(Vec::new());
        }
        let mut ids = Vec::new();
        for selected in &self.pending {
            plan_segments(
                &self.plans.projection,
                selected.scope.rows.clone(),
                &mut ids,
            )?;
        }
        ids.sort_unstable();
        ids.dedup();
        Ok(ids
            .into_iter()
            .map(|id| self.request(id, IoIntent::Prefetch))
            .collect())
    }

    fn request(&self, segment: SegmentId, intent: IoIntent) -> SegmentRequest {
        SegmentRequest::new(SegmentKey::new(self.plans.source, segment), intent)
    }

    /// The projection splits of `selected` that have a selected row, last first.
    fn cut(&mut self, selected: &SelectedRows) -> Vec<SelectedRows> {
        let SelectedRows { scope, mask } = selected;
        let start = scope.rows.start;
        let index = |row: u64| usize::try_from(row - start).vortex_expect("split row fits usize");
        let mut pruned_cuts = false;
        let mut splits = projection_splits(&self.plans.projection_starts, scope.rows.clone())
            .into_iter()
            .filter_map(|rows| {
                let mask = mask.slice(index(rows.start)..index(rows.end));
                if mask.all_false() {
                    pruned_cuts = true;
                    return None;
                }
                Some(SelectedRows {
                    scope: WorkScope {
                        file_ordinal: scope.file_ordinal,
                        rows,
                    },
                    mask,
                })
            })
            .collect::<Vec<_>>();
        splits.reverse();
        self.pruned_cuts = pruned_cuts;
        splits
    }

    fn forget_pruned(
        &self,
        selected: &SelectedRows,
        mut prefetch: Vec<SegmentRequest>,
    ) -> VortexResult<Vec<SegmentRequest>> {
        if !self.pruned_cuts {
            return Ok(prefetch);
        }
        let mut kept: HashSet<_> = prefetch.iter().map(|request| request.key.segment).collect();
        if let [selected] = self.pending.as_slice() {
            // A single morsel submits its own reads, so no prefetch batch protects them.
            let mut ids = Vec::new();
            plan_segments(
                &self.plans.projection,
                selected.scope.rows.clone(),
                &mut ids,
            )?;
            kept.extend(ids);
        }
        let mut ids = Vec::new();
        plan_segments(
            &self.plans.projection,
            selected.scope.rows.clone(),
            &mut ids,
        )?;
        ids.sort_unstable();
        ids.dedup();
        let mut forget: Vec<_> = ids
            .into_iter()
            .filter(|id| !kept.contains(id))
            .map(|id| self.request(id, IoIntent::Forget))
            .collect();
        // Withdraw stale neighbors before the selected projection reads become eligible.
        forget.append(&mut prefetch);
        Ok(forget)
    }
}

impl Planner for ProjectionPlanner {
    fn state(&self) -> State {
        if self.selected.is_none() && self.pending.is_empty() {
            State::Done
        } else {
            State::NeedsCompute
        }
    }

    fn compute(&mut self) -> VortexResult<PlannerOutput> {
        static FORGET_PRUNED: LazyLock<Option<bool>> = LazyLock::new(|| {
            std::env::var("VORTEX_SCAN_IO_FORGET_PRUNED_PROJECTION")
                .ok()
                .map(|value| value == "1")
        });
        if let Some(selected) = self.selected.take() {
            tracing::debug!(
                target: "vortex_layout::filter_stages",
                row_start = selected.scope.rows.start,
                row_end = selected.scope.rows.end,
                selected_rows = selected.mask.true_count(),
                "projection mask ready"
            );
            self.pending = self.cut(&selected);
            // No projection split has a selected row.
            if self.pending.is_empty() {
                return Ok(PlannerOutput::Done);
            }
            let wide = self
                .plans
                .projection
                .dtype()
                .as_struct_fields_opt()
                .is_some_and(|fields| fields.nfields() >= 8);
            let forget = (*FORGET_PRUNED).unwrap_or(wide);
            let prefetch = self.prefetch()?;
            let prefetch = if forget {
                self.forget_pruned(&selected, prefetch)?
            } else {
                prefetch
            };
            if !prefetch.is_empty() {
                return Ok(PlannerOutput::NeedsIO(prefetch));
            }
        }
        let Some(selected) = self.pending.pop() else {
            vortex_bail!("ProjectionPlanner: compute called after Done");
        };
        let scope = selected.scope.clone();
        Ok(PlannerOutput::Morsel(
            scope,
            Box::new(ProjectionMorsel::new(self.plans.clone(), selected)),
        ))
    }

    /// Prefetches are never delivered.
    fn deliver(&mut self, _key: SegmentKey, _bytes: BufferHandle) {}
}

/// Runs the projection plan over a split's selected rows and returns them as one batch, in row
/// order. A selection that projects to no rows finishes without a batch.
pub struct ProjectionMorsel {
    plans: ScanPlans,
    selected: SelectedRows,
    graph: Option<ProtocolGraph>,
    pieces: Vec<Piece>,
    done: bool,
}

impl ProjectionMorsel {
    /// Creates a morsel projecting `selected`.
    pub fn new(plans: ScanPlans, selected: SelectedRows) -> Self {
        Self {
            plans,
            selected,
            graph: None,
            pieces: Vec::new(),
            done: false,
        }
    }

    fn finish(&mut self) -> VortexResult<MorselOutput> {
        self.done = true;
        self.pieces.sort_by_key(|piece| piece.rows.start);
        let mut arrays: Vec<_> = std::mem::take(&mut self.pieces)
            .into_iter()
            .map(|piece| piece.array)
            .filter(|array| !array.is_empty())
            .collect();
        let array = match arrays.len() {
            0 => return Ok(MorselOutput::Done),
            1 => arrays.remove(0),
            _ => ChunkedArray::try_new(arrays, self.plans.projection.dtype().clone())?.into_array(),
        };
        Ok(MorselOutput::Batch(array))
    }
}

impl Morsel for ProjectionMorsel {
    fn state(&self) -> State {
        match &self.graph {
            _ if self.done => State::Done,
            None => State::NeedsCompute,
            Some(graph) => match graph.state() {
                State::Done => State::NeedsCompute,
                state => state,
            },
        }
    }

    fn compute(&mut self) -> VortexResult<MorselOutput> {
        if self.done {
            vortex_bail!("ProjectionMorsel: compute called after Done");
        }
        let Some(graph) = self.graph.as_mut() else {
            let graph = ScanGraph::try_new(
                self.plans.session.clone(),
                &self.plans.projection,
                self.selected.scope.rows.clone(),
                self.selected.mask.clone(),
                self.plans.row_offset,
                self.plans.values.clone(),
            )?;
            self.graph = Some(ProtocolGraph::new(
                graph,
                self.plans.source,
                Arc::clone(&self.plans.locations),
            ));
            return Ok(MorselOutput::Continue);
        };
        if graph.state() == State::Done {
            return self.finish();
        }
        Ok(match graph.compute()? {
            GraphStep::Yield => MorselOutput::Continue,
            GraphStep::NeedsIO(batch) => {
                if tracing::enabled!(target: "vortex_layout::filter_stages", tracing::Level::DEBUG)
                {
                    for request in &batch {
                        tracing::debug!(
                            target: "vortex_layout::filter_stages",
                            row_start = self.selected.scope.rows.start,
                            row_end = self.selected.scope.rows.end,
                            segment = *request.key.segment,
                            "projection input requested"
                        );
                    }
                }
                MorselOutput::NeedsIO(batch)
            }
            GraphStep::Piece(piece) => {
                self.pieces.push(piece);
                MorselOutput::Continue
            }
        })
    }

    fn deliver(&mut self, key: SegmentKey, bytes: BufferHandle) {
        if let Some(graph) = self.graph.as_mut() {
            graph.deliver(key, bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use vortex_array::dtype::DType;
    use vortex_array::dtype::Nullability::NonNullable;
    use vortex_array::dtype::PType;
    use vortex_array::dtype::StructFields;
    use vortex_buffer::Alignment;
    use vortex_io::segments::SourceId;
    use vortex_mask::Mask;
    use vortex_session::registry::ReadContext;

    use super::*;
    use crate::plan::physical::ConcatPlan;
    use crate::plan::physical::FilterPlan;
    use crate::plan::physical::PackPlan;
    use crate::plan::physical::SegmentScanPlan;
    use crate::scan::planning::SegmentLocation;
    use crate::test::new_session;

    #[test]
    fn single_morsel_withdraws_pruned_chunks() -> VortexResult<()> {
        let dtype = DType::Primitive(PType::I32, NonNullable);
        let chunks = (0..2)
            .map(|segment| {
                FilterPlan::new(
                    SegmentScanPlan::new(
                        dtype.clone(),
                        1000,
                        SegmentId::from(segment),
                        ReadContext::new([]),
                        None,
                    )
                    .into_plan(),
                )
                .into_plan()
            })
            .collect();
        let locations: Arc<[_]> = (0..2)
            .map(|offset| SegmentLocation {
                offset,
                length: 1,
                alignment: Alignment::none(),
            })
            .collect();
        let selected = SelectedRows {
            scope: WorkScope {
                file_ordinal: 0,
                rows: 0..2000,
            },
            mask: Mask::from_iter((0..2000).map(|row| row < 1000)),
        };
        let mut planner = ProjectionPlanner::new(
            ScanPlans {
                session: new_session(),
                source: SourceId(0),
                locations: Arc::clone(&locations),
                projection: ConcatPlan::try_new(dtype, chunks)?.into_plan(),
                projection_starts: Arc::from([0, 1000]),
                row_offset: 0,
                values: Default::default(),
            },
            SelectedRows {
                scope: selected.scope.clone(),
                mask: selected.mask.clone(),
            },
        );
        planner.pending = planner.cut(&selected);
        assert_eq!(planner.pending.len(), 1);
        let prefetch = planner.prefetch()?;
        assert!(prefetch.is_empty());
        let withdrawal = planner.forget_pruned(&selected, prefetch)?;
        let segments: Vec<_> = withdrawal
            .iter()
            .map(|request| request.key.segment)
            .collect();
        assert_eq!(segments, vec![SegmentId::from(1)]);
        Ok(())
    }

    #[rstest::rstest]
    fn prefetch_skips_empty_chunks_without_enumerating_row_runs(
        #[values(0, 2, 8)] fields: usize,
    ) -> VortexResult<()> {
        let dtype = DType::Primitive(PType::I32, NonNullable);
        let chunks = (0..3)
            .map(|segment| {
                FilterPlan::new(
                    SegmentScanPlan::new(
                        dtype.clone(),
                        1000,
                        SegmentId::from(segment),
                        ReadContext::new([]),
                        None,
                    )
                    .into_plan(),
                )
                .into_plan()
            })
            .collect();
        let locations: Arc<[_]> = (0..3)
            .map(|offset| SegmentLocation {
                offset,
                length: 1,
                alignment: Alignment::none(),
            })
            .collect();
        // More than 64 runs used to fall back to prefetching the entire bounding range,
        // including the empty middle chunk, after enumerating every run.
        let mask =
            Mask::from_iter((0..3000).map(|row| row % 2 == 0 && !(1000..2000).contains(&row)));
        let projection = ConcatPlan::try_new(dtype.clone(), chunks)?.into_plan();
        let projection = if fields == 0 {
            projection
        } else {
            PackPlan::try_new(
                StructFields::from_iter(
                    (0..fields).map(|index| (format!("f{index}"), dtype.clone())),
                ),
                NonNullable,
                3000,
                vec![projection; fields],
                None,
            )?
            .into_plan()
        };
        let mut planner = ProjectionPlanner::new(
            ScanPlans {
                session: new_session(),
                source: SourceId(0),
                locations: Arc::clone(&locations),
                projection,
                projection_starts: Arc::from([0, 1000, 2000]),
                row_offset: 0,
                values: Default::default(),
            },
            SelectedRows {
                scope: WorkScope {
                    file_ordinal: 0,
                    rows: 0..3000,
                },
                mask: mask.clone(),
            },
        );
        let PlannerOutput::NeedsIO(requests) = planner.compute()? else {
            vortex_bail!("expected projection prefetch");
        };
        let forgotten = std::env::var("VORTEX_SCAN_IO_FORGET_PRUNED_PROJECTION")
            .ok()
            .map(|value| value == "1")
            .unwrap_or(fields >= 8);
        assert_eq!(planner.pending.len(), 2);
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.intent == IoIntent::Forget)
                .count(),
            usize::from(forgotten)
        );
        let prefetch: Vec<_> = requests
            .into_iter()
            .filter(|request| request.intent == IoIntent::Prefetch)
            .collect();
        let segments: Vec<_> = prefetch.iter().map(|request| request.key.segment).collect();
        assert_eq!(segments, vec![SegmentId::from(0), SegmentId::from(2)]);
        let pruned = planner.forget_pruned(
            &SelectedRows {
                scope: WorkScope {
                    file_ordinal: 0,
                    rows: 0..3000,
                },
                mask: mask.clone(),
            },
            prefetch,
        )?;
        let forgotten: Vec<_> = pruned
            .iter()
            .filter(|request| request.intent == IoIntent::Forget)
            .map(|request| request.key.segment)
            .collect();
        assert_eq!(forgotten, vec![SegmentId::from(1)]);
        assert_eq!(
            pruned.first().map(|request| request.intent),
            Some(IoIntent::Forget)
        );
        let Mask::Values(values) = mask else {
            vortex_bail!("expected a partial mask");
        };
        assert!(values.cached_slices().is_none());
        Ok(())
    }
}
