// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Benchmark prototype: build list dependencies upfront and resolve selection at runtime.
//!
//! Chunk dispatchers wait for offsets and validity, then activate only selected children in the
//! same scheduler. Unselected chunks need no runtime sources or ports.
//!
//! Enabled only by `VORTEX_LIST_PIPELINE`. Supports primitive and nested-list fixture plans;
//! unsupported plans fail explicitly. This experiment does not implement the planner's decoded-work
//! admission budget or shared segment optimization.

use std::ops::Range;
use std::sync::Arc;
use std::sync::OnceLock;

use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::DType;
use vortex_array::dtype::PType;
use vortex_array::expr::stats::Precision;
use vortex_array::expr::stats::Stat;
use vortex_array::validity::Validity;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_mask::AllOr;
use vortex_mask::Mask;

use super::selected_ranges;
use crate::plan::physical::Concat;
use crate::plan::physical::ConcatPlan;
use crate::plan::physical::Filter;
use crate::plan::physical::LegacyStats;
use crate::plan::physical::ListPack;
use crate::plan::physical::ListPackPlan;
use crate::plan::physical::PlanRef;
use crate::plan::physical::SegmentScan;
use crate::plan::physical::SegmentScanPlan;
use crate::plan::physical::Zoned;
use crate::plan::physical::operator::Cx;
use crate::plan::physical::operator::Input;
use crate::plan::physical::operator::Operator;
use crate::plan::physical::operator::Source;
use crate::plan::physical::operator::SourceBlocked;
use crate::plan::physical::operator::SourceReady;
use crate::plan::physical::operator::Step;
use crate::plan::physical::operators::ConcatSource;
use crate::plan::physical::operators::ScanSource;
use crate::plan::physical::operators::UNBOUNDED;
use crate::plan::physical::operators::join_inlet;
use crate::plan::physical::pipeline::Chain;
use crate::plan::physical::pipeline::Compiler;
use crate::plan::physical::pipeline::overlapping;

#[derive(Clone)]
struct Demand {
    ranges: Arc<OnceLock<Vec<Range<u64>>>>,
    gate: Option<usize>,
    domain: Range<u64>,
    offset: u64,
    boundaries: bool,
}

impl Demand {
    fn ranges(&self, len: u64) -> Option<Vec<Range<u64>>> {
        let selected = self.ranges.get()?;
        let mut result: Vec<Range<u64>> = Vec::new();
        let first = selected.partition_point(|range| {
            range.end <= self.domain.start
                || range.end.saturating_add(u64::from(self.boundaries)) <= self.offset
        });
        for range in &selected[first..] {
            if range.start >= self.domain.end || range.start >= self.offset + len {
                break;
            }
            let start = range.start.max(self.domain.start);
            let end = range.end.min(self.domain.end);
            if start >= end {
                continue;
            }
            let end = end + u64::from(self.boundaries);
            let start = start.max(self.offset);
            let end = end.min(self.offset + len);
            if start >= end {
                continue;
            }
            let range = start - self.offset..end - self.offset;
            if let Some(last) = result.last_mut()
                && last.end >= range.start
            {
                last.end = last.end.max(range.end);
            } else {
                result.push(range);
            }
        }
        Some(result)
    }

    fn blocked(&self) -> VortexResult<SourceReady> {
        Ok(SourceReady::Blocked(SourceBlocked::Selection(
            self.gate
                .ok_or_else(|| vortex_err!("Unresolved demand has no producer"))?,
        )))
    }

    fn shifted(&self, offset: u64) -> Self {
        Self {
            offset: self.offset + offset,
            ..self.clone()
        }
    }

    fn boundaries(&self, rows: u64) -> Self {
        Self {
            domain: self.offset..self.offset + rows,
            boundaries: true,
            ..self.clone()
        }
    }
}

pub(crate) fn compile_deferred_list(
    plan: &ListPackPlan,
    rows: Range<u64>,
    mask: &Mask,
    compiler: &mut Compiler<'_>,
) -> VortexResult<Chain> {
    let ranges = match mask.slices() {
        AllOr::All => vec![rows],
        AllOr::None => Vec::new(),
        AllOr::Some(slices) => slices
            .iter()
            .map(|&(start, end)| rows.start + start as u64..rows.start + end as u64)
            .collect(),
    };
    let demand = Demand {
        ranges: Arc::new(OnceLock::from(ranges)),
        gate: None,
        domain: 0..plan.row_count(),
        offset: 0,
        boundaries: false,
    };
    compile_list(plan, demand, compiler)
}

fn compile_list(
    plan: &ListPackPlan,
    demand: Demand,
    compiler: &mut Compiler<'_>,
) -> VortexResult<Chain> {
    let offsets = compile(
        &plan.offsets()?,
        demand.boundaries(plan.row_count()),
        compiler,
    )?;
    let mut chains = vec![offsets];
    if let Some(validity) = plan.validity()? {
        chains.push(compile(&validity, demand.clone(), compiler)?);
    }
    let elements = plan.elements()?;
    let element_demand = Demand {
        ranges: Arc::new(OnceLock::new()),
        gate: Some(compiler.selection_gate()),
        domain: 0..elements.row_count(),
        offset: 0,
        boundaries: false,
    };
    chains.push(compile(&elements, element_demand.clone(), compiler)?);
    let source = DeferredList {
        plan: plan.clone(),
        demand,
        elements: element_demand,
        assembled: None,
        done: false,
    };
    Ok(compiler.join_with(chains, |_| UNBOUNDED, source))
}

fn compile(plan: &PlanRef, demand: Demand, compiler: &mut Compiler<'_>) -> VortexResult<Chain> {
    if let Some(filter) = plan.as_opt::<Filter>() {
        return compile(&filter.child_plan()?, demand, compiler);
    }
    if let Some(zoned) = plan.as_opt::<Zoned>() {
        let data = zoned
            .data_plan()?
            .ok_or_else(|| vortex_err!("Zoned data is absent"))?;
        return compile(&data, demand, compiler);
    }
    if let Some(stats) = plan.as_opt::<LegacyStats>() {
        return compile(&stats.data_plan()?, demand, compiler);
    }
    if let Some(concat) = plan.as_opt::<Concat>() {
        let mut source = DeferredConcat {
            plan: concat.clone(),
            demand,
        };
        return if source.demand.ranges.get().is_some() {
            source.activate(compiler)
        } else {
            Ok(Chain::new(source))
        };
    }
    if let Some(list) = plan.as_opt::<ListPack>() {
        return compile_list(list, demand, compiler);
    }
    if let Some(scan) = plan.as_opt::<SegmentScan>() {
        return Ok(Chain::new(DeferredScan {
            plan: scan.clone(),
            demand,
            source: None,
            empty: false,
        }));
    }
    vortex_bail!(
        "Deferred-list benchmark prototype cannot compile {}",
        plan.id()
    )
}

/// Keep one dispatcher while selection is unknown; lower only selected children afterwards.
struct DeferredConcat {
    plan: ConcatPlan,
    demand: Demand,
}

impl Source for DeferredConcat {
    fn ready(&mut self, _cx: &mut Cx<'_>) -> VortexResult<SourceReady> {
        if self.demand.ranges.get().is_some() {
            Ok(SourceReady::Activate)
        } else {
            self.demand.blocked()
        }
    }

    fn activate(&mut self, compiler: &mut Compiler<'_>) -> VortexResult<Chain> {
        let ranges = self
            .demand
            .ranges(self.plan.row_count())
            .ok_or_else(|| vortex_err!("Chunks activated before their selection"))?;
        let mut chains = Vec::new();
        let mut previous = None;
        for range in ranges {
            for (index, start, _) in overlapping(&self.plan, &range) {
                // Several selected ranges can lie in one chunk. Give that chunk one source,
                // which applies their combined selection and reads its segment only once.
                if previous == Some(index) {
                    continue;
                }
                previous = Some(index);
                chains.push(compile(
                    &self.plan.child_required(index)?,
                    self.demand.shifted(start),
                    compiler,
                )?);
            }
        }
        let count = chains.len();
        if count == 1 {
            return Ok(chains.remove(0));
        }
        Ok(compiler.join(chains, ConcatSource::new(count)))
    }
}

impl Operator for DeferredConcat {
    fn compute(&mut self, _input: Input, _cx: &mut Cx<'_>) -> VortexResult<Step> {
        vortex_bail!("Chunk dispatcher computed before activation")
    }
}

struct DeferredScan {
    plan: SegmentScanPlan,
    demand: Demand,
    source: Option<ScanSource>,
    empty: bool,
}

impl Source for DeferredScan {
    fn ready(&mut self, cx: &mut Cx<'_>) -> VortexResult<SourceReady> {
        if self.empty {
            return Ok(SourceReady::Ready);
        }
        if self.source.is_none() {
            let Some(ranges) = self.demand.ranges(self.plan.row_count()) else {
                return self.demand.blocked();
            };
            let Some(first) = ranges.first() else {
                self.empty = true;
                return Ok(SourceReady::Ready);
            };
            let start = first.start;
            let end = ranges
                .last()
                .ok_or_else(|| vortex_err!("Missing selected range"))?
                .end;
            let len = usize::try_from(end - start)?;
            let mask = if ranges.len() == 1 {
                None
            } else {
                Some(Mask::from_slices(
                    len,
                    ranges
                        .iter()
                        .map(|range| {
                            Ok((
                                usize::try_from(range.start - start)?,
                                usize::try_from(range.end - start)?,
                            ))
                        })
                        .collect::<VortexResult<Vec<_>>>()?,
                ))
            };
            let slice = if start == 0 && end == self.plan.row_count() {
                None
            } else {
                Some(usize::try_from(start)?..usize::try_from(end)?)
            };
            self.source = Some(ScanSource::new(self.plan.clone(), slice, mask));
        }
        self.source
            .as_mut()
            .ok_or_else(|| vortex_err!("Missing segment source"))?
            .ready(cx)
    }
}

impl Operator for DeferredScan {
    fn compute(&mut self, input: Input, cx: &mut Cx<'_>) -> VortexResult<Step> {
        match self.source.as_mut() {
            Some(source) => source.compute(input, cx),
            None if self.empty => Ok(Step::End),
            None => vortex_bail!("Segment computed before its selection"),
        }
    }
}

struct DeferredList {
    plan: ListPackPlan,
    demand: Demand,
    elements: Demand,
    assembled: Option<(ArrayRef, Validity)>,
    done: bool,
}

impl Source for DeferredList {
    fn ready(&mut self, cx: &mut Cx<'_>) -> VortexResult<SourceReady> {
        if self.done {
            return Ok(SourceReady::Ready);
        }
        let element_inlet = 1 + usize::from(self.plan.dtype().is_nullable());
        if self.assembled.is_none() {
            let Some(selected) = self.demand.ranges(self.plan.row_count()) else {
                return self.demand.blocked();
            };
            for index in 0..element_inlet {
                if !cx.inlet(index).closed() {
                    return Ok(SourceReady::Blocked(SourceBlocked::Inlet(index)));
                }
            }
            let offsets = join_inlet(cx, 0, self.plan.offsets()?.dtype())?
                .cast(DType::Primitive(PType::U64, false.into()))?
                .execute::<PrimitiveArray>(cx.exec())?;
            let validity = if self.plan.dtype().is_nullable() {
                Validity::Array(join_inlet(cx, 1, &DType::Bool(false.into()))?)
            } else {
                Validity::NonNullable
            };
            let count = selected
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>();
            let valid = validity.execute_mask(usize::try_from(count)?, cx.exec())?;
            let selected = selected
                .into_iter()
                .map(|range| Ok(usize::try_from(range.start)?..usize::try_from(range.end)?))
                .collect::<VortexResult<Vec<_>>>()?;
            let selected_indices = selected.iter().flat_map(|range| range.clone());
            let (mut selected_offsets, mut ranges) = selected_ranges(
                selected_indices,
                offsets.as_slice::<u64>(),
                &valid,
                self.plan.elements()?.row_count(),
            )?;
            if selected.len() == 1
                && !valid.all_true()
                && let Some(retained) = super::work::preserve_null_span(
                    self.plan.elements()?.dtype(),
                    offsets.as_slice::<u64>(),
                    selected_offsets.last().copied().unwrap_or(0),
                    &mut ranges,
                )
            {
                selected_offsets = retained;
            }
            let last = selected_offsets.last().copied().unwrap_or(0);
            let offsets = selected_offsets.into_array();
            let stats = offsets.statistics();
            stats.set(Stat::IsSorted, Precision::Exact(true.into()));
            stats.set(Stat::Min, Precision::Exact(0_u64.into()));
            stats.set(Stat::Max, Precision::Exact(last.into()));
            self.elements
                .ranges
                .set(ranges)
                .map_err(|_| vortex_err!("Selection resolved twice"))?;
            cx.resolve_selection(
                self.elements
                    .gate
                    .ok_or_else(|| vortex_err!("Missing element gate"))?,
            );
            self.assembled = Some((offsets, validity));
        }
        Ok(if cx.inlet(element_inlet).closed() {
            SourceReady::Ready
        } else {
            SourceReady::Blocked(SourceBlocked::Inlet(element_inlet))
        })
    }
}

impl Operator for DeferredList {
    fn compute(&mut self, _input: Input, cx: &mut Cx<'_>) -> VortexResult<Step> {
        if self.done {
            return Ok(Step::End);
        }
        self.done = true;
        let inlet = 1 + usize::from(self.plan.dtype().is_nullable());
        let elements = join_inlet(cx, inlet, self.plan.elements()?.dtype())?;
        let (offsets, validity) = self
            .assembled
            .take()
            .ok_or_else(|| vortex_err!("List selection unresolved"))?;
        Ok(Step::Final(
            ListArray::try_new(elements, offsets, validity)?.into_array(),
        ))
    }
}
