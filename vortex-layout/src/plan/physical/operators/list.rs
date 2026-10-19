// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Resolves selected list offsets before scanning their element ranges.

use std::collections::VecDeque;
use std::ops::Range;

use rustc_hash::FxHashMap;
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
use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_mask::AllOr;
use vortex_mask::Mask;
use vortex_utils::parallelism::get_available_parallelism;

use self::reads::ElementReads;
use super::join;
use super::join_inlet;
use crate::plan::physical::ListPackPlan;
use crate::plan::physical::operator::Cx;
use crate::plan::physical::operator::Input;
use crate::plan::physical::operator::Operator;
use crate::plan::physical::operator::Source;
use crate::plan::physical::operator::SourceBlocked;
use crate::plan::physical::operator::SourceReady;
use crate::plan::physical::operator::Step;
use crate::plan::physical::pipeline::ChildProjection;
use crate::plan::physical::pipeline::ReadId;
use crate::plan::physical::pipeline::Scan;
use crate::plan::physical::pipeline::Split;
use crate::plan::physical::pipeline::Turn;
use crate::plan::physical::pipeline::ValueCache;
use crate::segments::SegmentId;

mod reads;
mod work;

/// A list's offsets and validity arrive before its selected element ranges can be planned.
pub(crate) struct ListSource {
    plan: ListPackPlan,
    mask: Mask,
    values: ValueCache,
    plan_children: bool,
    awaiting_child: bool,
    serial_splits: Option<(Vec<Split>, bool)>,
    initialized: bool,
    offsets: Option<ArrayRef>,
    validity: Option<Validity>,
    elements: Option<Scan>,
    /// Element reads issued by this source when the elements are plain segment chunks.
    direct: Option<ElementReads>,
    reads: FxHashMap<SegmentId, VecDeque<ReadId>>,
    pending_children: usize,
    chunks: Vec<(usize, ArrayRef)>,
    output: Option<ArrayRef>,
    done: bool,
}

impl ListSource {
    pub(crate) fn new(
        plan: ListPackPlan,
        mask: Mask,
        values: ValueCache,
        plan_children: bool,
    ) -> Self {
        Self {
            plan,
            mask,
            values,
            plan_children,
            awaiting_child: false,
            serial_splits: None,
            initialized: false,
            offsets: None,
            validity: None,
            elements: None,
            direct: None,
            reads: FxHashMap::default(),
            pending_children: 0,
            chunks: Vec::new(),
            output: None,
            done: false,
        }
    }

    fn initialize(&mut self, cx: &mut Cx<'_>) -> VortexResult<()> {
        let offsets = join_inlet(cx, 0, self.plan.offsets()?.dtype())?
            .cast(DType::Primitive(PType::U64, false.into()))?
            .execute::<PrimitiveArray>(cx.exec())?;
        let validity = if self.plan.dtype().is_nullable() {
            Validity::Array(join_inlet(cx, 1, &DType::Bool(false.into()))?)
        } else {
            Validity::NonNullable
        };
        let valid = validity.execute_mask(self.mask.true_count(), cx.exec())?;
        let elements = self.plan.elements()?;
        let offsets = offsets.as_slice::<u64>();
        let (mut selected_offsets, mut ranges) = match self.mask.indices() {
            AllOr::All => {
                selected_ranges(0..self.mask.len(), offsets, &valid, elements.row_count())?
            }
            AllOr::Some(indices) => selected_ranges(
                indices.iter().copied(),
                offsets,
                &valid,
                elements.row_count(),
            )?,
            AllOr::None => (Buffer::from(vec![0_u64]), Vec::new()),
        };
        // An all-selected scope can retain payload under null rows. For dense, bounded spans,
        // this avoids filtering every element just to remove values hidden by list validity.
        if self.mask.all_true()
            && !valid.all_true()
            && let Some(offsets) = work::preserve_null_span(
                elements.dtype(),
                offsets,
                selected_offsets.last().copied().unwrap_or(0),
                &mut ranges,
            )
        {
            selected_offsets = offsets;
        }
        let offsets = selected_offsets;
        let last_offset = offsets.last().copied().unwrap_or(0);
        let offsets = offsets.into_array();
        // These offsets start at zero and are checked sums of nonnegative lengths. Retain
        // those facts so ListArray validation need not compute aggregates over them again.
        let stats = offsets.statistics();
        stats.set(Stat::IsSorted, Precision::Exact(true.into()));
        stats.set(Stat::Min, Precision::Exact(0_u64.into()));
        stats.set(Stat::Max, Precision::Exact(last_offset.into()));
        self.offsets = Some(offsets);
        self.validity = Some(validity);
        let overlap = work::can_overlap(
            &elements,
            &ranges,
            self.mask.true_count(),
            get_available_parallelism().unwrap_or(1),
        )?;
        let splits = work::element_splits(elements.dtype(), ranges)?;
        // Avoid handing tiny or variable-width work to the outer planner. The planner
        // decides whether the substantial fixed-width ranges can be split safely.
        let width = match elements.dtype() {
            DType::Primitive(ptype, _) => ptype.byte_width() as u64,
            DType::Bool(_) => 1,
            _ => 0,
        };
        let bytes = splits.iter().fold(0_u64, |bytes, split| {
            bytes.saturating_add((split.mask.true_count() as u64).saturating_mul(width))
        });
        if self.plan_children && bytes >= 512 * 1024 {
            self.serial_splits = Some((splits.clone(), overlap));
            self.awaiting_child = true;
            cx.request_child(ChildProjection {
                id: 0,
                plan: elements,
                splits,
                values: self.values.clone(),
            });
            self.initialized = true;
            return Ok(());
        }
        self.start_elements(cx, splits, overlap)?;
        self.initialized = true;
        Ok(())
    }

    fn start_elements(
        &mut self,
        cx: &mut Cx<'_>,
        splits: Vec<Split>,
        overlap: bool,
    ) -> VortexResult<()> {
        let elements = self.plan.elements()?;
        if let Some(mut reads) = ElementReads::try_new(&elements, &splits)? {
            reads.request(cx);
            self.direct = Some(reads);
            return Ok(());
        }
        let scan = Scan::try_new(cx.session().clone(), elements, splits)?
            .with_value_cache(self.values.clone())
            .with_child_planning(self.plan_children);
        self.elements = Some(if overlap {
            scan.with_max_active(2)
        } else {
            scan
        });
        Ok(())
    }

    fn finish(&mut self) -> VortexResult<()> {
        // Independent element ranges may finish out of order. Preserve batch order within
        // each range while putting the ranges back in list order.
        self.chunks.sort_by_key(|(split, _)| *split);
        let elements = join(
            self.plan.elements()?.dtype(),
            std::mem::take(&mut self.chunks)
                .into_iter()
                .map(|(_, array)| array)
                .collect(),
        )?;
        let offsets = self
            .offsets
            .take()
            .ok_or_else(|| vortex_err!("Missing list offsets"))?;
        let validity = self
            .validity
            .take()
            .ok_or_else(|| vortex_err!("Missing list validity"))?;
        self.output = Some(ListArray::try_new(elements, offsets, validity)?.into_array());
        self.elements = None;
        Ok(())
    }
}

impl Source for ListSource {
    fn ready(&mut self, cx: &mut Cx<'_>) -> VortexResult<SourceReady> {
        if self.done || self.output.is_some() {
            return Ok(SourceReady::Ready);
        }
        if !self.initialized {
            for index in 0..1 + usize::from(self.plan.dtype().is_nullable()) {
                if !cx.inlet(index).closed() {
                    return Ok(SourceReady::Blocked(SourceBlocked::Inlet(index)));
                }
            }
            self.initialize(cx)?;
        }
        if self.awaiting_child {
            let Some((_, arrays)) = cx.take_child() else {
                return Ok(SourceReady::Blocked(SourceBlocked::Child));
            };
            self.awaiting_child = false;
            let (splits, overlap) = self
                .serial_splits
                .take()
                .ok_or_else(|| vortex_err!("Missing deferred list ranges"))?;
            if let Some(arrays) = arrays {
                self.chunks.extend(arrays.into_iter().enumerate());
                self.finish()?;
                return Ok(SourceReady::Ready);
            }
            self.start_elements(cx, splits, overlap)?;
        }
        if let Some(reads) = self.direct.as_mut() {
            let session = cx.session().clone();
            while let Some((segment, bytes)) = cx.take_segment() {
                reads.deliver(segment, bytes, &session)?;
            }
            if !reads.is_done() {
                return Ok(SourceReady::Blocked(SourceBlocked::Io));
            }
            let reads = self
                .direct
                .take()
                .ok_or_else(|| vortex_err!("Missing list element reads"))?;
            self.chunks.extend(reads.finish()?.into_iter().enumerate());
            self.finish()?;
            return Ok(SourceReady::Ready);
        }
        let scan = self
            .elements
            .as_mut()
            .ok_or_else(|| vortex_err!("Missing list element scan"))?;
        while let Some((segment, bytes)) = cx.take_segment() {
            let request = self
                .reads
                .get_mut(&segment)
                .and_then(VecDeque::pop_front)
                .ok_or_else(|| vortex_err!("Unexpected list element segment {segment}"))?;
            scan.deliver(request, bytes)?;
        }
        while let Some((id, arrays)) = cx.take_child() {
            scan.complete_child(id, arrays)?;
            self.pending_children -= 1;
        }
        loop {
            match scan.step()? {
                Turn::Read(read) => {
                    self.reads
                        .entry(read.segment_id)
                        .or_default()
                        .push_back(read.id);
                    cx.request_read(read.segment_id);
                }
                Turn::Output(split, array) => self.chunks.push((split, array)),
                Turn::Child(child) => {
                    self.pending_children += 1;
                    cx.request_child(child);
                }
                Turn::Waiting => {
                    return Ok(SourceReady::Blocked(if self.pending_children > 0 {
                        SourceBlocked::Child
                    } else {
                        SourceBlocked::Io
                    }));
                }
                Turn::Done => {
                    self.finish()?;
                    return Ok(SourceReady::Ready);
                }
            }
        }
    }
}

impl Operator for ListSource {
    fn compute(&mut self, _input: Input, _cx: &mut Cx<'_>) -> VortexResult<Step> {
        if self.done {
            return Ok(Step::End);
        }
        self.done = true;
        self.output
            .take()
            .map(Step::Final)
            .ok_or_else(|| vortex_err!("List computed before its elements were ready"))
    }
}

/// Compacts selected offsets and merges adjacent element ranges. No mask spans the gaps between
/// distant lists, so planning memory scales with selected rows rather than their element span.
fn selected_ranges(
    selected: impl Iterator<Item = usize>,
    offsets: &[u64],
    valid: &Mask,
    element_count: u64,
) -> VortexResult<(Buffer<u64>, Vec<Range<u64>>)> {
    let mut output_offsets = Vec::with_capacity(valid.len() + 1);
    output_offsets.push(0_u64);
    let mut ranges: Vec<Range<u64>> = Vec::new();
    let mut previous = None;
    let mut cursor = 0;
    let mut total = 0_u64;
    let mut last_end = 0;
    for (index, is_valid) in selected.zip(valid.iter()) {
        // Consecutive selected lists share one boundary in the offsets selection.
        if previous == Some(index) {
            cursor -= 1;
        }
        let pair = offsets
            .get(cursor..cursor + 2)
            .ok_or_else(|| vortex_err!("Missing selected list boundaries"))?;
        let start = pair[0];
        let end = pair[1];
        vortex_ensure!(
            last_end <= start && start <= end && end <= element_count,
            "Invalid list element range {start}..{end}"
        );
        last_end = end;
        if is_valid {
            total = total
                .checked_add(end - start)
                .ok_or_else(|| vortex_err!("Selected list elements overflow"))?;
            if start < end {
                match ranges.last_mut() {
                    Some(last) if last.end == start => last.end = end,
                    _ => ranges.push(start..end),
                }
            }
        }
        output_offsets.push(total);
        previous = Some(index + 1);
        cursor += 2;
    }
    vortex_ensure!(
        cursor == offsets.len(),
        "Unexpected selected list boundaries"
    );
    Ok((Buffer::from(output_offsets), ranges))
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::adjacent(vec![0, 1], vec![0, 2, 5], vec![true, true], vec![0, 2, 5], vec![0..5])]
    #[case::gaps(vec![0, 3], vec![2, 4, 100, 103], vec![true, true], vec![0, 2, 5], vec![2..4, 100..103])]
    #[case::null(vec![0, 1, 4], vec![0, 3, 8, 100, 105], vec![true, false, true], vec![0, 3, 3, 8], vec![0..3, 100..105])]
    #[case::empty_lists(vec![0, 1], vec![2, 2, 2], vec![true, true], vec![0, 0, 0], vec![])]
    fn compacts_selected_elements(
        #[case] selected: Vec<usize>,
        #[case] boundaries: Vec<u64>,
        #[case] valid: Vec<bool>,
        #[case] offsets: Vec<u64>,
        #[case] ranges: Vec<Range<u64>>,
    ) -> VortexResult<()> {
        let (actual_offsets, actual_ranges) = selected_ranges(
            selected.into_iter(),
            &boundaries,
            &Mask::from_iter(valid),
            1000,
        )?;
        assert_eq!(actual_offsets.as_slice(), offsets);
        assert_eq!(actual_ranges, ranges);
        Ok(())
    }

    #[rstest]
    #[case::missing(vec![0, 1])]
    #[case::negative_length(vec![0, 10, 9])]
    #[case::out_of_bounds(vec![0, 9, 11])]
    fn rejects_invalid_boundaries(#[case] boundaries: Vec<u64>) {
        assert!(selected_ranges(0..2, &boundaries, &Mask::new_true(2), 10).is_err());
    }
}
