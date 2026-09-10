// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::IntoArray;
use vortex_array::dtype::DType;
use vortex_array::serde::SerializedArray;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_layout::layouts::flat::FlatLayout;

use crate::io::IoBatch;
use crate::io::IoKey;
use crate::io::IoTicket;
use crate::io::IoUse;
use crate::io::ProducerId;
use crate::node::ActivationRows;
use crate::node::ExecNode;
use crate::node::NodeId;
use crate::node::NodeState;
use crate::node::PlanCx;
use crate::node::PlanItem;
use crate::node::PlanPoll;
use crate::node::PushBatch;
use crate::node::PushCx;
use crate::node::RetireCx;
use crate::node::StageOutput;
use crate::node::Value;
use crate::node::Wait;
use crate::node::WaitSet;
use crate::stats::push_profile_enabled;

/// An immutable stored segment and its root-coordinate row range.
pub(crate) struct FlatSegment {
    pub(crate) layout: FlatLayout,
    pub(crate) range: Range<u64>,
}

impl FlatSegment {
    pub(crate) fn new(layout: FlatLayout, root_offset: u64) -> Self {
        let end = root_offset + layout.row_count();
        Self {
            layout,
            range: root_offset..end,
        }
    }
}

/// A source that pushes an ordered sequence of stored flat segments.
///
/// Read tickets and decode leases remain per segment. Each activation supplies the selection
/// for one or more complete segment overlaps, so later segments need not be activated before
/// an earlier segment can produce a batch. Consumer credits advance the emission cursor.
pub struct FlatExec {
    segments: Arc<[FlatSegment]>,
    producer: ProducerId,
    range: Range<u64>,
    active: Range<usize>,
    tickets: Vec<Option<IoTicket>>,
    selections: Vec<Option<ActivationRows>>,
    planned: usize,
    next: usize,
    output_credit: bool,
}

impl FlatExec {
    /// Build a source over one flat layout.
    pub fn new(layout: &FlatLayout, root_offset: u64, producer: ProducerId) -> Self {
        Self::from_segments(
            Arc::from([FlatSegment::new(layout.clone(), root_offset)]),
            producer,
        )
    }

    pub(crate) fn from_segments(segments: Arc<[FlatSegment]>, producer: ProducerId) -> Self {
        Self {
            segments,
            producer,
            range: 0..0,
            active: 0..0,
            tickets: Vec::new(),
            selections: Vec::new(),
            planned: 0,
            next: 0,
            output_credit: true,
        }
    }

    fn segment_span(&self, index: usize) -> Range<u64> {
        let segment = &self.segments[self.active.start + index];
        segment.range.start.max(self.range.start)..segment.range.end.min(self.range.end)
    }

    fn decode(&self, cx: &mut PushCx<'_>) -> VortexResult<Option<ArrayRef>> {
        let segment = &self.segments[self.active.start + self.next];
        let layout = &segment.layout;
        let key = IoKey::Segment(layout.segment_id());
        if let Some(shared) = cx.shared_decoded(key) {
            return Ok(Some(shared));
        }
        let ticket =
            self.tickets[self.next].ok_or_else(|| crate::io::unplanned_ticket(self.producer))?;
        let Some(bytes) = cx.ready(ticket)? else {
            return Ok(None);
        };
        let profile_started = push_profile_enabled().then(Instant::now);
        let parts = match layout.array_tree() {
            Some(tree) => SerializedArray::from_flatbuffer_and_segment(tree.clone(), bytes)?,
            None => SerializedArray::try_from(bytes)?,
        };
        let rows = usize::try_from(layout.row_count())
            .map_err(|_| vortex_err!("segment row count exceeds usize"))?;
        let session = cx.session().clone();
        let array = parts.decode(layout.dtype(), rows, layout.array_ctx(), &session)?;
        if let Some(started) = profile_started {
            cx.stats().push_profile_flat_decode.0 += 1;
            cx.stats().push_profile_flat_decode.1 += started.elapsed();
        }
        cx.stats().decodes += 1;
        cx.publish_decoded(key, &array);
        Ok(Some(array))
    }

    fn emit_push(&mut self, cx: &mut PushCx<'_>, out: &mut StageOutput) -> VortexResult<NodeState> {
        if self.next == self.active.len() {
            return Ok(NodeState::Done);
        }
        if !self.output_credit {
            return Ok(NodeState::NeedInput);
        }
        let Some(rows) = &self.selections[self.next] else {
            return Ok(NodeState::NeedInput);
        };
        let segment = &self.segments[self.active.start + self.next];
        let span = self.segment_span(self.next);
        let array = if rows.materialized().all_false() {
            Canonical::empty(segment.layout.dtype()).into_array()
        } else {
            let Some(mut array) = self.decode(cx)? else {
                let ticket = self.tickets[self.next]
                    .ok_or_else(|| crate::io::unplanned_ticket(self.producer))?;
                return Ok(NodeState::Waiting(
                    [Wait::Io(ticket)].into_iter().collect::<WaitSet>(),
                ));
            };
            let start = usize::try_from(span.start - segment.range.start)
                .vortex_expect("flat range start fits usize");
            let end = usize::try_from(span.end - segment.range.start)
                .vortex_expect("flat range end fits usize");
            if start > 0 || end < array.len() {
                array = array.slice(start..end)?;
            }
            if !rows.materialized().all_true() {
                let profile_started = push_profile_enabled().then(Instant::now);
                array = array.filter(rows.materialized().clone())?;
                if let Some(started) = profile_started {
                    cx.stats().push_profile_flat_filter.0 += 1;
                    cx.stats().push_profile_flat_filter.1 += started.elapsed();
                }
            }
            array
        };
        let rows = self.selections[self.next]
            .take()
            .vortex_expect("flat activation selection was checked above");
        let batch = PushBatch::try_new_materialized(span, rows, Value::Array(array))?;
        self.next += 1;
        self.output_credit = false;
        let last = self.next == self.active.len();
        out.set_batch(batch, last);
        Ok(if last {
            NodeState::Done
        } else {
            NodeState::NeedInput
        })
    }
}

impl ExecNode for FlatExec {
    fn push_profile_kind(&self) -> crate::node::PushProfileKind {
        crate::node::PushProfileKind::Flat
    }

    fn reset(&mut self, range: Range<u64>) {
        let root_offset = self
            .segments
            .first()
            .map_or(0, |segment| segment.range.start);
        self.range = root_offset + range.start..root_offset + range.end;
        let first = self
            .segments
            .partition_point(|segment| segment.range.end <= self.range.start);
        let end = self
            .segments
            .partition_point(|segment| segment.range.start < self.range.end);
        self.active = first..end.max(first);
        self.tickets.clear();
        self.tickets.resize(self.active.len(), None);
        self.selections.clear();
        self.selections.resize(self.active.len(), None);
        self.planned = 0;
        self.next = 0;
        self.output_credit = true;
    }

    fn next_plan(&mut self, cx: &mut PlanCx<'_>) -> VortexResult<PlanPoll> {
        if self.planned == self.active.len() {
            return Ok(PlanPoll::Complete);
        }
        if cx.out_of_budget() {
            return Ok(PlanPoll::Item(PlanItem::Plan));
        }
        let mut batch = IoBatch::new();
        let mut ticket_indices = Vec::new();
        while self.planned < self.active.len() && batch.uses().len() < cx.budget() as usize {
            let segment = &self.segments[self.active.start + self.planned];
            let layout = &segment.layout;
            let key = IoKey::Segment(layout.segment_id());
            if !cx.decoded_available(key) {
                batch.push(IoUse {
                    key,
                    extent: 0..layout.row_count(),
                    source_range: segment.range.clone(),
                    producer: self.producer,
                    estimated_bytes: estimate_bytes(layout.dtype(), layout.row_count()),
                });
                ticket_indices.push(self.planned);
            }
            self.planned += 1;
        }
        if batch.uses().is_empty() {
            return Ok(PlanPoll::Complete);
        }
        let tickets = cx.register(batch.clone())?;
        for (index, ticket) in ticket_indices.into_iter().zip(tickets) {
            self.tickets[index] = Some(ticket);
        }
        Ok(PlanPoll::Item(PlanItem::Io(batch)))
    }

    fn push_start(
        &mut self,
        span: Range<u64>,
        rows: ActivationRows,
        cx: &mut PushCx<'_>,
        out: &mut StageOutput,
    ) -> VortexResult<NodeState> {
        if span.start < self.range.start
            || span.end > self.range.end
            || span.is_empty()
            || u64::try_from(rows.logical().len()).ok() != Some(span.end - span.start)
        {
            return Err(vortex_err!(
                "invalid flat activation {span:?} for {:?}",
                self.range
            ));
        }
        let first = self
            .segments
            .partition_point(|segment| segment.range.end <= span.start);
        let end = self
            .segments
            .partition_point(|segment| segment.range.start < span.end);
        for index in first..end {
            let local = index - self.active.start;
            let expected = self.segment_span(local);
            if expected.start < span.start
                || expected.end > span.end
                || local < self.next
                || self.selections[local].is_some()
            {
                return Err(vortex_err!(
                    "flat activation {span:?} splits or repeats segment {expected:?}"
                ));
            }
            let start = usize::try_from(expected.start - span.start)
                .map_err(|_| vortex_err!("activation offset exceeds usize"))?;
            let end = usize::try_from(expected.end - span.start)
                .map_err(|_| vortex_err!("activation offset exceeds usize"))?;
            self.selections[local] = Some(if expected == span {
                rows.clone()
            } else {
                rows.slice(start..end)
            });
        }
        self.emit_push(cx, out)
    }

    fn push_resume(
        &mut self,
        cx: &mut PushCx<'_>,
        out: &mut StageOutput,
    ) -> VortexResult<NodeState> {
        self.emit_push(cx, out)
    }

    fn push_credit(
        &mut self,
        cx: &mut PushCx<'_>,
        out: &mut StageOutput,
    ) -> VortexResult<NodeState> {
        self.output_credit = true;
        self.emit_push(cx, out)
    }

    fn retire(&mut self, cx: &mut RetireCx<'_>) {
        for segment in &self.segments[self.active.start..self.active.start + self.planned] {
            cx.release_use(IoKey::Segment(segment.layout.segment_id()));
        }
        self.tickets.clear();
        self.selections.clear();
        self.planned = 0;
    }

    fn children(&self) -> &[NodeId] {
        &[]
    }
}

/// A rough per-row byte estimate, used only for admission accounting.
///
/// The layout does not carry segment byte sizes, so this is a width estimate rather than a
/// measurement; P2's cost model replaces it with the footer's real segment extents.
fn estimate_bytes(dtype: &DType, rows: u64) -> usize {
    let per_row = match dtype {
        DType::Bool(_) => 1,
        DType::Primitive(ptype, _) => ptype.byte_width(),
        DType::Decimal(..) => 16,
        DType::Utf8(_) | DType::Binary(_) => 16,
        _ => 8,
    };
    usize::try_from(rows)
        .unwrap_or(usize::MAX)
        .saturating_mul(per_row)
}
