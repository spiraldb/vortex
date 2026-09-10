// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::arrays::ConstantArray;
use vortex_array::dtype::DType;
use vortex_array::scalar::Scalar;
use vortex_array::serde::SerializedArray;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_layout::plan::SegmentScanPlan;
use vortex_mask::Mask;

use crate::demand::RowDomain;
use crate::io::IoKey;
use crate::io::IoTicket;
use crate::io::IoUse;
use crate::io::ProducerId;
use crate::node::Batch;
use crate::node::Cx;
use crate::node::LookAhead;
use crate::node::Operator;
use crate::node::Step;
use crate::stats::NoReadReason;

/// The only operator that touches the world: one stored segment, decoded and sliced.
///
/// `look_ahead` names the segment exactly once per morsel. If the shared cell for the segment already
/// holds a decoded value, it names nothing — the morsel's own lease keeps that value alive until
/// it closes. Otherwise `next` clones the ready bytes, decodes, publishes the whole segment into
/// the cell, and slices to the morsel's local range. It never filters: the plan places a filter
/// above it when a selection applies. The row hint is the only thing the leaf takes from its
/// parent, and it uses it for exactly two decisions: an all-false demand during look-ahead names no read, and
/// an all-false hint at next answers with placeholder rows without touching the read.
pub struct FlatExec {
    plan: SegmentScanPlan,
    /// Root-coordinate offset of this segment's row zero.
    root_offset: u64,
    /// Root rows whose initial demand leases this segment, including dictionary value scopes.
    lease_scope: Range<u64>,
    producer: ProducerId,
    /// The value placeholder rows carry, built on first use; never observed, but it must have
    /// the right dtype.
    placeholder: Option<Scalar>,

    domain: RowDomain,
    state: State,
}

enum State {
    Unplanned,
    /// The operator has been closed, releasing its lease.
    Closed,
    /// A lease is held. `ticket` is `None` when no read was needed: nothing wanted, or the
    /// segment was already decoded by another morsel.
    Named {
        ticket: Option<IoTicket>,
    },
    /// Value produced. The lease is held until `close`.
    Emitted,
}

impl FlatExec {
    /// Build a flat operator over one segment scan.
    pub fn new(
        plan: &SegmentScanPlan,
        root_offset: u64,
        producer: ProducerId,
        lease_scope: Range<u64>,
        domain: RowDomain,
    ) -> Self {
        Self {
            plan: plan.clone(),
            root_offset,
            lease_scope,
            producer,
            placeholder: None,
            domain,
            state: State::Unplanned,
        }
    }

    /// Decode the whole segment from ready bytes. `None` means the caller must park on the ticket.
    fn decode(&self, ticket: IoTicket, cx: &mut Cx<'_>) -> VortexResult<Option<ArrayRef>> {
        let Some(bytes) = cx.ready(ticket)? else {
            return Ok(None);
        };

        let parts = match self.plan.array_tree() {
            Some(tree) => SerializedArray::from_flatbuffer_and_segment(tree.clone(), bytes)?,
            None => SerializedArray::try_from(bytes)?,
        };
        let rows = usize::try_from(self.plan.row_count())
            .map_err(|_| vortex_err!("segment row count exceeds usize"))?;
        let session = cx.session().clone();
        let array = parts.decode(self.plan.dtype(), rows, self.plan.array_ctx(), &session)?;
        let segment = self.plan.segment_id();
        cx.stats().record_decode(*segment, array.len());
        tracing::trace!(
            target: "vortex_morsel::flat_decode",
            segment = *segment,
            values = array.len(),
            "decoded flat values"
        );
        cx.publish_decoded(IoKey::Segment(segment), &array);
        Ok(Some(array))
    }
}

impl Operator for FlatExec {
    fn row_domain(&self) -> &RowDomain {
        &self.domain
    }

    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        if !matches!(self.state, State::Unplanned) {
            return Ok(LookAhead::Complete);
        }
        if self.domain.range().is_empty() {
            self.state = State::Closed;
            return Ok(LookAhead::Complete);
        }

        let hint = self.domain.snapshot();
        let key = IoKey::Segment(self.plan.segment_id());
        let ticket = if hint.all_false() {
            // Nothing in this range is wanted: name no read. Execution stands in placeholder rows.
            cx.note_no_read(key, NoReadReason::NothingWanted);
            None
        } else if cx.decoded_available(key) {
            // A decoded value already published by another morsel makes the read unnecessary.
            // The lease this morsel holds (counted before the scan started) pins the value until
            // close, so skipping the read here can never leave `next` empty-handed.
            cx.note_no_read(key, NoReadReason::AlreadyDecoded);
            None
        } else {
            // The extent is the whole stored unit: two morsels straddling this segment name the
            // same cell and share one read.
            Some(cx.register(IoUse {
                key,
                extent: 0..self.plan.row_count(),
                producer: self.producer,
                demand: self.domain.demand(self.domain.range().clone())?,
            })?)
        };
        self.state = State::Named { ticket };
        Ok(LookAhead::Complete)
    }

    fn next(&mut self, hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step> {
        let ticket = match self.state {
            State::Unplanned => return Err(vortex_err!("flat leaf pulled before look-ahead")),
            State::Named { ticket } => ticket,
            State::Emitted | State::Closed => return Ok(Step::Finished),
        };
        let coverage = self.root_offset + self.domain.range().start
            ..self.root_offset + self.domain.range().end;
        let rows = usize::try_from(self.domain.range().end - self.domain.range().start)
            .vortex_expect("flat range fits usize");

        // Nobody will look at these rows: stand in for them without touching the read, even if
        // look-ahead did register one speculatively. The filter above drops them with its mask.
        if hint.all_false() {
            let scalar = self
                .placeholder
                .get_or_insert_with(|| placeholder_scalar(self.plan.dtype()))
                .clone();
            cx.stats().record_placeholder(rows);
            self.state = State::Emitted;
            return Ok(Step::Batch(Batch::array(
                coverage,
                ConstantArray::new(scalar, rows).into_array(),
            )));
        }

        let mut array =
            if let Some(shared) = cx.shared_decoded(IoKey::Segment(self.plan.segment_id())) {
                shared
            } else {
                let ticket = ticket.ok_or_else(|| crate::io::unplanned_ticket(self.producer))?;
                let Some(array) = self.decode(ticket, cx)? else {
                    cx.wait(ticket);
                    return Ok(Step::Blocked);
                };
                array
            };
        let start =
            usize::try_from(self.domain.range().start).vortex_expect("flat range start fits usize");
        let end =
            usize::try_from(self.domain.range().end).vortex_expect("flat range end fits usize");
        if start > 0 || end < array.len() {
            array = array.slice(start..end)?;
        }
        cx.stats().rows_materialized += array.len() as u64;
        self.state = State::Emitted;
        Ok(Step::Batch(Batch::array(coverage, array)))
    }

    fn close(&mut self, cx: &mut Cx<'_>) {
        if !matches!(self.state, State::Closed) {
            let start = self.lease_scope.start.max(cx.morsel.range.start);
            let end = self.lease_scope.end.min(cx.morsel.range.end);
            if start < end {
                let local_start = usize::try_from(start - cx.morsel.range.start)
                    .vortex_expect("lease offset fits usize");
                let local_end = usize::try_from(end - cx.morsel.range.start)
                    .vortex_expect("lease offset fits usize");
                if !cx.morsel.demand.slice(local_start..local_end).all_false() {
                    cx.release_use(IoKey::Segment(self.plan.segment_id()));
                }
            }
        }
        self.state = State::Closed;
    }

    fn describe(&self) -> String {
        format!(
            "Flat(segment {}, {} rows, {})",
            *self.plan.segment_id(),
            self.plan.row_count(),
            self.plan.dtype()
        )
    }
}

/// A value of `dtype` to stand in for rows nobody will look at: null where the type allows it,
/// otherwise the type's zero value (an extension type's is its storage type's).
pub(crate) fn placeholder_scalar(dtype: &DType) -> Scalar {
    if dtype.is_nullable() {
        Scalar::null(dtype.clone())
    } else {
        Scalar::zero_value(dtype)
    }
}
