// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Assembling lists from their offsets and the elements those offsets select.

use std::collections::VecDeque;
use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::IntoArray;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::ListArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::DType;
use vortex_array::dtype::Nullability;
use vortex_array::match_each_integer_ptype;
use vortex_array::validity::Validity;
use vortex_buffer::Buffer;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_mask::AllOr;
use vortex_mask::Mask;

use super::join_inlet;
use crate::plan::physical::PlanRef;
use crate::plan::physical::operator::Cx;
use crate::plan::physical::operator::Input;
use crate::plan::physical::operator::Operator;
use crate::plan::physical::operator::Source;
use crate::plan::physical::operator::SourceBlocked;
use crate::plan::physical::operator::SourceReady;
use crate::plan::physical::operator::Step;

const OFFSETS: usize = 0;
const VALIDITY: usize = 1;

/// Elements a batch of lists holds before it is emitted, unless the elements end first.
const ELEMENTS_PER_BATCH: u64 = 1 << 20;

/// The step a [`ListSource`] is at.
enum Phase {
    /// Waiting for the offsets, and the validity of a nullable list, to end.
    Offsets,
    /// Emitting lists as their elements arrive on the inlet compiled for them, or `None` when
    /// the selected lists hold no element.
    Elements(Option<usize>),
}

/// Reads a list's offsets, compiles its elements over the rows those offsets select, and zips
/// the two into lists as the elements arrive.
///
/// The offsets cover one more row than the lists, the validity of a nullable list only the
/// selected lists. Both are read whole before the elements are compiled: the element rows are
/// known only from the offsets, so the elements plan is compiled while the scan runs, through
/// [`Cx::compile`].
pub(crate) struct ListSource {
    elements: PlanRef,
    offsets_dtype: DType,
    /// Which of the lists the offsets cover are produced.
    mask: Mask,
    nullable: bool,
    phase: Phase,
    /// The offsets of the selected lists into the selected elements: list `i` holds elements
    /// `offsets[i]..offsets[i + 1]`.
    offsets: Vec<u64>,
    /// The selected lists' validity, once read.
    validity: Option<ArrayRef>,
    /// Lists emitted.
    emitted: usize,
    /// Elements taken from the inlet and not yet emitted, in order.
    buffer: VecDeque<ArrayRef>,
    /// Elements emitted, and taken but not yet emitted.
    consumed: u64,
    buffered: u64,
}

impl ListSource {
    /// Lists from `elements`, reading offsets on inlet 0 and, when `nullable`, the selected
    /// lists' validity on inlet 1. `mask` selects the lists produced from those the offsets
    /// cover.
    pub(crate) fn new(elements: PlanRef, offsets_dtype: DType, mask: Mask, nullable: bool) -> Self {
        Self {
            elements,
            offsets_dtype,
            mask,
            nullable,
            phase: Phase::Offsets,
            offsets: Vec::new(),
            validity: None,
            emitted: 0,
            buffer: VecDeque::new(),
            consumed: 0,
            buffered: 0,
        }
    }

    /// Reads the offsets and plans the element rows: the span of every selected list, merged
    /// where they meet. Returns the rows of the elements domain and the mask of those to read.
    fn plan_elements(&mut self, offsets: &ArrayRef, cx: &mut Cx<'_>) -> VortexResult<Plan> {
        let offsets = offsets.clone().execute::<PrimitiveArray>(cx.exec())?;
        let raw: Vec<u64> = match_each_integer_ptype!(offsets.ptype(), |T| {
            offsets
                .as_slice::<T>()
                .iter()
                .map(|&offset| {
                    u64::try_from(i128::from(offset))
                        .map_err(|_| vortex_err!("Negative list offset {offset}"))
                })
                .collect::<VortexResult<_>>()?
        });
        vortex_ensure!(
            raw.len() == self.mask.len() + 1,
            "List offsets cover {} lists but {} are selected over",
            raw.len().saturating_sub(1),
            self.mask.len()
        );
        let start = raw[0];
        let end = raw[raw.len() - 1];
        vortex_ensure!(start <= end, "List offsets decrease");
        if self.mask.all_true() {
            self.offsets = raw.iter().map(|&offset| offset - start).collect();
            return Ok(Plan {
                rows: start..end,
                mask: None,
            });
        }
        let AllOr::Some(selected) = self.mask.slices() else {
            vortex_bail!("A list mask that is neither all true nor sparse");
        };
        let mut slices: Vec<(usize, usize)> = Vec::new();
        let mut rebased = Vec::with_capacity(self.mask.true_count() + 1);
        rebased.push(0);
        let mut total = 0;
        for index in selected.iter().flat_map(|&(first, last)| first..last) {
            let (from, to) = (raw[index], raw[index + 1]);
            vortex_ensure!(from <= to, "List offsets decrease");
            total += to - from;
            rebased.push(total);
            if from == to {
                continue;
            }
            let (from, to) = (usize::try_from(from - start)?, usize::try_from(to - start)?);
            match slices.last_mut() {
                Some(last) if last.1 == from => last.1 = to,
                _ => slices.push((from, to)),
            }
        }
        self.offsets = rebased;
        let len = usize::try_from(end - start)?;
        Ok(Plan {
            rows: start..end,
            mask: Some(Mask::from_slices(len, slices)),
        })
    }

    /// The number of lists the elements taken so far complete.
    fn complete(&self) -> usize {
        let available = self.consumed + self.buffered;
        self.offsets
            .partition_point(|&offset| offset <= available)
            .saturating_sub(1)
    }

    fn lists(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Takes the first `len` buffered elements, slicing the last batch they reach.
    fn take_elements(&mut self, len: u64, dtype: &DType) -> VortexResult<ArrayRef> {
        let mut pieces = Vec::new();
        let mut left = usize::try_from(len)?;
        while left > 0 {
            let front = self
                .buffer
                .front_mut()
                .ok_or_else(|| vortex_err!("List elements ended early"))?;
            if front.len() <= left {
                left -= front.len();
                pieces.extend(self.buffer.pop_front());
            } else {
                let rest = front.slice(left..front.len())?;
                pieces.push(std::mem::replace(front, rest).slice(0..left)?);
                left = 0;
            }
        }
        self.buffered -= len;
        self.consumed += len;
        Ok(match pieces.len() {
            0 => Canonical::empty(dtype).into_array(),
            1 => pieces.swap_remove(0),
            // SAFETY: every piece comes from the elements inlet, whose batches share its dtype.
            _ => unsafe { ChunkedArray::new_unchecked(pieces, dtype.clone()) }.into_array(),
        })
    }
}

/// The element rows a list source compiles.
struct Plan {
    rows: Range<u64>,
    mask: Option<Mask>,
}

impl Source for ListSource {
    fn ready(&mut self, cx: &mut Cx<'_>) -> VortexResult<SourceReady> {
        if matches!(self.phase, Phase::Offsets) {
            if !cx.inlet(OFFSETS).closed() {
                return Ok(SourceReady::Blocked(SourceBlocked::Inlet(OFFSETS)));
            }
            if self.nullable && !cx.inlet(VALIDITY).closed() {
                return Ok(SourceReady::Blocked(SourceBlocked::Inlet(VALIDITY)));
            }
            let offsets = join_inlet(cx, OFFSETS, &self.offsets_dtype)?;
            if self.nullable {
                self.validity = Some(join_inlet(
                    cx,
                    VALIDITY,
                    &DType::Bool(Nullability::NonNullable),
                )?);
            }
            let plan = self.plan_elements(&offsets, cx)?;
            if plan.rows.is_empty() {
                self.phase = Phase::Elements(None);
                return Ok(SourceReady::Ready);
            }
            let len = usize::try_from(plan.rows.end - plan.rows.start)?;
            let mask = plan.mask.unwrap_or_else(|| Mask::new_true(len));
            let inlet = cx.compile(self.elements.clone(), plan.rows, mask);
            self.phase = Phase::Elements(Some(inlet));
            return Ok(SourceReady::Blocked(SourceBlocked::Inlet(inlet)));
        }
        let Phase::Elements(Some(index)) = self.phase else {
            return Ok(SourceReady::Ready);
        };
        let mut inlet = cx.inlet(index);
        while let Some(batch) = inlet.take() {
            self.buffered += batch.len() as u64;
            self.buffer.push_back(batch);
        }
        let closed = inlet.closed();
        if closed || self.emitted == self.lists() {
            return Ok(SourceReady::Ready);
        }
        let complete = self.complete();
        let ready = complete > self.emitted
            && self.offsets[complete] - self.offsets[self.emitted] >= ELEMENTS_PER_BATCH;
        Ok(if ready {
            SourceReady::Ready
        } else {
            SourceReady::Blocked(SourceBlocked::Inlet(index))
        })
    }
}

impl Operator for ListSource {
    fn compute(&mut self, _input: Input, cx: &mut Cx<'_>) -> VortexResult<Step> {
        let lists = self.lists();
        if self.emitted == lists {
            return Ok(Step::End);
        }
        let end = match self.phase {
            Phase::Offsets => vortex_bail!("List computed before its offsets were read"),
            Phase::Elements(None) => lists,
            Phase::Elements(Some(index)) if cx.inlet(index).closed() => lists,
            Phase::Elements(Some(_)) => self.complete(),
        };
        let start = self.emitted;
        let base = self.offsets[start];
        let dtype = self.elements.dtype().clone();
        let elements = self.take_elements(self.offsets[end] - base, &dtype)?;
        let offsets: Buffer<u64> = self.offsets[start..=end]
            .iter()
            .map(|&offset| offset - base)
            .collect();
        let validity = match &self.validity {
            Some(validity) => Validity::Array(validity.slice(start..end)?),
            None => Validity::NonNullable,
        };
        self.emitted = end;
        // SAFETY: the offsets are rebased to the elements taken for these lists, so they start
        // at zero, never decrease, and end at the elements' length; the validity covers the same
        // lists.
        let list = unsafe { ListArray::new_unchecked(elements, offsets.into_array(), validity) };
        Ok(Step::Final(list.into_array()))
    }
}
