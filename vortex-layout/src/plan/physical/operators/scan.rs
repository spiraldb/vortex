// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Reading and decoding one segment.

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::Chunked;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::List;
use vortex_array::arrays::ListView;
use vortex_array::arrays::Struct;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::list::ListArrayExt;
use vortex_array::arrays::list::ListArraySlotsExt;
use vortex_array::arrays::listview::ListViewRebuildMode;
use vortex_array::buffer::BufferHandle;
use vortex_array::serde::SerializedArray;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;
use vortex_session::VortexSession;

use crate::plan::physical::SegmentScanPlan;
use crate::plan::physical::operator::Cx;
use crate::plan::physical::operator::Input;
use crate::plan::physical::operator::Operator;
use crate::plan::physical::operator::Source;
use crate::plan::physical::operator::SourceBlocked;
use crate::plan::physical::operator::SourceReady;
use crate::plan::physical::operator::Step;
use crate::segments::SegmentId;

/// Decodes the whole of a scan's segment.
fn decode(
    plan: &SegmentScanPlan,
    segment: BufferHandle,
    session: &VortexSession,
) -> VortexResult<ArrayRef> {
    let serialized = match plan.array_tree() {
        Some(tree) => SerializedArray::from_flatbuffer_and_segment(tree.clone(), segment)?,
        None => SerializedArray::try_from(segment)?,
    };
    serialized.decode(
        plan.dtype(),
        usize::try_from(plan.row_count())?,
        plan.array_ctx(),
        session,
    )
}

/// Slices a whole decoded segment to `slice`, then keeps the rows `filter` selects.
fn select(
    array: ArrayRef,
    slice: Option<&Range<usize>>,
    filter: Option<&Mask>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrayRef> {
    let array = match slice {
        Some(slice) => trim_elements(array.slice(slice.clone())?, ctx)?,
        None => array,
    };
    match filter {
        Some(filter) => array.filter(filter.clone()),
        None => Ok(array),
    }
}

/// Slices `array` to `range`, keeping of every list in it, at any depth of structs and chunks,
/// only the elements the sliced lists hold. See [`trim_elements`].
pub(crate) fn slice_trimmed(
    array: &ArrayRef,
    range: Range<usize>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrayRef> {
    if range.start == 0 && range.end == array.len() {
        return Ok(array.clone());
    }
    if let Some(structs) = array.as_opt::<Struct>() {
        let fields = structs
            .iter_unmasked_fields()
            .map(|field| slice_trimmed(field, range.clone(), ctx))
            .collect::<VortexResult<Vec<_>>>()?;
        let validity = structs.struct_validity().slice(range.clone())?;
        let dtype = array
            .dtype()
            .as_struct_fields_opt()
            .ok_or_else(|| vortex_err!("A struct array without struct fields"))?
            .clone();
        // SAFETY: every field is sliced to the same rows as the validity, keeping its dtype.
        return Ok(
            unsafe { StructArray::new_unchecked(fields, dtype, range.len(), validity) }
                .into_array(),
        );
    }
    let sliced = array.slice(range)?;
    if let Some(chunked) = sliced.as_opt::<Chunked>() {
        let chunks = chunked
            .chunks()
            .into_iter()
            .map(|chunk| trim_elements(chunk, ctx))
            .collect::<VortexResult<Vec<_>>>()?;
        // SAFETY: trimming keeps each chunk's rows and dtype.
        return Ok(unsafe { ChunkedArray::new_unchecked(chunks, sliced.dtype().clone()) }
            .into_array());
    }
    trim_elements(sliced, ctx)
}

/// A slice of a list keeps every element of the lists around it, and exporting the slice
/// would decode and copy them all. Keeps only the elements the sliced lists hold.
fn trim_elements(array: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
    if array.is_empty() {
        return Ok(array);
    }
    if let Some(list) = array.as_opt::<List>() {
        let list = list.into_owned();
        let start = list.offset_at(0, ctx)?;
        let end = list.offset_at(list.len(), ctx)?;
        if start == 0 && end == list.elements().len() {
            return Ok(array);
        }
        return Ok(list.reset_offsets(false, ctx)?.into_array());
    }
    if let Some(list) = array.as_opt::<ListView>() {
        let list = list.into_owned();
        return Ok(list
            .rebuild(ListViewRebuildMode::TrimElements, ctx)?
            .into_array());
    }
    Ok(array)
}

/// Reads one segment, decodes it, and emits its rows of `slice`, filtered by `filter`.
pub(crate) struct ScanSource {
    plan: SegmentScanPlan,
    slice: Option<Range<usize>>,
    filter: Option<Mask>,
    /// The segment to read, until it has been asked for.
    unread: Option<SegmentId>,
    /// The segment has been emitted.
    done: bool,
}

impl ScanSource {
    pub(crate) fn new(
        plan: SegmentScanPlan,
        slice: Option<Range<usize>>,
        filter: Option<Mask>,
    ) -> Self {
        Self {
            unread: Some(plan.segment_id()),
            plan,
            slice,
            filter,
            done: false,
        }
    }
}

impl Source for ScanSource {
    fn ready(&mut self, cx: &mut Cx<'_>) -> VortexResult<SourceReady> {
        if let Some(segment) = self.unread.take() {
            cx.request_read(segment);
            return Ok(SourceReady::Blocked(SourceBlocked::Io));
        }
        Ok(if self.done || cx.has_bytes() {
            SourceReady::Ready
        } else {
            SourceReady::Blocked(SourceBlocked::Io)
        })
    }
}

impl Operator for ScanSource {
    fn compute(&mut self, _input: Input, cx: &mut Cx<'_>) -> VortexResult<Step> {
        if self.done {
            return Ok(Step::End);
        }
        let (_, bytes) = cx
            .take_segment()
            .ok_or_else(|| vortex_err!("Segment scan ran without its bytes"))?;
        self.done = true;
        let array = decode(&self.plan, bytes, cx.session())?;
        Ok(Step::Final(select(
            array,
            self.slice.as_ref(),
            self.filter.as_ref(),
            cx.exec(),
        )?))
    }
}

/// Narrows a whole decoded segment, the one batch of a share's port, to a reader's rows.
pub(crate) struct SelectStage {
    slice: Option<Range<usize>>,
    filter: Option<Mask>,
}

impl SelectStage {
    pub(crate) fn new(slice: Option<Range<usize>>, filter: Option<Mask>) -> Self {
        Self { slice, filter }
    }
}

impl Operator for SelectStage {
    fn compute(&mut self, input: Input, cx: &mut Cx<'_>) -> VortexResult<Step> {
        let Input::Batch(batch) = input else {
            return Ok(Step::End);
        };
        Ok(Step::Final(select(
            batch,
            self.slice.as_ref(),
            self.filter.as_ref(),
            cx.exec(),
        )?))
    }
}
