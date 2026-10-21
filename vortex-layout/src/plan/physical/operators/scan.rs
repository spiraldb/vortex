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
use vortex_array::arrays::ListArray;
use vortex_array::arrays::ListView;
use vortex_array::arrays::ListViewArray;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::Struct;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::arrays::list::ListArrayExt;
use vortex_array::arrays::list::ListArraySlotsExt;
use vortex_array::arrays::listview::ListViewArraySlotsExt;
use vortex_array::arrays::listview::ListViewRebuildMode;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::buffer::BufferHandle;
use vortex_array::dtype::IntegerPType;
use vortex_array::match_each_integer_ptype;
use vortex_array::serde::SerializedArray;
use vortex_buffer::BufferMut;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::AllOr;
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
///
/// The slice is narrowed first to the rows from the first selected to the last, so a filter
/// keeping one run of rows is a slice, and a filtered list holds only the elements around the
/// rows it keeps.
fn select(
    array: ArrayRef,
    slice: Option<&Range<usize>>,
    filter: Option<&Mask>,
    ctx: &mut ExecutionCtx,
) -> VortexResult<ArrayRef> {
    let mut range = slice.cloned().unwrap_or(0..array.len());
    let filter = match filter {
        Some(filter) => {
            let (Some(first), Some(last)) = (filter.first(), filter.last()) else {
                return array.slice(0..0);
            };
            range = range.start + first..range.start + last + 1;
            let filter = filter.slice(first..last + 1);
            (!filter.all_true()).then_some(filter)
        }
        None => None,
    };
    let array = if range.start == 0 && range.end == array.len() {
        array
    } else {
        trim_elements(array.slice(range)?, ctx)?
    };
    match filter {
        Some(filter) => match list_views(&array, &filter, ctx)? {
            Some(views) => Ok(views),
            None => array.filter(filter),
        },
        None => Ok(array),
    }
}

/// The fraction of a list's rows a filter keeps from which [`list_views`] views them.
const DENSE_LIST_FILTER: f64 = 0.5;

/// The lists of `array` that a dense `mask` keeps, as views of their elements.
///
/// Filtering a list filters its elements, lazily, so every reader of the result, each sum of
/// it and its export, filters nearly all of them again. Views of the kept lists over the
/// unfiltered elements filter only the offsets. `None` when `array` is not a list of offsets or
/// `mask` is sparse, where filtering crops the elements to the few lists kept.
fn list_views(
    array: &ArrayRef,
    mask: &Mask,
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ArrayRef>> {
    let Some(list) = array.as_opt::<List>() else {
        return Ok(None);
    };
    if mask.density() < DENSE_LIST_FILTER {
        return Ok(None);
    }
    let offsets = list.offsets().clone().execute::<PrimitiveArray>(ctx)?;
    let (starts, sizes) = match_each_integer_ptype!(offsets.ptype(), |O| {
        let offsets = offsets.as_slice::<O>();
        let mut starts = BufferMut::<O>::with_capacity(mask.true_count());
        let mut sizes = BufferMut::<O>::with_capacity(mask.true_count());
        let mut keep = |row: usize| {
            starts.push(offsets[row]);
            sizes.push(offsets[row + 1] - offsets[row]);
        };
        match mask.slices() {
            AllOr::All => (0..list.len()).for_each(&mut keep),
            AllOr::None => {}
            AllOr::Some(slices) => slices
                .iter()
                .flat_map(|&(start, end)| start..end)
                .for_each(&mut keep),
        }
        (starts.freeze().into_array(), sizes.freeze().into_array())
    });
    let validity = list.validity()?.filter(mask)?;
    // SAFETY: every view is a kept list's own offset and size, within the elements, and the
    // offsets and sizes share their integer type; the validity is filtered by the same mask.
    Ok(Some(
        unsafe { ListViewArray::new_unchecked(list.elements().clone(), starts, sizes, validity) }
            .into_array(),
    ))
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
        return Ok(
            unsafe { ChunkedArray::new_unchecked(chunks, sliced.dtype().clone()) }.into_array(),
        );
    }
    trim_elements(sliced, ctx)
}

/// A slice of a list keeps every element of the lists around it, and exporting the slice
/// would decode and copy them all. Keeps only the elements the sliced lists hold, and of
/// elements that are lists themselves, only theirs.
fn trim_elements(array: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
    if array.is_empty() {
        return Ok(array);
    }
    let list = if let Some(list) = array.as_opt::<List>() {
        let list = list.into_owned();
        let start = list.offset_at(0, ctx)?;
        let end = list.offset_at(list.len(), ctx)?;
        if start == 0 && end == list.elements().len() {
            list
        } else {
            list.reset_offsets(false, ctx)?
        }
    } else if let Some(list) = array.as_opt::<ListView>() {
        let list = list.into_owned();
        if let Some(list) = views_to_list(&list, ctx)? {
            return Ok(list);
        }
        return Ok(list
            .rebuild(ListViewRebuildMode::TrimElements, ctx)?
            .into_array());
    } else {
        return Ok(array);
    };
    if !list.elements().dtype().is_list() {
        return Ok(list.into_array());
    }
    let elements = trim_elements(list.elements().clone(), ctx)?;
    // SAFETY: trimming keeps the elements' rows, so the offsets still index them.
    Ok(
        unsafe { ListArray::new_unchecked(elements, list.offsets().clone(), list.validity()?) }
            .into_array(),
    )
}

/// `views` as a list of offsets holding only their elements, when they are in order and do not
/// overlap, as the [`list_views`] of a filter's kept lists are; `None` otherwise.
///
/// Exporting views with gaps between them gathers their elements one index at a time. A list
/// of offsets over the runs of elements the views hold is exported a run at a time.
fn views_to_list(views: &ListViewArray, ctx: &mut ExecutionCtx) -> VortexResult<Option<ArrayRef>> {
    let offsets = views.offsets().clone().execute::<PrimitiveArray>(ctx)?;
    let sizes = views.sizes().clone().execute::<PrimitiveArray>(ctx)?;
    if offsets.ptype() != sizes.ptype() || views.elements().dtype().is_list() {
        return Ok(None);
    }
    match_each_integer_ptype!(offsets.ptype(), |O| {
        typed_views_to_list(views, offsets.as_slice::<O>(), sizes.as_slice::<O>())
    })
}

fn typed_views_to_list<O: IntegerPType>(
    views: &ListViewArray,
    starts: &[O],
    sizes: &[O],
) -> VortexResult<Option<ArrayRef>> {
    let mut slices: Vec<(usize, usize)> = Vec::with_capacity(starts.len());
    let mut ends = BufferMut::<O>::with_capacity(starts.len() + 1);
    let mut total = O::zero();
    ends.push(total);
    for (&start, &size) in starts.iter().zip(sizes) {
        let (from, to): (usize, usize) = (start.as_(), (start + size).as_());
        match slices.last_mut() {
            Some(last) if last.1 > from => return Ok(None),
            Some(last) if last.1 == from => last.1 = to,
            _ if from < to => slices.push((from, to)),
            _ => {}
        }
        total += size;
        ends.push(total);
    }
    let (Some(&(low, _)), Some(&(_, high))) = (slices.first(), slices.last()) else {
        return Ok(None);
    };
    // Each run decodes straight into the export, where a filter of the elements between the
    // runs would build and apply a mask first.
    let elements = if slices.len() == 1 {
        views.elements().slice(low..high)?
    } else {
        let slices = slices
            .iter()
            .map(|&(from, to)| views.elements().slice(from..to))
            .collect::<VortexResult<Vec<_>>>()?;
        ChunkedArray::try_new(slices, views.elements().dtype().clone())?.into_array()
    };
    // SAFETY: the elements are those of the views in order, and `ends` their running lengths,
    // from zero to the elements' length.
    Ok(Some(
        unsafe {
            ListArray::new_unchecked(elements, ends.freeze().into_array(), views.validity()?)
        }
        .into_array(),
    ))
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

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::List;
    use vortex_array::arrays::ListViewArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::validity::Validity;
    use vortex_buffer::Buffer;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;

    use super::views_to_list;
    use crate::test::new_session;

    /// Ordered views become a list of only their elements; overlapping or empty ones do not.
    #[rstest]
    #[case::contiguous(&[1, 3], &[2, 3], true)]
    #[case::gaps(&[0, 3, 7], &[2, 1, 3], true)]
    #[case::empty_between(&[0, 2, 5], &[2, 0, 1], true)]
    #[case::overlapping(&[0, 1], &[3, 2], false)]
    #[case::out_of_order(&[4, 0], &[2, 2], false)]
    #[case::all_empty(&[3, 5], &[0, 0], false)]
    fn views_become_lists(
        #[case] offsets: &[u32],
        #[case] sizes: &[u32],
        #[case] converted: bool,
    ) -> VortexResult<()> {
        let mut ctx = new_session().create_execution_ctx();
        let views = ListViewArray::new(
            buffer![0i64, 1, 2, 3, 4, 5, 6, 7, 8, 9].into_array(),
            Buffer::copy_from(offsets).into_array(),
            Buffer::copy_from(sizes).into_array(),
            Validity::AllValid,
            &mut ctx,
        );
        let list = views_to_list(&views, &mut ctx)?;
        assert_eq!(list.is_some(), converted);
        if let Some(list) = list {
            assert!(list.is::<List>());
            assert_arrays_eq!(list, views.into_array(), &mut ctx);
        }
        Ok(())
    }
}
