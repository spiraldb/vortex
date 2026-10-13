// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Reading only the bytes of a flat layout's segment that some of its rows need.
//!
//! A flat layout with an inline array tree knows the shape of its array, and where each of its
//! buffers lives in the segment, before any of the segment is read. [`FlatRangeRead`] decodes the
//! array over placeholder buffers that record which bytes of the segment they stand for but hold
//! no data, slices it to the requested rows, and lets the optimizer push those slices down through
//! the encodings. The byte ranges left in the placeholders afterwards are the bytes the rows need:
//! a sliced primitive array keeps a few bytes of its values buffer, while a sliced dictionary keeps
//! a few of its codes but all of its values. Reading those ranges and swapping them into the
//! placeholders produces the sliced arrays.
//!
//! Some encodings must look at their data to slice, such as a variable-length array reading the
//! offsets of the sliced rows. Reading a placeholder fails, and records the bytes it asked for. We
//! then read those bytes and try again, so each such step costs an IO round trip but still reads
//! only the bytes that are looked at.

use std::any::Any;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::hash::Hash;
use std::hash::Hasher;
use std::ops::Range;
use std::panic;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::future::try_join_all;
use parking_lot::Mutex;
use parking_lot::RwLock;
use tracing::trace;
use vortex_array::ArrayRef;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::Slice;
use vortex_array::arrays::VarBinView;
use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::varbinview::BinaryView;
use vortex_array::buffer::BufferHandle;
use vortex_array::buffer::DeviceBuffer;
use vortex_array::optimizer::ArrayOptimizer;
use vortex_array::serde::SegmentBufferRange;
use vortex_array::serde::SerializedArray;
use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_buffer::ByteBuffer;
use vortex_buffer::ByteBufferMut;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_session::VortexSession;

use crate::layouts::flat::FlatLayout;
use crate::segments::SegmentSource;

/// The most IO round trips a partial read makes before giving up.
const MAX_ROUND_TRIPS: usize = 4;

/// Buffers up to this size are read whole with the first request.
const PREFETCH_BUFFER_BYTES: usize = 4096;

/// A read of some rows of a [`FlatLayout`] that fetches only the parts of its segment those rows
/// need.
///
/// It requires the layout to carry its array tree inline (see
/// [`FlatLayoutStrategy::with_inline_array_tree`]), so that the array can be decoded before any
/// of its segment is read.
///
/// [`FlatLayoutStrategy::with_inline_array_tree`]: crate::layouts::flat::writer::FlatLayoutStrategy::with_inline_array_tree
pub struct FlatRangeRead {
    layout: FlatLayout,
    row_ranges: Vec<Range<usize>>,
    session: VortexSession,
    buffer_ranges: Vec<SegmentBufferRange>,
    max_bytes: usize,
}

/// The result of a [`FlatRangeRead`].
#[derive(Debug)]
pub struct PartialRows {
    /// One array per requested row range.
    pub arrays: Vec<ArrayRef>,
    /// The bytes of the segment that were read, sorted and disjoint.
    pub byte_ranges: Vec<Range<usize>>,
    /// The number of IO round trips made.
    pub round_trips: usize,
}

impl PartialRows {
    /// The number of segment bytes read.
    pub fn nbytes(&self) -> usize {
        self.byte_ranges.iter().map(Range::len).sum()
    }
}

impl FlatRangeRead {
    /// Prepare to read each of `row_ranges` from `layout`.
    ///
    /// Fails if the layout has no inline array tree, or if a row range is out of bounds.
    pub fn try_new(
        layout: &FlatLayout,
        row_ranges: Vec<Range<usize>>,
        session: &VortexSession,
    ) -> VortexResult<Self> {
        let array_tree = layout
            .array_tree()
            .ok_or_else(|| vortex_err!("Partial flat layout reads require an inline array tree"))?;
        let row_count = usize::try_from(layout.row_count())?;
        for rows in &row_ranges {
            vortex_ensure!(
                rows.start <= rows.end && rows.end <= row_count,
                "Row range {rows:?} is out of bounds for a flat layout of {row_count} rows"
            );
        }
        let buffer_ranges = SerializedArray::segment_buffer_ranges(array_tree.clone())?;

        Ok(Self {
            layout: layout.clone(),
            row_ranges,
            session: session.clone(),
            buffer_ranges,
            max_bytes: usize::MAX,
        })
    }

    /// Give up on the partial read once it would read more than `max_bytes` of the segment.
    pub fn with_max_bytes(mut self, max_bytes: usize) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// The number of bytes of data buffers in the whole segment.
    pub fn segment_data_nbytes(&self) -> usize {
        self.buffer_ranges
            .iter()
            .map(|buffer| buffer.range.end)
            .max()
            .unwrap_or(0)
    }

    /// Read the rows from `segment_source`, returning one array per requested row range.
    ///
    /// Resolves to `None` if the rows cannot be read without reading more than the configured
    /// maximum number of bytes, or without decoding data in a way that partial reads do not
    /// support. The caller should then read the whole segment.
    pub fn read(
        self,
        segment_source: Arc<dyn SegmentSource>,
    ) -> BoxFuture<'static, VortexResult<Option<PartialRows>>> {
        async move {
            let bytes = Arc::new(SegmentBytes::default());
            for round_trip in 0..=MAX_ROUND_TRIPS {
                // Some encodings read their buffers directly rather than through the buffer handle,
                // such as to validate them while decoding, which panics on a placeholder. We read
                // the whole segment for them instead.
                let attempt = panic::catch_unwind(AssertUnwindSafe(|| self.plan(&bytes)))
                    .unwrap_or_else(|_| {
                        Err(vortex_err!("Decoding with placeholder buffers panicked"))
                    });
                let (arrays, mut needed) = match attempt {
                    Ok(arrays) => {
                        let mut needed = Vec::new();
                        for array in &arrays {
                            collect_unread(array, &mut needed)?;
                        }
                        (Some(arrays), needed)
                    }
                    Err(err) => {
                        let missing = bytes.take_missing();
                        if missing.is_empty() {
                            trace!("Partial flat read failed, reading whole segment: {err}");
                            return Ok(None);
                        }
                        (None, missing)
                    }
                };

                // Small buffers are read along with the first request: they cost little more and
                // are often needed to decode or slice, which would otherwise take round trips.
                if round_trip == 0 && !needed.is_empty() {
                    needed.extend(
                        self.buffer_ranges
                            .iter()
                            .filter(|buffer| buffer.range.len() <= PREFETCH_BUFFER_BYTES)
                            .map(|buffer| (buffer.range.clone(), buffer.alignment)),
                    );
                }
                let unread = bytes.unread(needed);
                if let Some(arrays) = arrays
                    && unread.is_empty()
                {
                    let arrays = arrays
                        .iter()
                        .map(fill_placeholders)
                        .collect::<VortexResult<_>>()?;
                    return Ok(Some(PartialRows {
                        arrays,
                        byte_ranges: bytes.read_ranges(),
                        round_trips: round_trip,
                    }));
                }

                let unread_bytes: usize = unread.iter().map(|(range, _)| range.len()).sum();
                if round_trip == MAX_ROUND_TRIPS
                    || bytes.nbytes().saturating_add(unread_bytes) > self.max_bytes
                {
                    return Ok(None);
                }

                let segment_id = self.layout.segment_id();
                let reads = try_join_all(unread.into_iter().map(|(range, alignment)| {
                    segment_source
                        .request_range(segment_id, range.clone(), alignment)
                        .map(move |buffer| {
                            Ok::<_, VortexError>((range, buffer?.try_into_host_sync()?))
                        })
                }))
                .await?;
                bytes.insert(reads);
            }
            Ok(None)
        }
        .boxed()
    }

    /// Decode the array over placeholders for the bytes of `bytes`, and slice it to the rows.
    fn plan(&self, bytes: &Arc<SegmentBytes>) -> VortexResult<Vec<ArrayRef>> {
        // Buffers already read are decoded from their bytes, so code that reads host memory
        // directly can use them.
        let buffers = self
            .buffer_ranges
            .iter()
            .map(|buffer| match bytes.get(&buffer.range) {
                Some(read) => BufferHandle::new_host(read.aligned(buffer.alignment)),
                None => Placeholder::handle(
                    Arc::clone(bytes),
                    vec![buffer.range.clone()],
                    buffer.alignment,
                ),
            })
            .collect();

        let row_count = usize::try_from(self.layout.row_count())?;
        let array_tree = self
            .layout
            .array_tree()
            .ok_or_else(|| vortex_err!("Missing inline array tree"))?;
        let array = SerializedArray::from_flatbuffer_with_buffers(array_tree.clone(), buffers)?
            .decode(
                self.layout.dtype(),
                row_count,
                self.layout.array_ctx(),
                &self.session,
            )?;

        // Attempt every row range, so that one round trip reads all the bytes they are missing.
        let mut ctx = self.session.create_execution_ctx();
        try_all(self.row_ranges.iter().map(|rows| {
            let sliced = array.slice(rows.clone())?;
            map_tree(&sliced, &mut |node| narrow(node, &mut ctx))
        }))
    }
}

/// The bytes of a segment read so far, shared by its placeholders.
#[derive(Default)]
struct SegmentBytes {
    /// Sorted, disjoint byte ranges of the segment with their data.
    read: RwLock<Vec<(Range<usize>, ByteBuffer)>>,
    /// Ranges that were accessed while unread.
    missing: Mutex<Vec<(Range<usize>, Alignment)>>,
}

impl SegmentBytes {
    fn nbytes(&self) -> usize {
        self.read.read().iter().map(|(range, _)| range.len()).sum()
    }

    fn read_ranges(&self) -> Vec<Range<usize>> {
        self.read
            .read()
            .iter()
            .map(|(range, _)| range.clone())
            .collect()
    }

    fn take_missing(&self) -> Vec<(Range<usize>, Alignment)> {
        std::mem::take(&mut *self.missing.lock())
    }

    /// The bytes of `range`, if they have all been read.
    fn get(&self, range: &Range<usize>) -> Option<ByteBuffer> {
        if range.is_empty() {
            return Some(ByteBuffer::empty());
        }
        let read = self.read.read();
        let first = read.partition_point(|(read, _)| read.end <= range.start);

        // The common case: one read covers the whole range, so we can slice it without copying.
        let (first_range, first_bytes) = read.get(first)?;
        if first_range.start <= range.start && range.end <= first_range.end {
            let start = range.start - first_range.start;
            return Some(first_bytes.slice_unaligned(start..start + range.len()));
        }

        // Otherwise the range must be covered by adjacent reads, which we copy together.
        let mut bytes = ByteBufferMut::with_capacity(range.len());
        let mut pos = range.start;
        for (read_range, read_bytes) in &read[first..] {
            if pos == range.end {
                break;
            }
            if read_range.start > pos {
                return None;
            }
            let end = range.end.min(read_range.end);
            bytes.extend_from_slice(
                &read_bytes.as_slice()[pos - read_range.start..end - read_range.start],
            );
            pos = end;
        }
        (pos == range.end).then(|| bytes.freeze())
    }

    /// The parts of `ranges` not read yet, coalesced, each with the alignment of the buffer
    /// it starts with when it starts at the start of a requested range.
    fn unread(&self, ranges: Vec<(Range<usize>, Alignment)>) -> Vec<(Range<usize>, Alignment)> {
        let read = self.read.read();
        let mut unread = Vec::new();
        for (range, alignment) in coalesce(ranges) {
            let mut pos = range.start;
            let first = read.partition_point(|(read, _)| read.end <= range.start);
            for (read_range, _) in &read[first..] {
                if read_range.start >= range.end {
                    break;
                }
                if read_range.start > pos {
                    unread.push((pos..read_range.start, alignment_at(pos, &range, alignment)));
                }
                pos = pos.max(read_range.end);
            }
            if pos < range.end {
                unread.push((pos..range.end, alignment_at(pos, &range, alignment)));
            }
        }
        unread
    }

    fn insert(&self, reads: Vec<(Range<usize>, ByteBuffer)>) {
        let mut read = self.read.write();
        read.extend(reads);
        read.sort_by_key(|(range, _)| range.start);
    }
}

fn alignment_at(pos: usize, range: &Range<usize>, alignment: Alignment) -> Alignment {
    if pos == range.start {
        alignment
    } else {
        Alignment::none()
    }
}

/// Sort ranges and merge those that overlap or touch, keeping the alignment of the buffer each
/// merged range starts with. Empty ranges need no IO and are dropped.
fn coalesce(mut ranges: Vec<(Range<usize>, Alignment)>) -> Vec<(Range<usize>, Alignment)> {
    ranges.retain(|(range, _)| !range.is_empty());
    // Of the buffers starting at the same offset, the most aligned one decides the alignment.
    ranges.sort_by(|(a, a_align), (b, b_align)| a.start.cmp(&b.start).then(b_align.cmp(a_align)));

    let mut merged: Vec<(Range<usize>, Alignment)> = Vec::with_capacity(ranges.len());
    for (range, alignment) in ranges {
        match merged.last_mut() {
            Some((last, _)) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => merged.push((range, alignment)),
        }
    }
    merged
}

/// A placeholder buffer for bytes of a segment that may not have been read yet.
///
/// Its bytes are the concatenation of `pieces` of the segment. It reports their length and
/// alignment, and slicing narrows the pieces, so arrays can be decoded, sliced and optimized over
/// it without IO. Reading its bytes succeeds once they have been read, and otherwise fails and
/// records the missing pieces so they can be read for the next attempt.
#[derive(Clone)]
struct Placeholder {
    bytes: Arc<SegmentBytes>,
    pieces: Arc<[Range<usize>]>,
    len: usize,
    alignment: Alignment,
}

impl Placeholder {
    fn handle(
        bytes: Arc<SegmentBytes>,
        pieces: Vec<Range<usize>>,
        alignment: Alignment,
    ) -> BufferHandle {
        let len = pieces.iter().map(Range::len).sum();
        BufferHandle::new_device(Arc::new(Self {
            bytes,
            pieces: pieces.into(),
            len,
            alignment,
        }))
    }

    fn of(buffer: &BufferHandle) -> Option<&Self> {
        buffer
            .as_device_opt()
            .and_then(|device| device.as_any().downcast_ref::<Self>())
    }

    /// The pieces with the alignment each must be read with.
    fn aligned_pieces(&self) -> impl Iterator<Item = (Range<usize>, Alignment)> + '_ {
        self.pieces.iter().enumerate().map(|(idx, piece)| {
            let alignment = if idx == 0 {
                self.alignment
            } else {
                Alignment::none()
            };
            (piece.clone(), alignment)
        })
    }

    /// The pieces of the segment holding the placeholder's bytes `range`.
    fn segment_pieces(&self, range: Range<usize>) -> Vec<Range<usize>> {
        let mut pieces = Vec::new();
        let mut pos = 0;
        for piece in self.pieces.iter() {
            let piece_range = pos..pos + piece.len();
            pos = piece_range.end;
            let start = range.start.max(piece_range.start);
            let end = range.end.min(piece_range.end);
            if start < end {
                let offset = piece.start - piece_range.start;
                pieces.push(start + offset..end + offset);
            }
        }
        pieces
    }

    /// The placeholder's bytes, or an error recording the pieces that have not been read.
    fn read(&self, alignment: Alignment) -> VortexResult<ByteBuffer> {
        let alignment = alignment.max(self.alignment);
        let pieces = self
            .pieces
            .iter()
            .map(|piece| self.bytes.get(piece))
            .collect::<Option<Vec<_>>>();
        let Some(pieces) = pieces else {
            let mut missing = self.bytes.missing.lock();
            missing.extend(
                self.aligned_pieces()
                    .filter(|(piece, _)| self.bytes.get(piece).is_none()),
            );
            vortex_bail!("Bytes {:?} of the segment have not been read", self.pieces);
        };

        if let [piece] = pieces.as_slice() {
            return Ok(piece.clone().aligned(alignment));
        }
        let mut bytes = ByteBufferMut::with_capacity_aligned(self.len, alignment);
        for piece in &pieces {
            bytes.extend_from_slice(piece.as_slice());
        }
        Ok(bytes.freeze())
    }
}

impl Debug for Placeholder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Placeholder")
            .field("pieces", &self.pieces)
            .field("alignment", &self.alignment)
            .finish()
    }
}

impl PartialEq for Placeholder {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.bytes, &other.bytes)
            && self.pieces == other.pieces
            && self.alignment == other.alignment
    }
}

impl Eq for Placeholder {}

impl Hash for Placeholder {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pieces.hash(state);
        self.alignment.hash(state);
    }
}

impl DeviceBuffer for Placeholder {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn len(&self) -> usize {
        self.len
    }

    fn alignment(&self) -> Alignment {
        self.alignment
    }

    fn copy_to_host_sync(&self, alignment: Alignment) -> VortexResult<ByteBuffer> {
        self.read(alignment)
    }

    fn copy_to_host(
        &self,
        alignment: Alignment,
    ) -> VortexResult<BoxFuture<'static, VortexResult<ByteBuffer>>> {
        let bytes = self.read(alignment)?;
        Ok(async move { Ok(bytes) }.boxed())
    }

    fn slice(&self, range: Range<usize>) -> Arc<dyn DeviceBuffer> {
        assert!(
            range.start <= range.end && range.end <= self.len,
            "slice {range:?} out of bounds of {} bytes",
            self.len
        );
        Arc::new(Self {
            bytes: Arc::clone(&self.bytes),
            pieces: self.segment_pieces(range.clone()).into(),
            len: range.len(),
            alignment: self.alignment,
        })
    }

    fn aligned(self: Arc<Self>, alignment: Alignment) -> VortexResult<Arc<dyn DeviceBuffer>> {
        // Bytes are aligned when they are read, so any alignment can be promised here.
        Ok(Arc::new(Self {
            alignment,
            ..(*self).clone()
        }))
    }
}

/// Rebuild `array` bottom-up, applying `f` to each node after its children.
fn map_tree(
    array: &ArrayRef,
    f: &mut dyn FnMut(ArrayRef) -> VortexResult<ArrayRef>,
) -> VortexResult<ArrayRef> {
    let mut array = array.clone();
    if array.nchildren() > 0 {
        let slots = try_all(
            array
                .slots()
                .iter()
                .map(|slot| slot.as_ref().map(|child| map_tree(child, f)).transpose()),
        )?;
        // SAFETY: every rewrite applied by `f` preserves the logical values of the node.
        array = unsafe { array.with_slots(slots)? };
    }
    f(array)
}

/// Collect all of `results`, or return the first error after evaluating every one.
///
/// Evaluating them all lets each one record the bytes it is missing.
fn try_all<T, C: FromIterator<T>>(
    results: impl Iterator<Item = VortexResult<T>>,
) -> VortexResult<C> {
    let results = results.collect::<Vec<_>>();
    results.into_iter().collect()
}

/// Narrow a node of a sliced array to the bytes its rows need.
fn narrow(array: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
    narrow_varbinview(push_down_slice(array, ctx)?)
}

/// Push a slice that no reduce rule could push into its child down with the child's slice kernel.
///
/// Slice kernels may read some of the child's data, such as the positions of its patches, which
/// placeholders serve once those bytes have been read.
fn push_down_slice(array: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
    if !array.is::<Slice>() {
        return Ok(array);
    }
    // Slice kernels search positional children, such as patch indices or run ends, in host
    // memory, so these must be read first.
    if let Some(child) = array.nth_child(0) {
        for (name, grandchild) in child.named_children() {
            if is_positional(&name) {
                require_read(&grandchild)?;
            }
        }
    }
    match array.execute_parent(ctx)? {
        Some(sliced) => map_tree(&sliced.optimize()?, &mut |node| narrow(node, ctx)),
        None => Ok(array),
    }
}

/// Whether a child slot holds positions that slicing searches.
fn is_positional(slot_name: &str) -> bool {
    slot_name == "ends" || slot_name.ends_with("indices") || slot_name.ends_with("offsets")
}

/// Fail if any placeholder in `array` has not been read, recording its pieces as missing.
fn require_read(array: &ArrayRef) -> VortexResult<()> {
    let mut unread = Vec::new();
    let mut state = None;
    visit_placeholders(array, &mut |placeholder| {
        unread.extend(placeholder.aligned_pieces());
        state.get_or_insert_with(|| Arc::clone(&placeholder.bytes));
    });
    let Some(state) = state else {
        return Ok(());
    };
    let unread = state.unread(unread);
    if unread.is_empty() {
        return Ok(());
    }
    state.missing.lock().extend(unread);
    vortex_bail!("Positional children must be read before slicing")
}

fn visit_placeholders(array: &ArrayRef, f: &mut dyn FnMut(&Placeholder)) {
    for buffer in array.buffer_handles() {
        if let Some(placeholder) = Placeholder::of(&buffer) {
            f(placeholder);
        }
    }
    for child in array.children_iter() {
        visit_placeholders(child, f);
    }
}

/// Collect the unread pieces of the placeholders in `array`.
fn collect_unread(
    array: &ArrayRef,
    ranges: &mut Vec<(Range<usize>, Alignment)>,
) -> VortexResult<()> {
    for buffer in array.buffer_handles() {
        if let Some(placeholder) = Placeholder::of(&buffer) {
            ranges.extend(placeholder.aligned_pieces());
        } else if buffer.is_on_device() {
            vortex_bail!("Unexpected device buffer in a partial flat read");
        }
    }
    for child in array.children_iter() {
        collect_unread(child, ranges)?;
    }
    Ok(())
}

/// Replace every placeholder buffer in `array` with its bytes, which must have been read.
fn fill_placeholders(array: &ArrayRef) -> VortexResult<ArrayRef> {
    map_tree(array, &mut |array| {
        let buffers = array.buffer_handles();
        if !buffers
            .iter()
            .any(|buffer| Placeholder::of(buffer).is_some())
        {
            return Ok(array);
        }
        let buffers = buffers
            .into_iter()
            .map(|buffer| match Placeholder::of(&buffer) {
                Some(placeholder) => Ok(BufferHandle::new_host(
                    placeholder.read(placeholder.alignment)?,
                )),
                None => Ok(buffer),
            })
            .collect::<VortexResult<Vec<_>>>()?;
        // SAFETY: each placeholder is replaced by the bytes it stands for, with the same length
        // and alignment.
        unsafe { array.with_buffers(buffers) }
    })
}

/// Narrow the data buffers of a sliced `VarBinView` array to the bytes its views reference.
///
/// Slicing a `VarBinView` array keeps all of its data buffers, but the views of the remaining rows
/// reference only a few of their bytes. Once the views are read, we replace each data buffer with
/// a placeholder for just the referenced bytes, and point the views at their new offsets.
fn narrow_varbinview(array: ArrayRef) -> VortexResult<ArrayRef> {
    let Some(varbinview) = array.as_opt::<VarBinView>() else {
        return Ok(array);
    };
    let data_buffers = varbinview.data_buffers();
    let placeholders = data_buffers.iter().map(Placeholder::of).collect::<Vec<_>>();
    if placeholders.iter().all(Option::is_none) {
        return Ok(array);
    }

    // Fails, recording the views as missing, until the views have been read.
    let views_bytes = varbinview
        .views_handle()
        .try_to_host_sync()?
        .aligned(Alignment::of::<BinaryView>());
    let views = Buffer::<BinaryView>::from_byte_buffer(views_bytes);

    // The byte ranges of each unread data buffer referenced by any view, coalesced. Buffers that
    // were already read are kept whole.
    let mut referenced = data_buffers
        .iter()
        .zip(&placeholders)
        .map(|(buffer, placeholder)| match placeholder {
            Some(_) => Vec::new(),
            None => vec![(0..buffer.len(), Alignment::none())],
        })
        .collect::<Vec<_>>();
    for view in views.iter().filter(|view| !view.is_inlined()) {
        let view = view.as_view();
        let buffer = view.buffer_index as usize;
        vortex_ensure!(
            buffer < referenced.len(),
            "View references missing buffer {buffer}"
        );
        if placeholders[buffer].is_some() {
            referenced[buffer].push((view.as_range(), Alignment::none()));
        }
    }
    let referenced = referenced.into_iter().map(coalesce).collect::<Vec<_>>();

    let unchanged = referenced
        .iter()
        .zip(data_buffers.iter())
        .all(|(ranges, buffer)| {
            buffer.is_empty()
                || matches!(ranges.as_slice(), [(range, _)] if range.len() == buffer.len())
        });
    if unchanged {
        return Ok(array);
    }

    // The new offset of each referenced range within its narrowed buffer.
    let new_offset = |buffer: usize, offset: usize| -> usize {
        let mut base = 0;
        for (range, _) in &referenced[buffer] {
            if range.contains(&offset) {
                return base + offset - range.start;
            }
            base += range.len();
        }
        unreachable!("every view offset is in a referenced range")
    };
    let mut new_views = BufferMut::<BinaryView>::with_capacity(views.len());
    for view in views.iter() {
        if view.is_inlined() {
            new_views.push(*view);
        } else {
            let view_ref = view.as_view();
            let offset = new_offset(view_ref.buffer_index as usize, view_ref.offset as usize);
            let offset = u32::try_from(offset)?;
            new_views.push(BinaryView::from(
                view_ref.with_buffer_and_offset(view_ref.buffer_index, offset),
            ));
        }
    }

    let new_buffers = data_buffers
        .iter()
        .zip(&placeholders)
        .zip(&referenced)
        .map(|((buffer, placeholder), ranges)| match placeholder {
            Some(placeholder) => {
                let pieces = ranges
                    .iter()
                    .flat_map(|(range, _)| placeholder.segment_pieces(range.clone()))
                    .collect();
                Placeholder::handle(Arc::clone(&placeholder.bytes), pieces, Alignment::none())
            }
            None => buffer.clone(),
        })
        .collect::<Arc<[_]>>();

    let validity = array.validity()?;
    // SAFETY: the views reference the same bytes as before, at their offsets in the narrowed
    // buffers, so the array has the same values.
    let narrowed = unsafe {
        VarBinViewArray::new_handle_unchecked(
            BufferHandle::new_host(new_views.freeze().into_byte_buffer()),
            new_buffers,
            array.dtype().clone(),
            validity,
        )
    };
    Ok(narrowed.into_array())
}

#[cfg(test)]
mod tests;
