// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;

use futures::FutureExt;
use futures::future::BoxFuture;
use vortex_array::buffer::BufferHandle;
use vortex_buffer::Alignment;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;

use crate::segments::SegmentId;
/// Static future resolving to a segment byte buffer.
pub type SegmentFuture = BoxFuture<'static, VortexResult<BufferHandle>>;

/// Provides segment data to a [`crate::LayoutReader`].
///
/// Implementations may issue asynchronous file reads, object-store requests, cache lookups, or
/// in-memory buffer slices. Returned futures must be independent and safe to poll concurrently.
pub trait SegmentSource: 'static + Send + Sync {
    /// Request a segment, returning a future that will eventually resolve to the segment data.
    fn request(&self, id: SegmentId) -> SegmentFuture;

    /// Request a byte range of a segment, relative to the start of the segment.
    ///
    /// The returned buffer has exactly `range.len()` bytes and is aligned to `alignment`.
    ///
    /// The default implementation requests the whole segment and slices it, so it is correct for
    /// every source but saves no IO. Sources that can read part of a segment should override it.
    fn request_range(
        &self,
        id: SegmentId,
        range: Range<usize>,
        alignment: Alignment,
    ) -> SegmentFuture {
        self.request(id)
            .map(move |segment| slice_segment(segment?, id, range, alignment))
            .boxed()
    }
}

/// Slice `range` out of a whole `segment` with the given alignment, returning an error if it is out
/// of bounds.
pub(crate) fn slice_segment(
    segment: BufferHandle,
    id: SegmentId,
    range: Range<usize>,
    alignment: Alignment,
) -> VortexResult<BufferHandle> {
    if range.start > range.end || range.end > segment.len() {
        vortex_bail!(
            "Range {range:?} is out of bounds for segment {id} of length {}",
            segment.len()
        );
    }
    segment
        .ensure_aligned(Alignment::none())?
        .slice(range)
        .ensure_aligned(alignment)
}
