// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::BitAnd;
use std::ops::Range;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use tracing::trace;
use vortex_array::ArrayRef;
use vortex_array::IntoArray;
use vortex_array::MaskFuture;
use vortex_array::VortexSessionExecute;
use vortex_array::arrays::ChunkedArray;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::dtype::DType;
use vortex_array::dtype::FieldMask;
use vortex_array::expr::BoundExpression;
use vortex_array::serde::SerializedArray;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_mask::AllOr;
use vortex_mask::Mask;
use vortex_session::VortexSession;

use crate::layouts::SharedArrayFuture;
use crate::layouts::flat::FlatLayout;
use crate::layouts::flat::FlatRangeRead;
use crate::layouts::flat::PartialRows;
use crate::reader::LayoutReader;
use crate::reader::RowSplits;
use crate::reader::SplitRange;
use crate::segments::SegmentSource;

/// The threshold of mask density below which we will evaluate the expression only over the
/// selected rows, and above which we evaluate the expression over all rows and then select
/// after.
// TODO(ngates): more experimentation is needed, and this should probably be dynamic based on the
//  actual expression? Perhaps all expressions are given a selection mask to decide for themselves?
const EXPR_EVAL_THRESHOLD: f64 = 0.2;

/// A partial read is used only if it reads at most this fraction of the segment's data bytes.
/// Above it, reading the whole segment costs little more and keeps its single, coalescable request.
const PARTIAL_READ_MAX_FRACTION: f64 = 0.5;

/// Segments with fewer bytes of data than this are read whole: a partial read would save little.
const PARTIAL_READ_MIN_SEGMENT_BYTES: usize = 32 * 1024;

/// The most row ranges a partial read slices the array into. Selected runs separated by the
/// smallest gaps are merged until this many remain, bounding the planning work.
const PARTIAL_READ_MAX_RANGES: usize = 256;

pub struct FlatReader {
    layout: FlatLayout,
    name: Arc<str>,
    segment_source: Arc<dyn SegmentSource>,
    session: VortexSession,
}

impl FlatReader {
    pub(crate) fn new(
        layout: FlatLayout,
        name: Arc<str>,
        segment_source: Arc<dyn SegmentSource>,
        session: VortexSession,
    ) -> Self {
        Self {
            layout,
            name,
            segment_source,
            session,
        }
    }

    /// Register the segment request and return a future that would resolve into the deserialised array.
    fn array_future(&self) -> SharedArrayFuture {
        array_future(&self.layout, self.segment_source.as_ref(), &self.session)
    }

    /// Start reading only the rows `mask` selects from `row_range`, if the mask is already known
    /// and the layout has an inline array tree.
    ///
    /// Without an inline array tree, or while the mask is still being computed, we read the whole
    /// segment: its request is registered eagerly so that it can be coalesced with others.
    fn partial_read(&self, row_range: &Range<usize>, mask: &MaskFuture) -> Option<PartialRead> {
        self.layout.array_tree()?;
        let mask = mask.clone().now_or_never()?.ok()?;
        let whole_layout = row_range.start == 0
            && u64::try_from(row_range.end).is_ok_and(|end| end == self.layout.row_count());
        if mask.all_true() && whole_layout {
            return None;
        }

        let local_ranges = selected_ranges(&mask, PARTIAL_READ_MAX_RANGES);
        let row_ranges = local_ranges
            .iter()
            .map(|rows| row_range.start + rows.start..row_range.start + rows.end)
            .collect::<Vec<_>>();
        let read = match FlatRangeRead::try_new(&self.layout, row_ranges.clone(), &self.session) {
            Ok(read) => read,
            Err(err) => {
                trace!("Flat partial read of {} not possible: {err}", self.name);
                return None;
            }
        };

        if read.segment_data_nbytes() < PARTIAL_READ_MIN_SEGMENT_BYTES {
            return None;
        }

        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_precision_loss,
            clippy::cast_sign_loss,
            reason = "a heuristic byte budget"
        )]
        let max_bytes = (read.segment_data_nbytes() as f64 * PARTIAL_READ_MAX_FRACTION) as usize;
        Some(PartialRead {
            rows: read
                .with_max_bytes(max_bytes)
                .read(Arc::clone(&self.segment_source)),
            row_ranges,
            local_ranges,
            mask,
            layout: self.layout.clone(),
            segment_source: Arc::clone(&self.segment_source),
            session: self.session.clone(),
        })
    }
}

/// Register the segment request and return a future that would resolve into the deserialised array.
fn array_future(
    layout: &FlatLayout,
    segment_source: &dyn SegmentSource,
    session: &VortexSession,
) -> SharedArrayFuture {
    let row_count =
        usize::try_from(layout.row_count()).vortex_expect("row count must fit in usize");

    // We create the segment_fut here to ensure we give the segment reader visibility into
    // how to prioritize this segment, even if the `array` future has already been initialized.
    // This is gross... see the function's TODO for a maybe better solution?
    let segment_fut = segment_source.request(layout.segment_id());

    let ctx = layout.array_ctx().clone();
    let session = session.clone();
    let dtype = layout.dtype().clone();
    let array_tree = layout.array_tree().cloned();
    async move {
        let segment = segment_fut.await?;
        let parts = if let Some(array_tree) = array_tree {
            // Use the pre-stored flatbuffer from layout metadata combined with segment buffers.
            SerializedArray::from_flatbuffer_and_segment(array_tree, segment)?
        } else {
            // Parse the flatbuffer from the segment itself.
            SerializedArray::try_from(segment)?
        };
        parts
            .decode(&dtype, row_count, &ctx, &session)
            .map_err(Arc::new)
    }
    .boxed()
    .shared()
}

/// An in-flight read of only the rows a mask selects.
struct PartialRead {
    rows: BoxFuture<'static, VortexResult<Option<PartialRows>>>,
    /// The row ranges read, relative to the layout.
    row_ranges: Vec<Range<usize>>,
    /// The row ranges read, relative to the evaluated row range.
    local_ranges: Vec<Range<usize>>,
    mask: Mask,
    // To read the whole segment if the partial read gives up.
    layout: FlatLayout,
    segment_source: Arc<dyn SegmentSource>,
    session: VortexSession,
}

impl PartialRead {
    /// Resolve the array of the selected rows, with the mask they were selected by.
    async fn selected(self) -> VortexResult<(ArrayRef, Mask)> {
        let arrays = match self.rows.await? {
            Some(rows) => rows.arrays,
            None => {
                let array =
                    array_future(&self.layout, self.segment_source.as_ref(), &self.session).await?;
                self.row_ranges
                    .iter()
                    .map(|rows| array.slice(rows.clone()))
                    .collect::<VortexResult<_>>()?
            }
        };
        let arrays = arrays
            .into_iter()
            .zip(&self.local_ranges)
            .map(|(array, rows)| {
                let mask = self.mask.slice(rows.clone());
                if mask.all_true() {
                    Ok(array)
                } else {
                    array.filter(mask)
                }
            })
            .collect::<VortexResult<Vec<_>>>()?;
        let array = if arrays.len() == 1 {
            arrays.into_iter().next().vortex_expect("one array")
        } else {
            ChunkedArray::try_new(arrays, self.layout.dtype().clone())?.into_array()
        };
        Ok((array, self.mask))
    }
}

/// The ranges of rows `mask` selects, merging those separated by the smallest gaps until at most
/// `max_ranges` remain.
fn selected_ranges(mask: &Mask, max_ranges: usize) -> Vec<Range<usize>> {
    let AllOr::Some(slices) = mask.slices() else {
        return if mask.all_false() {
            Vec::new()
        } else {
            std::iter::once(0..mask.len()).collect()
        };
    };
    let mut ranges = slices
        .iter()
        .map(|&(start, end)| start..end)
        .collect::<Vec<_>>();
    if ranges.len() <= max_ranges {
        return ranges;
    }

    // Keep the widest gaps as the boundaries between ranges, and close all the others.
    let mut gaps = ranges
        .windows(2)
        .enumerate()
        .map(|(idx, pair)| (pair[1].start - pair[0].end, idx))
        .collect::<Vec<_>>();
    gaps.sort_unstable_by(|a, b| b.cmp(a));
    let mut boundaries = gaps[..max_ranges.saturating_sub(1)]
        .iter()
        .map(|&(_, idx)| idx)
        .collect::<Vec<_>>();
    boundaries.sort_unstable();

    let mut merged = Vec::with_capacity(max_ranges);
    let mut start = ranges[0].start;
    for idx in boundaries {
        merged.push(start..ranges[idx].end);
        start = ranges[idx + 1].start;
    }
    merged.push(start..ranges.pop().vortex_expect("non-empty").end);
    merged
}

impl LayoutReader for FlatReader {
    fn name(&self) -> &Arc<str> {
        &self.name
    }

    fn dtype(&self) -> &DType {
        self.layout.dtype()
    }

    fn row_count(&self) -> u64 {
        self.layout.row_count()
    }

    fn register_splits(
        &self,
        _field_mask: &[FieldMask],
        split_range: &SplitRange,
        splits: &mut RowSplits,
    ) -> VortexResult<()> {
        split_range.check_bounds(self.layout.row_count())?;
        splits.push(split_range.root_row_range().end);
        Ok(())
    }

    fn pruning_evaluation(
        &self,
        _row_range: &Range<u64>,
        _expr: &BoundExpression,
        mask: Mask,
    ) -> VortexResult<MaskFuture> {
        Ok(MaskFuture::ready(mask))
    }

    fn filter_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &BoundExpression,
        mask: MaskFuture,
    ) -> VortexResult<MaskFuture> {
        let row_range = usize::try_from(row_range.start)
            .vortex_expect("Row range begin must fit within FlatLayout size")
            ..usize::try_from(row_range.end)
                .vortex_expect("Row range end must fit within FlatLayout size");
        let name = Arc::clone(&self.name);
        let expr = expr.clone();
        let session = self.session.clone();

        if let Some(partial) = self.partial_read(&row_range, &mask) {
            return Ok(MaskFuture::new(mask.len(), async move {
                let (array, mask) = partial.selected().await?;
                let array = array.apply_bound(&expr)?;
                let mut ctx = session.create_execution_ctx();
                let array_mask = array.fill_null(false)?.execute::<Mask>(&mut ctx)?;
                trace!(
                    "Flat partial mask evaluation {} - {} (mask = {}) => {}",
                    name,
                    expr,
                    mask.density(),
                    array_mask.density(),
                );
                Ok(mask.intersect_by_rank(&array_mask))
            }));
        }

        let array = self.array_future();
        Ok(MaskFuture::new(mask.len(), async move {
            // TODO(ngates): if the mask density is low enough, or if the mask is dense within a range
            //  (as often happens with zone map pruning), then we could slice/filter the array prior
            //  to evaluating the expression.
            let mut array = array.clone().await?;
            let mask = mask.await?;

            // Slice the array based on the row mask.
            if row_range.start > 0 || row_range.end < array.len() {
                array = array.slice(row_range.clone())?;
            }

            let mask_density = mask.density();
            let array_mask = if mask_density < EXPR_EVAL_THRESHOLD {
                // We have the choice to apply the filter or the expression first, we apply the
                // expression first so that it can try pushing down itself and then the filter
                // after this.
                let array = array.apply_bound(&expr)?;
                let array = array.filter(mask.clone())?;
                let mut ctx = session.create_execution_ctx();
                let array_mask = array.fill_null(false)?.execute::<Mask>(&mut ctx)?;

                mask.intersect_by_rank(&array_mask)
            } else {
                // Run over the full array, with a simpler bitand at the end.
                let array = array.apply_bound(&expr)?;
                let mut ctx = session.create_execution_ctx();
                let array_mask = array.fill_null(false)?.execute::<Mask>(&mut ctx)?;

                mask.bitand(&array_mask)
            };

            trace!(
                "Flat mask evaluation {} - {} (mask = {}) => {}",
                name,
                expr,
                mask_density,
                array_mask.density(),
            );

            Ok(array_mask)
        }))
    }

    fn projection_evaluation(
        &self,
        row_range: &Range<u64>,
        expr: &BoundExpression,
        mask: MaskFuture,
    ) -> VortexResult<BoxFuture<'static, VortexResult<ArrayRef>>> {
        let row_range = usize::try_from(row_range.start)
            .vortex_expect("Row range begin must fit within FlatLayout size")
            ..usize::try_from(row_range.end)
                .vortex_expect("Row range end must fit within FlatLayout size");
        let name = Arc::clone(&self.name);
        let expr = expr.clone();

        if let Some(partial) = self.partial_read(&row_range, &mask) {
            return Ok(async move {
                trace!("Flat partial array evaluation {} - {}", name, expr);
                let (array, _) = partial.selected().await?;
                array.apply_bound(&expr)
            }
            .boxed());
        }

        let array = self.array_future();
        Ok(async move {
            trace!("Flat array evaluation {} - {}", name, expr);

            let mut array = array.clone().await?;
            let mask = mask.await?;

            // Slice the array based on the row mask.
            if row_range.start > 0 || row_range.end < array.len() {
                array = array.slice(row_range.clone())?;
            }

            // First apply the filter to the array.
            // NOTE(ngates): we *must* filter first before applying the expression, as the
            // expression may depend on the filtered rows being removed e.g.
            //  `CAST(a, u8) WHERE a < 256`
            if !mask.all_true() {
                array = array.filter(mask)?;
            }

            // Evaluate the projection expression.
            array = array.apply_bound(&expr)?;

            Ok(array)
        }
        .boxed())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use vortex_array::ArrayContext;
    use vortex_array::IntoArray;
    use vortex_array::MaskFuture;
    use vortex_array::VortexSessionExecute;
    use vortex_array::arrays::BoolArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::assert_arrays_eq;
    use vortex_array::expr::gt;
    use vortex_array::expr::lit;
    use vortex_array::expr::root;
    use vortex_array::validity::Validity;
    use vortex_buffer::buffer;
    use vortex_error::VortexResult;
    use vortex_io::runtime::single::block_on;
    use vortex_io::session::RuntimeSessionExt;
    use vortex_mask::Mask;

    use super::selected_ranges;
    use crate::LayoutStrategy;
    use crate::layouts::flat::writer::FlatLayoutStrategy;
    use crate::segments::TestSegments;
    use crate::sequence::SequenceId;
    use crate::sequence::SequentialArrayStreamExt;
    use crate::test::new_session;

    #[test]
    fn selected_ranges_merges_smallest_gaps() {
        let mask = Mask::from_slices(100, vec![(0, 1), (2, 3), (10, 11), (50, 51)]);
        assert_eq!(selected_ranges(&mask, 4), vec![0..1, 2..3, 10..11, 50..51]);
        assert_eq!(selected_ranges(&mask, 2), vec![0..11, 50..51]);
        assert_eq!(selected_ranges(&mask, 1), vec![0..51]);
        assert!(selected_ranges(&Mask::new_false(10), 4).is_empty());
    }

    #[test]
    fn flat_identity() -> VortexResult<()> {
        block_on(|handle| async {
            let session = new_session().with_handle(handle);
            let mut ctx = session.create_execution_ctx();
            let array_ctx = ArrayContext::empty();
            let segments = Arc::new(TestSegments::default());
            let (ptr, eof) = SequenceId::root().split();
            let array =
                PrimitiveArray::new(buffer![1, 2, 3, 4, 5], Validity::AllValid).into_array();
            let layout = FlatLayoutStrategy::default()
                .write_stream(
                    array_ctx.into(),
                    Arc::<TestSegments>::clone(&segments),
                    array.to_array_stream().sequenced(ptr),
                    eof,
                    &session,
                )
                .await?;

            assert_eq!(
                format!("{}", layout),
                "vortex.flat(i32?, rows=5, segments=[0])"
            );

            let reader = layout.new_reader("".into(), segments, &session, &Default::default())?;
            let expr = root().bind(reader.dtype())?;
            let result = reader
                .projection_evaluation(
                    &(0..layout.row_count()),
                    &expr,
                    MaskFuture::new_true(layout.row_count().try_into()?),
                )?
                .await?;

            assert_arrays_eq!(result, array, &mut ctx);

            Ok(())
        })
    }

    #[test]
    fn flat_expr() {
        block_on(|handle| async {
            let session = new_session().with_handle(handle);
            let mut ctx = session.create_execution_ctx();
            let array_ctx = ArrayContext::empty();

            let segments = Arc::new(TestSegments::default());
            let (ptr, eof) = SequenceId::root().split();
            let array =
                PrimitiveArray::new(buffer![1, 2, 3, 4, 5], Validity::AllValid).into_array();
            let layout = FlatLayoutStrategy::default()
                .write_stream(
                    array_ctx.into(),
                    Arc::<TestSegments>::clone(&segments),
                    array.to_array_stream().sequenced(ptr),
                    eof,
                    &session,
                )
                .await
                .unwrap();

            let reader = layout
                .new_reader("".into(), segments, &session, &Default::default())
                .unwrap();
            let expr = gt(root(), lit(3i32)).bind(reader.dtype()).unwrap();
            let result = reader
                .projection_evaluation(
                    &(0..layout.row_count()),
                    &expr,
                    MaskFuture::new_true(layout.row_count().try_into().unwrap()),
                )
                .unwrap()
                .await
                .unwrap();

            let expected = BoolArray::from_iter([false, false, false, true, true].map(Some));
            assert_arrays_eq!(result, expected, &mut ctx);
        })
    }

    #[test]
    fn flat_unaligned_row_mask() {
        block_on(|handle| async {
            let session = new_session().with_handle(handle);
            let mut ctx = session.create_execution_ctx();
            let array_ctx = ArrayContext::empty();
            let segments = Arc::new(TestSegments::default());
            let (ptr, eof) = SequenceId::root().split();
            let array =
                PrimitiveArray::new(buffer![1, 2, 3, 4, 5], Validity::AllValid).into_array();
            let layout = FlatLayoutStrategy::default()
                .write_stream(
                    array_ctx.into(),
                    Arc::<TestSegments>::clone(&segments),
                    array.to_array_stream().sequenced(ptr),
                    eof,
                    &session,
                )
                .await
                .unwrap();

            let reader = layout
                .new_reader("".into(), segments, &session, &Default::default())
                .unwrap();
            let expr = root().bind(reader.dtype()).unwrap();
            let result = reader
                .projection_evaluation(&(2..4), &expr, MaskFuture::new_true(2))
                .unwrap()
                .await
                .unwrap();

            let expected = PrimitiveArray::new(buffer![3i32, 4], Validity::AllValid).into_array();
            assert_arrays_eq!(result, expected, &mut ctx);
        })
    }
}
