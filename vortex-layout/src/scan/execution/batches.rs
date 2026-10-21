// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Cutting a projection's output into batches its consumer processes in cache.

use std::collections::VecDeque;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::Canonical;
use vortex_array::ExecutionCtx;
use vortex_array::IntoArray;
use vortex_array::arrays::Chunked;
use vortex_array::arrays::ChunkedArray;
use vortex_array::arrays::List;
use vortex_array::arrays::ListView;
use vortex_array::arrays::Struct;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::chunked::ChunkedArrayExt;
use vortex_array::arrays::list::ListArrayExt;
use vortex_array::arrays::list::ListArraySlotsExt;
use vortex_array::arrays::listview::ListViewArraySlotsExt;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::dtype::DType;
use vortex_error::VortexResult;
use vortex_error::vortex_bail;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::plan::physical::operators::list_views;
use crate::plan::physical::operators::slice_trimmed;
use crate::scan::execution::ProjectedFilter;

/// Rows a batch gathers before it is emitted. The plan produces rows in pieces at the stored
/// chunks' boundaries; pieces at least this long are emitted as they are, since joining them
/// would only copy rows the consumer splits again.
const BATCH_ROWS: usize = 8192;

/// Exported bytes a batch holds before it is emitted, and the most a batch is cut to.
///
/// A wide row, as a genotype list of thousands of values, makes a batch of [`BATCH_ROWS`] rows
/// hundreds of megabytes. Every expression its consumer evaluates over it then allocates and
/// writes as much again, out of cache and straight from the operating system. Batches of a few
/// megabytes keep those intermediates in cache and reuse the allocator's memory.
const BATCH_BYTES: u64 = 4 << 20;

/// Gathers a projection's pieces, in row order, into batches of about [`BATCH_ROWS`] rows or
/// [`BATCH_BYTES`] bytes once exported, whichever is fewer, cutting pieces of wide rows.
pub(super) struct Batcher {
    dtype: DType,
    pieces: Vec<ArrayRef>,
    rows: usize,
    bytes: u64,
    ready: VecDeque<ArrayRef>,
}

impl Batcher {
    pub(super) fn new(dtype: DType) -> Self {
        Self {
            dtype,
            pieces: Vec::new(),
            rows: 0,
            bytes: 0,
            ready: VecDeque::new(),
        }
    }

    /// Adds the next piece.
    pub(super) fn push(&mut self, piece: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<()> {
        let rows = piece.len();
        if rows == 0 {
            return Ok(());
        }
        let bytes = exported_bytes(&piece, ctx)?;
        let per_row = bytes.div_ceil(rows as u64).max(1);
        let batch_rows = usize::try_from(BATCH_BYTES / per_row)
            .unwrap_or(usize::MAX)
            .clamp(1, BATCH_ROWS);
        if rows > batch_rows && batch_rows < BATCH_ROWS {
            self.flush()?;
            // Each batch cut from the piece slices every field. A computed field, such as a
            // list sum, would then be computed again over each slice of its inputs, so it is
            // computed once here. Lists stay lazy: they are cut so that each batch exports only
            // its own elements.
            let piece = narrow_fields(piece, ctx)?;
            for start in (0..rows).step_by(batch_rows) {
                let end = (start + batch_rows).min(rows);
                self.ready
                    .push_back(slice_trimmed(&piece, start..end, ctx)?);
            }
            return Ok(());
        }
        self.rows += rows;
        self.bytes += bytes;
        self.pieces.push(piece);
        if self.rows >= BATCH_ROWS || self.bytes >= BATCH_BYTES {
            self.flush()?;
        }
        Ok(())
    }

    /// The next batch ready to emit.
    pub(super) fn pop(&mut self) -> Option<ArrayRef> {
        self.ready.pop_front()
    }

    /// Whether a batch is ready to emit.
    pub(super) fn has_ready(&self) -> bool {
        !self.ready.is_empty()
    }

    /// Makes the pieces gathered so far a batch.
    pub(super) fn flush(&mut self) -> VortexResult<()> {
        self.rows = 0;
        self.bytes = 0;
        let mut pieces = std::mem::take(&mut self.pieces);
        match pieces.len() {
            0 => {}
            1 => self.ready.extend(pieces.pop()),
            _ => self
                .ready
                .push_back(ChunkedArray::try_new(pieces, self.dtype.clone())?.into_array()),
        }
        Ok(())
    }
}

/// `piece` with every field of it executed but its lists, and those of them that are not
/// lists of offsets or views executed into views, so a cut of them keeps only its elements.
fn narrow_fields(piece: ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
    let Some(structs) = piece.as_opt::<Struct>() else {
        return Ok(piece);
    };
    let fields = structs
        .iter_unmasked_fields()
        .map(|field| {
            if field.is_canonical() || field.is::<List>() {
                Ok(field.clone())
            } else {
                Ok(field.clone().execute::<Canonical>(ctx)?.into_array())
            }
        })
        .collect::<VortexResult<Vec<_>>>()?;
    let validity = structs.struct_validity();
    let dtype = piece
        .dtype()
        .as_struct_fields_opt()
        .ok_or_else(|| vortex_err!("A struct array without struct fields"))?
        .clone();
    // SAFETY: executing a field keeps its rows and dtype.
    Ok(unsafe { StructArray::new_unchecked(fields, dtype, piece.len(), validity) }.into_array())
}

/// The rows of `piece`, a struct of projected fields, that `filters` keep, or `None` when they
/// keep none.
///
/// The fields the filters read are executed once, for the filters and the rows kept alike.
/// The other fields are filtered lazily, and the lists among them viewed when the filters keep
/// most of their rows, so only the rows kept are decoded.
pub(super) fn filter_projected(
    piece: ArrayRef,
    filters: &[Arc<dyn ProjectedFilter>],
    ctx: &mut ExecutionCtx,
) -> VortexResult<Option<ArrayRef>> {
    let Some(structs) = piece.as_opt::<Struct>() else {
        vortex_bail!("A projected filter over a {} projection", piece.dtype());
    };
    let dtype = piece
        .dtype()
        .as_struct_fields_opt()
        .ok_or_else(|| vortex_err!("A struct array without struct fields"))?
        .clone();
    let mut fields: Vec<ArrayRef> = structs.iter_unmasked_fields().cloned().collect();
    let mut mask = Mask::new_true(piece.len());
    for filter in filters {
        let mut read = Vec::with_capacity(filter.fields().len());
        for name in filter.fields().iter() {
            let index = dtype
                .find(name)
                .ok_or_else(|| vortex_err!("A projected filter reads no field {name}"))?;
            if !fields[index].is_canonical() {
                fields[index] = fields[index]
                    .clone()
                    .execute::<Canonical>(ctx)?
                    .into_array();
            }
            read.push((name.clone(), fields[index].clone()));
        }
        mask = mask & &filter.evaluate(StructArray::from_fields(&read)?.into_array())?;
        if mask.all_false() {
            return Ok(None);
        }
    }
    let validity = structs.struct_validity();
    if mask.all_true() {
        // SAFETY: executing a field keeps its rows and dtype.
        return Ok(Some(
            unsafe { StructArray::new_unchecked(fields, dtype, piece.len(), validity) }
                .into_array(),
        ));
    }
    let fields = fields
        .into_iter()
        .map(|field| match list_views(&field, &mask, ctx)? {
            Some(views) => Ok(views),
            None => field.filter(mask.clone()),
        })
        .collect::<VortexResult<Vec<_>>>()?;
    let validity = validity.filter(&mask)?;
    // SAFETY: every field and the validity keep the same rows, those of the mask.
    Ok(Some(
        unsafe { StructArray::new_unchecked(fields, dtype, mask.true_count(), validity) }
            .into_array(),
    ))
}

/// About the bytes `array` takes once exported: fixed-width values by their width, lists by
/// the elements they hold. Anything else counts its encoded bytes.
fn exported_bytes(array: &ArrayRef, ctx: &mut ExecutionCtx) -> VortexResult<u64> {
    let len = array.len() as u64;
    if let Some(chunked) = array.as_opt::<Chunked>() {
        let mut bytes = 0;
        for chunk in chunked.chunks() {
            bytes += exported_bytes(&chunk, ctx)?;
        }
        return Ok(bytes);
    }
    Ok(match array.dtype() {
        DType::Null => 0,
        DType::Bool(_) => len.div_ceil(8),
        DType::Primitive(ptype, _) => len * ptype.byte_width() as u64,
        DType::Struct(..) => match array.as_opt::<Struct>() {
            Some(structs) => {
                let mut bytes = 0;
                for field in structs.iter_unmasked_fields() {
                    bytes += exported_bytes(field, ctx)?;
                }
                bytes
            }
            None => array.nbytes(),
        },
        DType::List(..) => {
            let offsets = len * 4;
            if let Some(list) = array.as_opt::<List>() {
                let elements = list.elements();
                if elements.is_empty() {
                    return Ok(offsets);
                }
                let held = (list.offset_at(list.len(), ctx)? - list.offset_at(0, ctx)?) as u64;
                let all = exported_bytes(elements, ctx)?;
                offsets + all * held / elements.len() as u64
            } else if let Some(list) = array.as_opt::<ListView>() {
                offsets + exported_bytes(list.elements(), ctx)?
            } else {
                array.nbytes()
            }
        }
        _ => array.nbytes(),
    })
}

#[cfg(test)]
mod tests {
    use vortex_array::IntoArray;
    use vortex_array::VortexSessionExecute;
    use vortex_array::array_session;
    use vortex_array::arrays::ListArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::list::ListArraySlotsExt;
    use vortex_array::assert_arrays_eq;
    use vortex_array::validity::Validity;
    use vortex_buffer::Buffer;
    use vortex_error::VortexResult;

    use super::*;

    #[test]
    fn narrow_pieces_gather_into_one_batch() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        let dtype = PrimitiveArray::from_iter(0u32..1)
            .into_array()
            .dtype()
            .clone();
        let mut batches = Batcher::new(dtype);
        for start in (0u32..9000).step_by(1000) {
            batches.push(
                PrimitiveArray::from_iter(start..start + 1000).into_array(),
                &mut ctx,
            )?;
        }
        // The ninth piece passes 8192 rows and emits all nine.
        let first = batches.pop().expect("8192 rows make a batch");
        assert_eq!(first.len(), 9000);
        assert!(batches.pop().is_none());
        assert_arrays_eq!(first, PrimitiveArray::from_iter(0u32..9000), &mut ctx);
        Ok(())
    }

    /// Lists of 10,000 `u64`s are 80 kB a row, so a 100-row piece is cut into batches of 52
    /// rows, each holding only its own lists' elements.
    #[test]
    fn wide_rows_are_cut_and_trimmed() -> VortexResult<()> {
        let mut ctx = array_session().create_execution_ctx();
        const WIDTH: usize = 10_000;
        let lists = ListArray::try_new(
            Buffer::from_iter(0..100 * WIDTH as u64).into_array(),
            Buffer::from_iter((0..=100).map(|row| u32::try_from(row * WIDTH).unwrap_or(0)))
                .into_array(),
            Validity::NonNullable,
            &mut ctx,
        )?
        .into_array();
        let mut batches = Batcher::new(lists.dtype().clone());
        batches.push(lists.clone(), &mut ctx)?;
        let mut start = 0;
        while let Some(batch) = batches.pop() {
            let rows = batch.len();
            assert_eq!(rows, 52.min(100 - start));
            let list = batch.as_opt::<List>().expect("a cut list stays a list");
            assert_eq!(list.elements().len(), rows * WIDTH);
            assert_arrays_eq!(batch, lists.slice(start..start + rows)?, &mut ctx);
            start += rows;
        }
        assert_eq!(start, 100);
        Ok(())
    }
}
