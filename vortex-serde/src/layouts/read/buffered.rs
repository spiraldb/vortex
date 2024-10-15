use std::collections::VecDeque;
use std::mem;

use vortex::array::ChunkedArray;
use vortex::compute::slice;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_error::{vortex_bail, VortexResult};

use crate::layouts::read::selection::{RowRange, RowSelector};
use crate::layouts::read::{LayoutReader, ReadResult};
use crate::layouts::{Messages, RangeResult};

pub type RangedArray = (RowRange, Array);

#[derive(Debug)]
pub struct BufferedReader {
    layouts: VecDeque<RangedLayoutReader>,
    arrays: VecDeque<RangedArray>,
    batch_size: usize,
}

impl BufferedReader {
    pub fn new(layouts: VecDeque<RangedLayoutReader>, batch_size: usize) -> Self {
        Self {
            layouts,
            arrays: Default::default(),
            batch_size,
        }
    }

    fn is_empty(&self) -> bool {
        self.layouts.is_empty() && self.arrays.is_empty()
    }

    fn buffered_row_count(&self) -> usize {
        self.arrays.iter().map(|(_, a)| a.len()).sum()
    }

    fn buffer(&mut self, selection: RowSelector) -> VortexResult<Option<ReadResult>> {
        while self.buffered_row_count() < self.batch_size {
            if let Some((row_r, mut layout)) = self.layouts.pop_front() {
                // This selection doesn't know about rows in this chunk, we should put it back and wait for another request with different range
                if selection.length() <= row_r.begin {
                    self.layouts.push_front((row_r, layout));
                    return Ok(None);
                }
                let layout_selection = selection
                    .intersect(&RowSelector::new(vec![row_r], row_r.end))
                    .offset(row_r.begin);
                if let Some(rr) = layout.read_next(layout_selection)? {
                    self.layouts.push_front((row_r, layout));
                    match rr {
                        read_more @ ReadResult::ReadMore(..) => {
                            return Ok(Some(read_more));
                        }
                        ReadResult::Batch(a) => self.arrays.push_back((row_r, a)),
                    }
                } else {
                    if row_r.end > selection.length() && row_r.begin < selection.length() {
                        self.layouts.push_front((row_r, layout));
                        return Ok(None);
                    }
                    continue;
                }
            } else {
                return Ok(None);
            }
        }
        Ok(None)
    }

    pub fn read_next_batch(&mut self, selection: RowSelector) -> VortexResult<Option<ReadResult>> {
        if self.is_empty() {
            return Ok(None);
        }

        if let Some(rr) = self.buffer(selection)? {
            match rr {
                read_more @ ReadResult::ReadMore(..) => return Ok(Some(read_more)),
                ReadResult::Batch(_) => {
                    unreachable!("Batches should be handled inside the buffer call")
                }
            }
        }

        let mut rows_to_read = if self.layouts.is_empty() {
            usize::min(self.batch_size, self.buffered_row_count())
        } else {
            self.batch_size
        };

        let mut result = Vec::new();

        while rows_to_read != 0 {
            match self.arrays.pop_front() {
                None => break,
                Some((row_r, array)) => {
                    if array.len() > rows_to_read {
                        let taken = slice(&array, 0, rows_to_read)?;
                        let leftover = slice(&array, rows_to_read, array.len())?;
                        self.arrays.push_front((row_r, leftover));
                        rows_to_read -= taken.len();
                        result.push(taken);
                    } else {
                        rows_to_read -= array.len();
                        result.push(array);
                    }
                }
            }
        }

        match result.len() {
            0 | 1 => Ok(result.pop().map(ReadResult::Batch)),
            _ => {
                let dtype = result[0].dtype().clone();
                Ok(Some(ReadResult::Batch(
                    ChunkedArray::try_new(result, dtype)?.into_array(),
                )))
            }
        }
    }

    pub fn advance(&mut self, up_to_row: usize) -> VortexResult<Messages> {
        if self
            .arrays
            .front()
            .map(|(rr, _)| up_to_row < rr.begin)
            .or_else(|| self.layouts.front().map(|(rr, _)| up_to_row < rr.begin))
            .unwrap_or(true)
        {
            vortex_bail!("Can't advance backwards")
        }

        let mut new_arrays = mem::take(&mut self.arrays)
            .into_iter()
            .skip_while(|(rr, _)| rr.end < up_to_row)
            .collect::<VecDeque<_>>();
        if let Some((rr, carr)) = new_arrays.pop_front() {
            let slice_end = carr.len();
            let sliced = slice(
                carr,
                slice_end - (rr.len() - (up_to_row - rr.begin)),
                slice_end,
            )?;

            new_arrays.push_front((rr, sliced));
        };
        self.arrays = new_arrays;

        let mut new_layouts = mem::take(&mut self.layouts)
            .into_iter()
            .skip_while(|(rr, _)| rr.end < up_to_row)
            .collect::<VecDeque<_>>();
        let res = if let Some((rr, mut l)) = new_layouts.pop_front() {
            let advance = l.advance(up_to_row - rr.begin);
            new_layouts.push_front((rr, l));
            advance
        } else {
            Ok(vec![])
        };
        self.layouts = new_layouts;
        res
    }
}

pub type RangedLayoutReader = (RowRange, Box<dyn LayoutReader>);

#[derive(Debug)]
pub struct ChunkedFilter {
    layouts: VecDeque<RangedLayoutReader>,
}

impl ChunkedFilter {
    pub fn new(layouts: VecDeque<RangedLayoutReader>) -> Self {
        Self { layouts }
    }

    pub fn filter_more(&mut self) -> VortexResult<Option<RangeResult>> {
        while let Some((range, mut head)) = self.layouts.pop_front() {
            if let Some(rr) = head.read_range()? {
                self.layouts.push_front((range, head));
                return Ok(Some(match rr {
                    read_more @ RangeResult::ReadMore(_) => read_more,
                    RangeResult::Range(r) => {
                        let length = r.length();
                        RangeResult::Range(RowSelector::from_ranges(
                            r.into_iter().map(|rr| {
                                RowRange::new(rr.begin + range.begin, rr.end + range.begin)
                            }),
                            length + range.begin,
                        ))
                    }
                }));
            } else {
                continue;
            }
        }
        Ok(None)
    }

    pub fn advance(&mut self, up_to_row: usize) -> VortexResult<Messages> {
        let mut new_layouts = mem::take(&mut self.layouts)
            .into_iter()
            .skip_while(|(rr, _)| rr.end < up_to_row)
            .collect::<VecDeque<_>>();
        let res = if let Some((rr, mut l)) = new_layouts.pop_front() {
            let advance = l.advance(up_to_row - rr.begin);
            new_layouts.push_front((rr, l));
            advance
        } else {
            Ok(vec![])
        };
        self.layouts = new_layouts;
        res
    }
}
