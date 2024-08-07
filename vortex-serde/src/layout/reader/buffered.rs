use std::collections::VecDeque;

use vortex::array::chunked::ChunkedArray;
use vortex::compute::slice;
use vortex::{Array, ArrayDType, IntoArray};
use vortex_error::VortexResult;

use crate::layout::{Layout, ReadResult};

#[derive(Debug)]
pub struct BufferedLayoutReader {
    layouts: VecDeque<Box<dyn Layout>>,
    arrays: VecDeque<Array>,
    batch_size: usize,
}

impl BufferedLayoutReader {
    pub fn new(layouts: VecDeque<Box<dyn Layout>>, batch_size: usize) -> Self {
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
        self.arrays.iter().map(|arr| arr.len()).sum()
    }

    fn buffer(&mut self) -> VortexResult<Option<ReadResult>> {
        while self.buffered_row_count() < self.batch_size {
            if let Some(mut layout) = self.layouts.pop_front() {
                let read = layout.read()?;
                if let Some(rr) = read {
                    self.layouts.push_front(layout);
                    match rr {
                        g @ ReadResult::GetMsgs(..) => {
                            return Ok(Some(g));
                        }
                        ReadResult::Batch(a) => self.arrays.push_back(a),
                    }
                } else {
                    continue;
                }
            } else {
                return Ok(None);
            }
        }
        Ok(None)
    }

    pub fn read(&mut self) -> VortexResult<Option<ReadResult>> {
        if self.is_empty() {
            return Ok(None);
        }

        if let Some(rr) = self.buffer()? {
            match rr {
                g @ ReadResult::GetMsgs(..) => return Ok(Some(g)),
                ReadResult::Batch(_) => {
                    unreachable!("Batches should be handled inside the buffer call")
                }
            }
        }

        let mut rows_to_read = self.batch_size;
        if self.layouts.is_empty() {
            rows_to_read = usize::min(self.batch_size, self.buffered_row_count());
        }

        let mut result = Vec::new();

        while rows_to_read != 0 {
            match self.arrays.pop_front() {
                None => break,
                Some(array) => {
                    if array.len() > rows_to_read {
                        let taken = slice(&array, 0, rows_to_read)?;
                        let leftover = slice(&array, rows_to_read, array.len())?;
                        self.arrays.push_front(leftover);
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
}
