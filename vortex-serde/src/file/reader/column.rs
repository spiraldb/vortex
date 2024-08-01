use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use vortex::array::chunked::ChunkedArray;
use vortex::compute::slice;
use vortex::{Array, IntoArray};
use vortex_dtype::DType;
use vortex_error::{VortexError, VortexResult};

use crate::file::layouts::Layout;
use crate::io::VortexReadAt;
use crate::{ArrayBufferReader, ReadResult};

pub(super) struct ColumnReader {
    #[allow(dead_code)]
    name: Arc<str>,
    dtype: DType,
    layouts: VecDeque<Layout>,
    arrays: VecDeque<Array>,
}

impl ColumnReader {
    pub fn new(name: Arc<str>, dtype: DType, layouts: VecDeque<Layout>) -> Self {
        Self {
            name,
            dtype,
            layouts,
            arrays: Default::default(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.layouts.is_empty() && self.arrays.is_empty()
    }

    pub fn buffered_row_count(&self) -> usize {
        self.arrays.iter().map(|arr| arr.len()).sum()
    }

    pub async fn load<R: VortexReadAt>(
        &mut self,
        reader: &mut R,
        batch_size: usize,
        context: Arc<vortex::Context>,
    ) -> VortexResult<()> {
        loop {
            if self.buffered_row_count() >= batch_size {
                return Ok(());
            }

            if let Some(layout) = self.layouts.pop_front() {
                let byte_range = layout.as_flat().unwrap().range;
                let mut buffer = BytesMut::with_capacity(byte_range.len());
                unsafe { buffer.set_len(byte_range.len()) };

                let mut buff = reader
                    .read_at_into(byte_range.begin, buffer)
                    .await
                    .map_err(VortexError::from)
                    .unwrap()
                    .freeze();

                let mut array_reader = ArrayBufferReader::new();
                let mut read_buf = Bytes::new();
                while let Some(ReadResult::ReadMore(u)) = array_reader.read(read_buf.clone())? {
                    read_buf = buff.split_to(u);
                }

                let array = array_reader
                    .into_array(context.clone(), self.dtype.clone())
                    .unwrap();

                self.arrays.push_back(array);
            } else {
                break Ok(());
            }
        }
    }

    pub fn read_rows(&mut self, mut rows_needed: usize) -> VortexResult<Option<Array>> {
        if self.buffered_row_count() == 0 && self.layouts.is_empty() {
            return Ok(None);
        }

        if self.layouts.is_empty() {
            rows_needed = usize::min(rows_needed, self.buffered_row_count());
        }

        let mut result = Vec::default();

        loop {
            if rows_needed == 0 {
                break;
            }

            match self.arrays.pop_front() {
                None => break,
                Some(array) => match array.len().cmp(&rows_needed) {
                    Ordering::Greater => {
                        let taken = slice(&array, 0, rows_needed)?;
                        let leftover = slice(&array, rows_needed, array.len())?;
                        self.arrays.push_front(leftover);
                        rows_needed -= taken.len();
                        result.push(taken);
                    }
                    Ordering::Equal | Ordering::Less => {
                        rows_needed -= array.len();
                        result.push(array);
                    }
                },
            }
        }

        match result.len() {
            0 => Ok(None),
            1 => Ok(Some(result.remove(0))),
            _ => Ok(Some(
                ChunkedArray::try_new(result, self.dtype.clone())?.into_array(),
            )),
        }
    }
}
