use std::collections::VecDeque;
use std::sync::Arc;

use vortex::array::struct_::StructArray;
use vortex::{Array, Context, IntoArray};
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::file::layouts::Layout;
use crate::file::reader::column::ColumnReader;
use crate::io::VortexReadAt;

pub(super) struct BatchReader<R> {
    readers: Vec<(Arc<str>, ColumnReader)>,
    reader: R,
}

impl<R: VortexReadAt> BatchReader<R> {
    pub fn new(
        reader: R,
        column_info: impl Iterator<Item = (Arc<str>, DType, VecDeque<Layout>)>,
    ) -> Self {
        Self {
            reader,
            readers: column_info
                .map(|(name, dtype, layouts)| {
                    (name.clone(), ColumnReader::new(dtype.clone(), layouts))
                })
                .collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.readers().all(|c| c.is_empty())
    }

    fn readers(&self) -> impl Iterator<Item = &ColumnReader> {
        self.readers.iter().map(|(_, r)| r)
    }

    pub async fn load(&mut self, batch_size: usize, context: Arc<Context>) -> VortexResult<()> {
        for (_, column_reader) in self.readers.iter_mut() {
            column_reader
                .load(&mut self.reader, batch_size, context.clone())
                .await?;
        }

        Ok(())
    }

    pub fn next(&mut self, batch_size: usize) -> Option<VortexResult<Array>> {
        let mut final_columns = vec![];

        for (col_name, column_reader) in self.readers.iter_mut() {
            match column_reader.read_rows(batch_size) {
                Ok(Some(array)) => final_columns.push((col_name.clone(), array)),
                Ok(None) => {
                    debug_assert!(
                        final_columns.is_empty(),
                        "All columns should be empty together"
                    );
                    return None;
                }
                Err(e) => return Some(Err(e)),
            }
        }

        Some(VortexResult::Ok(
            StructArray::from_fields(final_columns.as_slice()).into_array(),
        ))
    }
}
