use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use vortex::array::struct_::StructArray;
use vortex::{Array, IntoArray};
use vortex_dtype::DType;
use vortex_error::VortexResult;

use super::schema::Schema;
use crate::file::layouts::Layout;
use crate::file::reader::column::ColumnReader;
use crate::io::VortexReadAt;

pub(super) struct BatchReader<R> {
    readers: HashMap<Arc<str>, ColumnReader>,
    schema: Schema,
    reader: R,
}

impl<R: VortexReadAt> BatchReader<R> {
    pub fn new(reader: R, schema: Schema) -> Self {
        Self {
            reader,
            schema,
            readers: Default::default(),
        }
    }

    pub fn add_column(&mut self, name: Arc<str>, dtype: DType, layouts: VecDeque<Layout>) {
        self.readers
            .insert(name.clone(), ColumnReader::new(name, dtype, layouts));
    }

    pub fn is_empty(&self) -> bool {
        self.readers.values().all(|c| c.is_empty())
    }

    pub async fn load(
        &mut self,
        batch_size: usize,
        context: Arc<vortex::Context>,
    ) -> VortexResult<()> {
        for column_reader in self.readers.values_mut() {
            column_reader
                .load(&mut self.reader, batch_size, context.clone())
                .await?;
        }

        Ok(())
    }

    pub fn next(&mut self, batch_size: usize) -> Option<VortexResult<Array>> {
        let mut final_columns = vec![];

        for col_name in self.schema.fields().iter() {
            let column_reader = self.readers.get_mut(col_name).unwrap();

            match column_reader.read_rows(batch_size) {
                Some(Ok(array)) => final_columns.push((col_name.clone(), array)),
                Some(Err(e)) => return Some(Err(e)),
                None => return None,
            }
        }

        Some(VortexResult::Ok(
            StructArray::from_fields(final_columns.as_slice()).into_array(),
        ))
    }
}
