use arrow_buffer::Buffer;
use vortex::serde::data::ArrayData;
use vortex_error::VortexResult;
use vortex_schema::DType;

use crate::visitor::ArrayVisitor;
use crate::Array;

/// A column batch contains the flattened list of columns.
pub struct ColumnBatch {
    dtype: DType,
    columns: Vec<Array<'static>>,
}

pub struct ColumnBatchBuilder {
    columns: Vec<ArrayData>,
}

impl ArrayVisitor for ColumnBatchBuilder {
    fn visit_column(&mut self, _name: &str, _array: &Array) -> VortexResult<()> {
        todo!()
    }

    fn visit_child(&mut self, _name: &str, _array: &Array) -> VortexResult<()> {
        // If the array is a struct, then pull out each column.
        // But we can't do this in case some non-column child is a struct.
        // Can we ask an array for column(idx)? Seems like a lot of work.
        todo!()
    }

    fn visit_buffer(&mut self, _buffer: &Buffer) -> VortexResult<()> {
        todo!()
    }
}
