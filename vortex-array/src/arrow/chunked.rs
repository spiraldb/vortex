use arrow_array::{Array, ArrayRef};
use arrow_buffer::NullBuffer;
use arrow_data::ArrayData;
use arrow_schema::DataType;
use std::any::Any;
use std::fmt::Debug;

#[derive(Debug)]
pub struct ChunkedArray {
    data_type: DataType,
    chunks: Vec<ArrayRef>,
}

impl ChunkedArray {
    pub fn new(chunks: Vec<ArrayRef>) -> Self {
        chunks.iter().for_each(|a| {
            assert_eq!(a.data_type(), chunks[0].data_type());
        });
        Self {
            data_type: chunks[0].data_type().clone(),
            chunks,
        }
    }

    pub fn chunks(&self) -> &[ArrayRef] {
        &self.chunks
    }
}

impl Array for ChunkedArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        unimplemented!()
    }

    fn into_data(self) -> ArrayData {
        unimplemented!()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, _offset: usize, _length: usize) -> ArrayRef {
        unimplemented!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn offset(&self) -> usize {
        todo!()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        todo!()
    }

    fn get_buffer_memory_size(&self) -> usize {
        todo!()
    }

    fn get_array_memory_size(&self) -> usize {
        todo!()
    }
}
