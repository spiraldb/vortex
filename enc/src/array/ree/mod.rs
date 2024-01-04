use arrow2::scalar::Scalar;

use crate::array::{impl_array, Array, ArrowIterator};
use crate::types::DType;

#[derive(Clone)]
pub struct REEArray {
    ends: Box<dyn Array>,
    values: Box<dyn Array>,
    length: usize,
}

pub const KIND: &str = "enc.ree";

impl REEArray {
    pub fn new(ends: Box<dyn Array>, values: Box<dyn Array>, length: usize) -> Self {
        Self {
            ends,
            values,
            length,
        }
    }
}

impl Array for REEArray {
    impl_array!();

    fn len(&self) -> usize {
        todo!()
    }

    fn dtype(&self) -> &DType {
        self.values.dtype()
    }

    fn kind(&self) -> &str {
        KIND
    }

    fn scalar_at(&self, _index: usize) -> Box<dyn Scalar> {
        todo!()
        // Find the index in the sorted ends array that represents the run containing this index.
        // let run = array::compute::searchsorted(self.ends.as_ref(), index)
        // self.values.scalar_at(run)
    }

    fn iter_arrow(&self) -> Box<ArrowIterator> {
        todo!();
        // let mut last_offset: u64 = 0;
        // let ends_arrays = self
        //     .ends
        //     .iter_arrow()
        //     .map(|b| {
        //         arrow2::compute::cast::cast(b.as_ref(), &DataType::UInt64, CastOptions::default())
        //             .unwrap()
        //     })
        //     .flat_map(|v| {
        //         let primitive_array = v.as_any().downcast_ref::<PrimitiveArray<u64>>().unwrap();
        //         primitive_array.values_iter()
        //     })
        //     .map(|offset| {
        //         let run_length = offset - last_offset;
        //         last_offset = *offset;
        //         run_length
        //     });
        // let values_array = self.values.iter_arrow()
    }
}
