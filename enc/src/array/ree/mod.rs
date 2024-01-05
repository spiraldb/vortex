use crate::array::{impl_array, Array, ArrowIterator};
use crate::arrow::compat;
use crate::scalar::Scalar;
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
        self.length
    }

    fn dtype(&self) -> &DType {
        self.values.dtype()
    }

    fn kind(&self) -> &str {
        KIND
    }

    fn scalar_at(&self, index: usize) -> Box<dyn Scalar> {
        use polars_core::prelude::*;
        use polars_ops::prelude::*;

        let ends_chunks: Vec<ArrayRef> = self
            .ends
            .iter_arrow()
            .map(|chunk| compat::into_polars(chunk.as_ref()))
            .collect();
        let ends: Series = ("ends", ends_chunks).try_into().unwrap();

        // TODO(ngates): cast the index into the same scalar type as the "ends" array
        let search: Series = [index as i32].iter().collect();

        let maybe_run = search_sorted(&ends, &search, SearchSortedSide::Right, false)
            .unwrap()
            .get(0);

        match maybe_run {
            Some(run) => {
                let run = run as usize;
                self.values.scalar_at(run)
            }
            None => panic!("TODO: return result"),
        }
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

#[cfg(test)]
mod test {
    use crate::array::primitive::PrimitiveArray;
    use crate::types::IntWidth;

    use super::*;

    #[test]
    fn new() {
        let arr = REEArray::new(
            PrimitiveArray::from_vec(vec![2, 5, 10]).boxed(),
            PrimitiveArray::from_vec(vec![1, 2, 3]).boxed(),
            10,
        );
        assert_eq!(arr.len(), 10);
        assert_eq!(arr.dtype(), &DType::Int(IntWidth::_32));

        // 0, 1 => 1
        // 2, 3, 4 => 2
        // 5, 6, 7, 8, 9 => 3
        assert_eq!(arr.scalar_at(0).try_into(), Ok(1));
        assert_eq!(arr.scalar_at(2).try_into(), Ok(2));
        assert_eq!(arr.scalar_at(5).try_into(), Ok(3));
        assert_eq!(arr.scalar_at(9).try_into(), Ok(3));
    }
}
