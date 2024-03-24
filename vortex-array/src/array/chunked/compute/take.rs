use crate::array::chunked::ChunkedArray;
use crate::array::{Array, ArrayRef};
use crate::compute::cast::cast;
use crate::compute::flatten::flatten_primitive;
use crate::compute::search_sorted::{SearchSorted, SearchSortedSide};
use crate::compute::take::TakeFn;
use crate::error::VortexResult;
use crate::ptype::PType;
use hashbrown::HashMap;

impl TakeFn for ChunkedArray {
    fn take(&self, indices: &dyn Array) -> VortexResult<ArrayRef> {
        let indices = flatten_primitive(cast(indices, PType::U64.into())?.as_ref())?;

        let mut indices_by_chunk = HashMap::new();
        indices.typed_data::<u64>().iter().for_each(|i| {
            let pos = self.chunk_ends().search_sorted(i, SearchSortedSide::Right);
            if !indices_by_chunk.contains_key(&pos) {
                indices_by_chunk.insert(pos, Vec::new());
            }
            indices_by_chunk.get_mut(&pos).unwrap().push(i);
        });

        println!("Indices by chunk: {:?}", indices_by_chunk);

        todo!()
    }
}

#[cfg(test)]
mod test {
    use crate::array::chunked::ChunkedArray;
    use crate::array::IntoArray;
    use crate::compute::take::take;

    #[test]
    fn test_take() {
        let a = vec![1, 2, 3].into_array();
        let arr = ChunkedArray::new(vec![a.clone(), a.clone(), a.clone()], a.dtype().clone());
        let indices = vec![0, 0, 6, 4].into_array();

        let result = take(&arr, &indices).unwrap();
        println!("TAKE {:?}", result);
    }
}
