use vortex_error::{vortex_err, VortexResult};

use crate::array::{Array, WithArrayCompute};
use crate::compute::search_sorted::{SearchSorted, SearchSortedSide};
use crate::scalar::Scalar;

pub trait BinarySearchFn {
    fn binary_search(&self, value: &Scalar) -> VortexResult<Result<usize, usize>>;
}

pub fn binary_search<T: Into<Scalar>>(
    array: &dyn Array,
    target: T,
) -> VortexResult<Result<usize, usize>> {
    let scalar = target.into().cast(array.dtype())?;
    array.with_compute(|c| {
        if let Some(binary_search) = c.binary_search() {
            return binary_search.binary_search(&scalar);
        }

        if c.scalar_at().is_some() {
            return Ok(SearchSorted::search_sorted(
                &array,
                &scalar,
                SearchSortedSide::Exact,
            ));
        }

        Err(vortex_err!(
            NotImplemented: "binary_search",
            array.encoding().id().name()
        ))
    })
}
