use itertools::Itertools;
use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::{Array, ArrayDType};

/// Trait for typed array variants which support the process of unfurling to somewhere else.
pub trait AsContiguousFn {
    fn as_contiguous(&self, arrays: &[Array]) -> VortexResult<Array>;
}

/// Macro that
#[macro_export]
macro_rules! impl_default_as_contiguous_fn {
    ($typ:ty) => {
        impl $crate::compute::as_contiguous::AsContiguousFn for $typ {
            fn as_contiguous(&self, arrays: &[$crate::Array]) -> vortex_error::VortexResult<$crate::Array> {
                let dtype = $crate::ArrayDType::dtype(self).clone();
                if !arrays
                    .iter()
                    .map(|array| $crate::ArrayDType::dtype(array).clone())
                    .all(|dty| dty == dtype)
                {
                    vortex_error::vortex_bail!(ComputeError: "mismatched dtypes in call to as_contiguous");
                }

                let mut chunks = Vec::with_capacity(arrays.iter().map(|array| array.len()).sum());
                for array in arrays {
                    chunks.push(array.clone().flatten()?.into_array());
                }

                let chunked_array = $crate::array::chunked::ChunkedArray::try_new(chunks, dtype)?.into_array();
                $crate::compute::as_contiguous::as_contiguous(&[chunked_array])
            }
        }
    };
}

pub fn as_contiguous(arrays: &[Array]) -> VortexResult<Array> {
    // Simple case: slice with 1 element
    if arrays.len() == 1 {
        return Ok(arrays[0].clone());
    }

    if arrays.is_empty() {
        vortex_bail!(ComputeError: "No arrays to concatenate");
    }
    if !arrays.iter().map(|chunk| chunk.encoding().id()).all_equal() {
        println!(
            "ENCODINGS: {:?}",
            arrays
                .iter()
                .map(|chunk| chunk.encoding().id())
                .collect_vec()
        );
        vortex_bail!(ComputeError: "Chunks have differing encodings");
    }
    if !arrays.iter().map(|chunk| chunk.dtype()).all_equal() {
        vortex_bail!(ComputeError:
            "Chunks have differing dtypes",
        );
    }

    let first = arrays.first().unwrap();
    first.with_dyn(|a| {
        a.as_contiguous()
            .map(|f| f.as_contiguous(arrays))
            .unwrap_or_else(|| {
                Err(vortex_err!(
                    NotImplemented: "as_contiguous",
                    first.encoding().id()
                ))
            })
    })
}
