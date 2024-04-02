use itertools::Itertools;

use vortex_error::{VortexError, VortexResult};

use crate::array::{Array, ArrayRef, WithArrayCompute};

pub trait AsContiguousFn {
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef>;
}

pub fn as_contiguous(arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
    if arrays.is_empty() {
        return Err(VortexError::ComputeError("No arrays to concatenate".into()));
    }
    if !arrays.iter().map(|chunk| chunk.encoding().id()).all_equal() {
        return Err(VortexError::ComputeError(
            "Chunks have differing encodings".into(),
        ));
    }

    let first = arrays.first().unwrap();
    first.with_compute(|c| {
        c.as_contiguous()
            .map(|f| f.as_contiguous(arrays))
            .unwrap_or_else(|| {
                Err(VortexError::NotImplemented(
                    "as_contiguous",
                    first.encoding().id().name(),
                ))
            })
    })
}
