use itertools::Itertools;

use crate::array::ArrayRef;
use crate::error::{VortexError, VortexResult};

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
    first
        .as_contiguous()
        .map(|f| f.as_contiguous(arrays))
        .unwrap_or_else(|| {
            Err(VortexError::NotImplemented(
                "as_contiguous",
                first.encoding().id(),
            ))
        })
}
