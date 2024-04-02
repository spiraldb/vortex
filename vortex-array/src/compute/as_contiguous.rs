use itertools::Itertools;

use vortex_error::{vortex_bail, vortex_err, VortexResult};

use crate::array::ArrayRef;

pub trait AsContiguousFn {
    fn as_contiguous(&self, arrays: &[ArrayRef]) -> VortexResult<ArrayRef>;
}

pub fn as_contiguous(arrays: &[ArrayRef]) -> VortexResult<ArrayRef> {
    if arrays.is_empty() {
        vortex_bail!(ComputeError: "No arrays to concatenate");
    }
    if !arrays.iter().map(|chunk| chunk.encoding().id()).all_equal() {
        vortex_bail!(ComputeError:
            "Chunks have differing encodings",
        );
    }

    let first = arrays.first().unwrap();
    first
        .as_contiguous()
        .map(|f| f.as_contiguous(arrays))
        .unwrap_or_else(|| {
            Err(vortex_err!(
                NotImplemented: "as_contiguous",
                first.encoding().id().name()
            ))
        })
}
