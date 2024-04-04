use vortex_error::VortexResult;

use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::sparse::{SparseArray, SparseEncoding};
use crate::array::{Array, ArrayRef, OwnedArray};
use crate::compress::{CompressConfig, CompressCtx, EncodingCompression};

impl EncodingCompression for SparseEncoding {
    fn cost(&self) -> u8 {
        0
    }

    fn can_compress(
        &self,
        array: &dyn OwnedArray,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        (array.encoding().id() == Self::ID).then_some(self)
    }

    fn compress(
        &self,
        array: &dyn OwnedArray,
        like: Option<&dyn OwnedArray>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let sparse_array = array.as_sparse();
        let sparse_like = like.map(|la| la.as_sparse());
        Ok(SparseArray::new(
            ctx.auxiliary("indices").compress(
                sparse_array.indices().as_ref(),
                sparse_like.map(|sa| sa.indices()),
            )?,
            ctx.named("values").compress(
                sparse_array.values().as_ref(),
                sparse_like.map(|sa| sa.values()),
            )?,
            sparse_array.len(),
            sparse_array.fill_value.clone(),
        )
        .into_array())
    }
}
