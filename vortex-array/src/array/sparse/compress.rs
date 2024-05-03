use vortex_error::VortexResult;

use crate::array::sparse::{Sparse, SparseArray, SparseEncoding};
use crate::compress::{CompressConfig, Compressor, EncodingCompression};
use crate::{Array, ArrayDef, ArrayTrait, IntoArray, OwnedArray};

impl EncodingCompression for SparseEncoding {
    fn cost(&self) -> u8 {
        0
    }

    fn can_compress(
        &self,
        array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        (array.encoding().id() == Sparse::ID).then_some(self)
    }

    fn compress(
        &self,
        array: &Array,
        like: Option<&Array>,
        ctx: Compressor,
    ) -> VortexResult<OwnedArray> {
        let sparse_array = SparseArray::try_from(array)?;
        let sparse_like = like.map(|la| SparseArray::try_from(la).unwrap());
        Ok(SparseArray::new(
            ctx.auxiliary("indices").compress(
                &sparse_array.indices(),
                sparse_like.as_ref().map(|sa| sa.indices()).as_ref(),
            )?,
            ctx.named("values").compress(
                &sparse_array.values(),
                sparse_like.as_ref().map(|sa| sa.values()).as_ref(),
            )?,
            sparse_array.len(),
            sparse_array.fill_value().clone(),
        )
        .into_array())
    }
}
