use vortex_error::VortexResult;

use crate::array::composite::{CompositeArray, CompositeEncoding};
use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::{Array, ArrayRef, OwnedArray};
use crate::compress::{CompressConfig, CompressCtx, EncodingCompression};

impl EncodingCompression for CompositeEncoding {
    fn cost(&self) -> u8 {
        0
    }

    fn can_compress(
        &self,
        array: &dyn OwnedArray,
        config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        (array.encoding().id() == Self::ID).then_some(self)
    }

    fn compress(
        &self,
        array: &dyn OwnedArray,
        like: Option<&dyn OwnedArray>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let composite_array = array.as_composite();
        let composite_like = like.map(|like_array| like_array.as_composite());

        Ok(CompositeArray::new(
            composite_array.id(),
            composite_array.metadata().clone(),
            ctx.compress(
                composite_array.underlying().as_ref(),
                composite_like.map(|c| c.underlying()),
            )?,
        )
        .into_array())
    }
}
