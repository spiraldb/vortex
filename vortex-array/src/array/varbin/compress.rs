use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::varbin::{VarBinArray, VarBinEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, EncodingCompression, Estimate};
use crate::error::VortexResult;

impl EncodingCompression for VarBinEncoding {
    fn cost(&self) -> u8 {
        0 // We simply destructure.
    }

    fn can_compress(&self, array: &dyn Array, _config: &CompressConfig) -> Option<Estimate> {
        (array.encoding().id() == &Self::ID).then_some(Estimate::default())
    }

    fn compress(
        &self,
        array: &dyn Array,
        like: Option<&dyn Array>,
        ctx: CompressCtx,
    ) -> VortexResult<ArrayRef> {
        let vb = array.as_varbin();
        let vblike = like.map(|a| a.as_varbin());
        Ok(VarBinArray::new(
            ctx.auxiliary("offsets")
                .compress(vb.offsets(), vblike.map(|l| l.offsets()))?,
            dyn_clone::clone_box(vb.bytes()),
            vb.dtype().clone(),
            vb.validity()
                .map(|v| {
                    ctx.auxiliary("validity")
                        .compress(v, vblike.and_then(|l| l.validity()))
                })
                .transpose()?,
        )
        .boxed())
    }
}
