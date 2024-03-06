use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::varbinview::{VarBinViewArray, VarBinViewEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use crate::error::VortexResult;
use itertools::Itertools;
use std::ops::Deref;

impl EncodingCompression for VarBinViewEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        (array.encoding().id() == &Self::ID).then_some(&(varbinview_compressor as Compressor))
    }
}

fn varbinview_compressor(
    array: &dyn Array,
    like: Option<&dyn Array>,
    ctx: CompressCtx,
) -> VortexResult<ArrayRef> {
    let varbinview_array = array.as_varbinview();
    let varbinview_like = like.map(|like_array| like_array.as_varbinview());

    Ok(VarBinViewArray::new(
        // TODO(robert): Can we compress views? Not right now
        dyn_clone::clone_box(varbinview_array.views()),
        varbinview_array
            .data()
            .iter()
            .enumerate()
            .map(|(i, d)| {
                ctx.compress(
                    d.as_ref(),
                    varbinview_like
                        .and_then(|v| v.data().get(i))
                        .map(Deref::deref),
                )
            })
            .try_collect()?,
        array.dtype().clone(),
        varbinview_array
            .validity()
            .map(|v| {
                ctx.compress(
                    v.as_ref(),
                    varbinview_like.and_then(|vbvlike| vbvlike.validity()),
                )
            })
            .transpose()?,
    )
    .boxed())
}
