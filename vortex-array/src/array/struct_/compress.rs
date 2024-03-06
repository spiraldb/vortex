use crate::array::downcast::DowncastArrayBuiltin;
use crate::array::struct_::{StructArray, StructEncoding};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use crate::error::VortexResult;
use itertools::Itertools;
use std::ops::Deref;

impl EncodingCompression for StructEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        (array.encoding().id() == &Self::ID).then_some(&(struct_compressor as Compressor))
    }
}

fn struct_compressor(
    array: &dyn Array,
    like: Option<&dyn Array>,
    ctx: CompressCtx,
) -> VortexResult<ArrayRef> {
    let struct_array = array.as_struct();
    let struct_like = like.map(|like_array| like_array.as_struct());

    let fields = struct_array
        .fields()
        .iter()
        .enumerate()
        .map(|(i, chunk)| {
            let like_chunk = struct_like
                .and_then(|c_like| c_like.fields().get(i))
                .map(Deref::deref);
            ctx.compress(chunk.deref(), like_chunk)
        })
        .try_collect()?;

    Ok(StructArray::new(struct_array.names().clone(), fields).boxed())
}
