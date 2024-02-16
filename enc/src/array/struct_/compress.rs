use crate::array::struct_::{StructArray, StructEncoding, STRUCT_ENCODING};
use crate::array::{Array, ArrayRef};
use crate::compress::{CompressConfig, CompressCtx, Compressor, EncodingCompression};
use rayon::iter::IndexedParallelIterator;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::ParallelIterator;

impl EncodingCompression for StructEncoding {
    fn compressor(
        &self,
        array: &dyn Array,
        _config: &CompressConfig,
    ) -> Option<&'static Compressor> {
        if array.encoding().id() == &STRUCT_ENCODING {
            Some(&(struct_compressor as Compressor))
        } else {
            None
        }
    }
}

fn struct_compressor(array: &dyn Array, like: Option<&dyn Array>, ctx: CompressCtx) -> ArrayRef {
    let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
    let struct_like =
        like.map(|like_array| like_array.as_any().downcast_ref::<StructArray>().unwrap());

    let fields = struct_like
        .map(|s_like| {
            struct_array
                .fields()
                .par_iter()
                .zip_eq(s_like.fields())
                .map(|(field, field_like)| ctx.compress(field.as_ref(), Some(field_like.as_ref())))
                .collect()
        })
        .unwrap_or_else(|| {
            struct_array
                .fields()
                .par_iter()
                .map(|field| ctx.compress(field.as_ref(), None))
                .collect()
        });

    StructArray::new(struct_array.names().clone(), fields).boxed()
}
