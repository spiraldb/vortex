use arrow_array::builder::BinaryViewBuilder;
use vortex::array::VarBinViewArray;
use vortex::arrow::FromArrowArray;
use vortex::validity::ArrayValidity;
use vortex::{ArrayDType, Canonical, IntoCanonical};
use vortex_error::VortexResult;

use crate::FSSTArray;

impl IntoCanonical for FSSTArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        self.with_decompressor(|decompressor| {
            let mut builder = BinaryViewBuilder::with_capacity(self.len());

            // TODO(aduffy): add decompression functions that support writing directly into and output buffer.
            let codes_array = self.codes().into_canonical()?.into_varbinview()?;

            // TODO(aduffy): make this loop faster.
            for idx in 0..self.len() {
                if !codes_array.is_valid(idx) {
                    builder.append_null()
                } else {
                    let compressed = codes_array.bytes_at(idx)?;
                    let value = decompressor.decompress(compressed.as_slice());
                    builder.append_value(value)
                }
            }

            let arrow_array = builder.finish();

            // Force the DType
            let canonical_varbin = VarBinViewArray::try_from(&vortex::Array::from_arrow(
                &arrow_array,
                self.dtype().is_nullable(),
            ))?;

            let forced_dtype = VarBinViewArray::try_new(
                canonical_varbin.views(),
                canonical_varbin.buffers().collect(),
                self.dtype().clone(),
                canonical_varbin.validity(),
            )?;

            Ok(Canonical::VarBinView(forced_dtype))
        })
    }
}
