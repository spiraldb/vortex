use arrow_array::builder::GenericByteBuilder;
use arrow_array::types::BinaryType;
use fsst::Symbol;
use vortex::array::VarBinArray;
use vortex::arrow::FromArrowArray;
use vortex::validity::ArrayValidity;
use vortex::{ArrayDType, Canonical, IntoCanonical};
use vortex_error::VortexResult;

use crate::FSSTArray;

impl IntoCanonical for FSSTArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        self.with_decompressor(|decompressor| {
            // Note: the maximum amount of decompressed space for an FSST array is 8 * n_elements,
            // as each code can expand into a symbol of 1-8 bytes.
            let max_items = self.len();
            let max_bytes = self.codes().nbytes() * size_of::<Symbol>();

            // Create the target Arrow binary array
            // TODO(aduffy): switch to BinaryView when PR https://github.com/spiraldb/vortex/pull/476 merges
            let mut builder = GenericByteBuilder::<BinaryType>::with_capacity(max_items, max_bytes);

            // TODO(aduffy): add decompression functions that support writing directly into and output buffer.
            let codes_array = self.codes().into_canonical()?.into_varbin()?;

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

            // Force the DTYpe
            let canonical_varbin = VarBinArray::try_from(&vortex::Array::from_arrow(
                &arrow_array,
                self.dtype().is_nullable(),
            ))?;

            let forced_dtype = VarBinArray::try_new(
                canonical_varbin.offsets(),
                canonical_varbin.bytes(),
                self.dtype().clone(),
                canonical_varbin.validity(),
            )?;

            Ok(Canonical::VarBin(forced_dtype))
        })
    }
}
