use vortex::array::{PrimitiveArray, VarBinArray};
use vortex::validity::Validity;
use vortex::{ArrayDType, Canonical, IntoArray, IntoCanonical};
use vortex_error::VortexResult;

use crate::FSSTArray;

impl IntoCanonical for FSSTArray {
    fn into_canonical(self) -> VortexResult<Canonical> {
        self.with_decompressor(|decompressor| {
            let compressed_bytes = VarBinArray::try_from(self.codes())?.bytes().as_primitive();

            // Bulk-decompress the entire array.
            let uncompressed_bytes =
                decompressor.decompress(compressed_bytes.maybe_null_slice::<u8>());

            // Convert the uncompressed_lengths into offsets for building a new VarBinArray.
            let mut offsets: Vec<i32> = Vec::with_capacity(self.len() + 1);
            let mut offset = 0;
            offsets.push(offset);

            let uncompressed_lens_array = self
                .uncompressed_lengths()
                .into_canonical()?
                .into_primitive()?;
            let uncompressed_lens_slice = uncompressed_lens_array.maybe_null_slice::<i32>();

            for len in uncompressed_lens_slice.iter() {
                offset += len;
                offsets.push(offset);
            }

            let offsets_array =
                PrimitiveArray::from_vec(offsets, Validity::NonNullable).into_array();
            let uncompressed_bytes_array =
                PrimitiveArray::from_vec(uncompressed_bytes, Validity::NonNullable).into_array();

            Ok(Canonical::VarBin(VarBinArray::try_new(
                offsets_array,
                uncompressed_bytes_array,
                self.dtype().clone(),
                self.validity(),
            )?))
        })
    }
}
