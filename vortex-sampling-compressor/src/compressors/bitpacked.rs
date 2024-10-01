use std::collections::HashSet;

use vortex::array::PrimitiveArray;
use vortex::encoding::EncodingRef;
use vortex::stats::ArrayStatistics;
use vortex::{Array, ArrayDef, IntoArray};
use vortex_error::{vortex_err, VortexResult};
use vortex_fastlanes::{
    bitpack, bitpack_patches, count_exceptions, find_best_bit_width, BitPacked, BitPackedArray,
    BitPackedEncoding,
};

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct BitPackedCompressor;

impl EncodingCompressor for BitPackedCompressor {
    fn id(&self) -> &str {
        BitPacked::ID.as_ref()
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        // Only support primitive arrays
        let parray = PrimitiveArray::try_from(array).ok()?;

        // Only supports unsigned ints
        if !parray.ptype().is_unsigned_int() {
            return None;
        }

        let bit_width = find_best_bit_width(&parray).ok()?;

        // Check that the bit width is less than the type's bit width
        if bit_width == parray.ptype().bit_width() {
            return None;
        }

        Some(self)
    }

    fn compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        let parray = array.as_primitive();
        let bit_width_freq = parray
            .statistics()
            .compute_bit_width_freq()
            .ok_or_else(|| vortex_err!(ComputeError: "missing bit width frequency"))?;

        let bit_width = find_best_bit_width(&parray)?;
        let num_exceptions = count_exceptions(bit_width, &bit_width_freq);

        if bit_width == parray.ptype().bit_width() {
            // Nothing we can do
            return Ok(CompressedArray::uncompressed(array.clone()));
        }

        let validity = ctx.compress_validity(parray.validity())?;
        let packed = bitpack(&parray, bit_width)?;
        let patches = (num_exceptions > 0)
            .then(|| {
                bitpack_patches(&parray, bit_width, num_exceptions).map(
                    |(patch_indices, patch_values, index_offset)| -> VortexResult<_> {
                        Ok((
                            ctx.auxiliary("patch_indices")
                                .compress(&patch_indices, like.as_ref().and_then(|l| l.child(0)))?,
                            ctx.auxiliary("patch_values")
                                .compress(&patch_values, like.as_ref().and_then(|l| l.child(1)))?,
                            index_offset,
                        ))
                    },
                )
            })
            .flatten()
            .transpose()?;

        Ok(CompressedArray::new(
            BitPackedArray::try_new(
                packed,
                parray.ptype(),
                validity,
                patches
                    .as_ref()
                    .map(|(patch_indices, patch_values, index_offset)| {
                        (
                            patch_indices.array.clone(),
                            patch_values.array.clone(),
                            *index_offset,
                        )
                    }),
                bit_width,
                parray.len(),
            )?
            .into_array(),
            Some(CompressionTree::new(
                self,
                match patches {
                    None => vec![],
                    Some((patch_indices, patch_values, _)) => {
                        vec![patch_indices.path, patch_values.path]
                    }
                },
            )),
        ))
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([&BitPackedEncoding as EncodingRef])
    }
}
