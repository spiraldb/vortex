use std::collections::HashSet;

use vortex::array::PrimitiveArray;
use vortex::encoding::EncodingRef;
use vortex::stats::{ArrayStatistics, Stat};
use vortex::{Array, ArrayDef, IntoArray};
use vortex_error::VortexResult;
use vortex_zigzag::{zigzag_encode, ZigZag, ZigZagArray, ZigZagEncoding};

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct ZigZagCompressor;

impl EncodingCompressor for ZigZagCompressor {
    fn id(&self) -> &str {
        ZigZag::ID.as_ref()
    }

    fn cost(&self) -> u8 {
        0
    }

    fn decompression_seconds_per_gb(&self) -> f64 {
        0.01
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        // Only support primitive arrays
        let parray = PrimitiveArray::try_from(array).ok()?;

        // Only supports signed integers
        if !parray.ptype().is_signed_int() {
            return None;
        }

        // Only compress if the array has negative values
        // TODO(ngates): also check that Stat::Max is less than half the max value of the type
        parray
            .statistics()
            .compute_as_cast::<i64>(Stat::Min)
            .filter(|&min| min < 0)
            .map(|_| self as &dyn EncodingCompressor)
    }

    fn compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        let encoded = zigzag_encode(PrimitiveArray::try_from(array)?)?;
        let compressed =
            ctx.compress(&encoded.encoded(), like.as_ref().and_then(|l| l.child(0)))?;
        Ok(CompressedArray::new(
            ZigZagArray::try_new(compressed.array)?.into_array(),
            Some(CompressionTree::new(self, vec![compressed.path])),
        ))
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([&ZigZagEncoding as EncodingRef])
    }
}
