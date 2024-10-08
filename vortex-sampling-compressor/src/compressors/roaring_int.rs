use std::collections::HashSet;

use vortex::encoding::EncodingRef;
use vortex::stats::ArrayStatistics;
use vortex::{Array, ArrayDType, ArrayDef, IntoArray, IntoArrayVariant};
use vortex_error::VortexResult;
use vortex_roaring::{roaring_int_encode, RoaringInt, RoaringIntEncoding};

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct RoaringIntCompressor;

impl EncodingCompressor for RoaringIntCompressor {
    fn id(&self) -> &str {
        RoaringInt::ID.as_ref()
    }

    fn decompression_seconds_per_gb(&self) -> f64 {
        // this is made up
        1.0
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        // Only support non-nullable uint arrays
        if !array.dtype().is_unsigned_int() || array.dtype().is_nullable() {
            return None;
        }

        // Only support sorted unique arrays
        if !array
            .statistics()
            .compute_is_strict_sorted()
            .unwrap_or(false)
        {
            return None;
        }

        if array.statistics().compute_max().unwrap_or(0) > u32::MAX as usize {
            return None;
        }

        Some(self)
    }

    fn compress<'a>(
        &'a self,
        array: &Array,
        _like: Option<CompressionTree<'a>>,
        _ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        Ok(CompressedArray::new(
            roaring_int_encode(array.clone().into_primitive()?)?.into_array(),
            Some(CompressionTree::flat(self)),
        ))
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([&RoaringIntEncoding as EncodingRef])
    }
}
