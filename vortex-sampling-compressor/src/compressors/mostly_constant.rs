use std::collections::HashSet;

use vortex::array::{Constant, ConstantArray, ConstantEncoding};
use vortex::compute::unary::scalar_at;
use vortex::encoding::EncodingRef;
use vortex::stats::ArrayStatistics;
use vortex::{Array, ArrayDef, IntoArray};
use vortex_error::VortexResult;

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct MostlyConstantCompressor;

impl EncodingCompressor for MostlyConstantCompressor {
    fn id(&self) -> &str {
        "vortex.mostly_constant"
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        (!array.is_empty() && !array.statistics().compute_is_constant().unwrap_or(false))
            .then_some(self as &dyn EncodingCompressor)
    }

    fn compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        _ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        if let Some(like) = like {
            if like.compressor.id() == ConstantCompressor.id() {
                let like = like.array.as_constant()?;
                let array = array.as_constant()?;
                if array.value() == like.value() {
                    return Ok(CompressedArray::new(
                        ConstantArray::new(array.value(), array.len()).into_array(),
                        Some(CompressionTree::flat(self)),
                    ));
                }
            }
        }
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([&ConstantEncoding as EncodingRef])
    }
}
