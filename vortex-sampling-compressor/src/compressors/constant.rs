use vortex::array::{Constant, ConstantArray};
use vortex::compute::unary::scalar_at;
use vortex::stats::ArrayStatistics;
use vortex::{Array, ArrayDef, IntoArray};
use vortex_error::VortexResult;

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct ConstantCompressor;

impl EncodingCompressor for ConstantCompressor {
    fn id(&self) -> &str {
        Constant::ID.as_ref()
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        (!array.is_empty() && array.statistics().compute_is_constant().unwrap_or(false))
            .then_some(self as &dyn EncodingCompressor)
    }

    fn compress<'a>(
        &'a self,
        array: &Array,
        _like: Option<CompressionTree<'a>>,
        _ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        Ok(CompressedArray::new(
            ConstantArray::new(scalar_at(array, 0)?, array.len()).into_array(),
            Some(CompressionTree::flat(self)),
        ))
    }
}
