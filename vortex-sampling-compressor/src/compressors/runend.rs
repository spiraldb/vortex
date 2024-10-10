use std::collections::HashSet;

use vortex::array::Primitive;
use vortex::encoding::EncodingRef;
use vortex::stats::ArrayStatistics;
use vortex::{Array, ArrayDef, IntoArray};
use vortex_error::VortexResult;
use vortex_runend::compress::runend_encode;
use vortex_runend::{RunEnd, RunEndArray, RunEndEncoding};

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::{constants, SamplingCompressor};

pub const DEFAULT_RUN_END_COMPRESSOR: RunEndCompressor = RunEndCompressor { ree_threshold: 2.0 };

#[derive(Debug, Clone, Copy)]
pub struct RunEndCompressor {
    ree_threshold: f32,
}

impl EncodingCompressor for RunEndCompressor {
    fn id(&self) -> &str {
        RunEnd::ID.as_ref()
    }

    fn cost(&self) -> u8 {
        constants::RUN_END_COST
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        if array.encoding().id() != Primitive::ID {
            return None;
        }

        let avg_run_length = array.len() as f32
            / array
                .statistics()
                .compute_run_count()
                .unwrap_or(array.len()) as f32;
        if avg_run_length < self.ree_threshold {
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
        let primitive_array = array.as_primitive();

        let (ends, values) = runend_encode(&primitive_array);
        let compressed_ends = ctx
            .auxiliary("ends")
            .compress(&ends.into_array(), like.as_ref().and_then(|l| l.child(0)))?;
        let compressed_values = ctx
            .named("values")
            .excluding(self)
            .compress(&values.into_array(), like.as_ref().and_then(|l| l.child(1)))?;

        Ok(CompressedArray::new(
            RunEndArray::try_new(
                compressed_ends.array,
                compressed_values.array,
                ctx.compress_validity(primitive_array.validity())?,
            )
            .map(|a| a.into_array())?,
            Some(CompressionTree::new(
                self,
                vec![compressed_ends.path, compressed_values.path],
            )),
        ))
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([&RunEndEncoding as EncodingRef])
    }
}
