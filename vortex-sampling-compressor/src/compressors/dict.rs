use std::collections::HashSet;

use vortex::array::{Primitive, PrimitiveArray, VarBin, VarBinArray};
use vortex::encoding::EncodingRef;
use vortex::stats::ArrayStatistics;
use vortex::{Array, ArrayDef, IntoArray};
use vortex_dict::{dict_encode_primitive, dict_encode_varbin, Dict, DictArray, DictEncoding};
use vortex_error::VortexResult;

use crate::compressors::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::{constants, SamplingCompressor};

#[derive(Debug)]
pub struct DictCompressor;

impl EncodingCompressor for DictCompressor {
    fn id(&self) -> &str {
        Dict::ID.as_ref()
    }

    fn cost(&self) -> u8 {
        constants::depth::DICT_COST
    }

    fn decompression_gib_per_second(&self) -> f64 {
        constants::decompression::DICT_GIB_PER_S
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor> {
        // TODO(robert): Add support for VarBinView
        if array.encoding().id() != Primitive::ID && array.encoding().id() != VarBin::ID {
            return None;
        };

        // No point dictionary coding if the array is unique.
        // We don't have a unique stat yet, but strict-sorted implies unique.
        if array
            .statistics()
            .compute_is_strict_sorted()
            .unwrap_or(false)
        {
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
        let (codes, values) = match array.encoding().id() {
            Primitive::ID => {
                let p = PrimitiveArray::try_from(array)?;
                let (codes, values) = dict_encode_primitive(&p);
                (codes.into_array(), values.into_array())
            }
            VarBin::ID => {
                let vb = VarBinArray::try_from(array)?;
                let (codes, values) = dict_encode_varbin(&vb);
                (codes.into_array(), values.into_array())
            }

            _ => unreachable!("This array kind should have been filtered out"),
        };

        let (codes, values) = (
            ctx.auxiliary("codes")
                .excluding(self)
                .compress(&codes, like.as_ref().and_then(|l| l.child(0)))?,
            ctx.named("values")
                .excluding(self)
                .compress(&values, like.as_ref().and_then(|l| l.child(1)))?,
        );

        Ok(CompressedArray::new(
            DictArray::try_new(codes.array, values.array)?.into_array(),
            Some(CompressionTree::new(self, vec![codes.path, values.path])),
        ))
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([&DictEncoding as EncodingRef])
    }
}
