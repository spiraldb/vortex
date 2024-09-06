use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;

use fsst::Compressor;
use vortex::array::{VarBin, VarBinView};
use vortex::encoding::EncodingRef;
use vortex::{ArrayDType, ArrayDef, IntoArray};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};
use vortex_fsst::{fsst_compress, fsst_train_compressor, FSSTEncoding, FSST};

use super::{CompressedArray, CompressionTree, EncoderMetadata, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct FSSTCompressor;

/// Maximum size in bytes of the FSST symbol table
const FSST_SYMTAB_MAX_SIZE: usize = 8 * 255 + 255;

impl EncoderMetadata for Compressor {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl EncodingCompressor for FSSTCompressor {
    fn id(&self) -> &str {
        FSST::ID.as_ref()
    }

    fn can_compress(&self, array: &vortex::Array) -> Option<&dyn EncodingCompressor> {
        // FSST arrays must have DType::Utf8.
        //
        // Note that while it can accept binary data, it is unlikely to perform well.
        if !matches!(array.dtype(), &DType::Utf8(_)) {
            return None;
        }

        // FSST can be applied on top of VarBin and VarBinView
        if array.encoding().id() == VarBin::ID || array.encoding().id() == VarBinView::ID {
            return Some(self);
        }

        None
    }

    fn compress<'a>(
        &'a self,
        array: &vortex::Array,
        // TODO(aduffy): reuse compressor from sample run if we have saved it off.
        like: Option<CompressionTree<'a>>,
        _ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        // Size-check: FSST has a builtin 2KB overhead due to the symbol table, and usually compresses
        // between 2-3x depending on the text quality.
        //
        // It's not worth running a full compression step unless the array is large enough.
        if array.nbytes() < 10 * FSST_SYMTAB_MAX_SIZE {
            return Ok(CompressedArray::uncompressed(array.clone()));
        }

        let compressor = like
            .and_then(|mut tree| tree.metadata())
            .map(VortexResult::Ok)
            .unwrap_or_else(|| Ok(Arc::new(fsst_train_compressor(array)?)))?;

        let Some(fsst_compressor) = compressor.as_any().downcast_ref::<Compressor>() else {
            vortex_bail!("Could not downcast metadata as FSST Compressor")
        };

        let result_array =
            if array.encoding().id() == VarBin::ID || array.encoding().id() == VarBinView::ID {
                // For a VarBinArray or VarBinViewArray, compress directly.
                fsst_compress(array, fsst_compressor)?.into_array()
            } else {
                vortex_bail!("Unsupported encoding for FSSTCompressor: {}", array.encoding().id())
            };

        Ok(CompressedArray::new(
            result_array,
            // Save a copy of the compressor that was used to compress this array.
            Some(CompressionTree::new_with_metadata(self, vec![], compressor)),
        ))
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([&FSSTEncoding as EncodingRef])
    }
}
