use std::collections::HashSet;
use std::fmt::Debug;

use fsst::Compressor;
use vortex::array::{VarBin, VarBinView};
use vortex::encoding::EncodingRef;
use vortex::{ArrayDType, ArrayDef, IntoArray};
use vortex_dict::{Dict, DictArray};
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};
use vortex_fsst::{fsst_compress, fsst_train_compressor, FSSTEncoding, FSST};

use super::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct FSSTCompressor;

/// Size in bytes of the Symbol table for FSST
const FSST_SYMBOL_TABLE_SIZE: usize = 4_096;

/// We use a 16KB sample of text from the input.
///
/// This value is derived from the FSST paper section 4.4
const DEFAULT_SAMPLE_BYTES: usize = 16 * 1_024;

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

        // FSST can be applied on top of VarBin, VarBinView, and Dict encodings.
        if array.encoding().id() == VarBin::ID
            || array.encoding().id() == VarBinView::ID
            || array.encoding().id() == Dict::ID
        {
            return Some(self);
        }

        // Size-check: FSST has a builtin 4KB overhead due to the symbol table, and usually compresses
        // between 2-3x depending on the text quality.
        if array.nbytes() > 10 * FSST_SYMBOL_TABLE_SIZE {
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
    ) -> VortexResult<super::CompressedArray<'a>> {
        let compressor = like
            .and_then(|mut c| unsafe { c.metadata::<Compressor>() })
            .map(|m| {
                println!("using pretrained compressor");
                m
            })
            .unwrap_or_else(|| {
                println!("training new compressor");
                Box::new(fsst_train_compressor(array, DEFAULT_SAMPLE_BYTES))
            });

        let result_array =
            if array.encoding().id() == VarBin::ID || array.encoding().id() == VarBinView::ID {
                // For a VarBinArray or VarBinViewArray, compress directly.
                fsst_compress(array.clone(), compressor.as_ref()).into_array()
            } else if let Ok(dict) = DictArray::try_from(array) {
                // For a dict array, just compress the values
                let values = fsst_compress(dict.values(), compressor.as_ref());
                let codes = dict.codes();

                DictArray::try_new(codes, values.into_array())?.into_array()
            } else {
                vortex_bail!(
                    InvalidArgument: "unsupported encoding for FSSTCompressor {:?}",
                    array.encoding().id()
                )
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
