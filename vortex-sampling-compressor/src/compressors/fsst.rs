use std::collections::HashSet;

use vortex::array::{VarBinArray, VarBinViewArray};
use vortex::encoding::EncodingRef;
use vortex::{ArrayDType, ArrayDef, IntoArray};
use vortex_dict::DictArray;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, VortexResult};
use vortex_fsst::{fsst_compress, FSSTEncoding, FSST};

use super::{CompressedArray, CompressionTree, EncodingCompressor};
use crate::SamplingCompressor;

#[derive(Debug)]
pub struct FSSTCompressor;

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

        // FSST cannot be applied recursively.
        if array.encoding().id() == FSST::ID {
            return None;
        }

        Some(self)
    }

    fn compress<'a>(
        &'a self,
        array: &vortex::Array,
        _like: Option<CompressionTree<'a>>,
        _ctx: SamplingCompressor<'a>,
    ) -> VortexResult<super::CompressedArray<'a>> {
        // TODO(aduffy): use like array to clone the existing symbol table
        let fsst_array =
            if VarBinArray::try_from(array).is_ok() || VarBinViewArray::try_from(array).is_ok() {
                // For a VarBinArray or VarBinViewArray, compress directly.
                fsst_compress(array.clone(), None)
            } else if let Ok(dict) = DictArray::try_from(array) {
                // For a dict array, just compress the values
                fsst_compress(dict.values(), None)
            } else {
                vortex_bail!(
                    InvalidArgument: "unsupported encoding for FSSTCompressor {:?}",
                    array.encoding().id()
                )
            };

        Ok(CompressedArray::new(fsst_array.into_array(), None))
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        HashSet::from([&FSSTEncoding as EncodingRef])
    }
}
