use std::collections::HashSet;

use arbitrary::Error::EmptyChoose;
use arbitrary::{Arbitrary, Result, Unstructured};

use crate::compressors::{CompressorRef, EncodingCompressor};
use crate::{SamplingCompressor, ALL_COMPRESSORS};

impl<'a, 'b: 'a> Arbitrary<'a> for SamplingCompressor<'b> {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let compressors: HashSet<CompressorRef> = u.arbitrary()?;
        if compressors.is_empty() {
            return Err(EmptyChoose);
        }
        Ok(Self::new(compressors))
    }
}

impl<'a, 'b: 'a> Arbitrary<'a> for &'b dyn EncodingCompressor {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        u.choose(&ALL_COMPRESSORS.clone()).cloned()
    }
}
