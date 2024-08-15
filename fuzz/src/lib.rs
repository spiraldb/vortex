use std::ops::Range;

use libfuzzer_sys::arbitrary::{Arbitrary, Result, Unstructured};
use vortex::Array;
use vortex_sampling_compressor::compressors::alp::ALPCompressor;
use vortex_sampling_compressor::compressors::bitpacked::BitPackedCompressor;
use vortex_sampling_compressor::compressors::dict::DictCompressor;
use vortex_sampling_compressor::compressors::r#for::FoRCompressor;
use vortex_sampling_compressor::compressors::roaring_bool::RoaringBoolCompressor;
use vortex_sampling_compressor::compressors::roaring_int::RoaringIntCompressor;
use vortex_sampling_compressor::compressors::runend::DEFAULT_RUN_END_COMPRESSOR;
use vortex_sampling_compressor::compressors::sparse::SparseCompressor;
use vortex_sampling_compressor::compressors::zigzag::ZigZagCompressor;
use vortex_sampling_compressor::compressors::EncodingCompressor;

pub struct FuzzArrayAction {
    pub array: Array,
    pub action: Action,
}

impl std::fmt::Debug for FuzzArrayAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuzzArrayAction")
            .field("action", &self.action)
            .field("array", &self.array)
            .finish()
    }
}

#[derive()]
pub enum Action {
    NoOp,
    Compress(Box<dyn EncodingCompressor>),
    Slice(Range<usize>),
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoOp => write!(f, "NoOp"),
            Self::Slice(arg0) => f.debug_tuple("Slice").field(arg0).finish(),
            Self::Compress(c) => write!(f, "Compress({})", c.id()),
        }
    }
}

impl<'a> Arbitrary<'a> for FuzzArrayAction {
    fn arbitrary(u: &mut Unstructured<'a>) -> Result<Self> {
        let array = Array::arbitrary(u)?;
        let action = match u.int_in_range(0..=9)? {
            0 => {
                let start = u.choose_index(array.len())?;
                let stop = u.choose_index(array.len() - start)? + start;
                Action::Slice(start..stop)
            }
            1 => Action::Compress(Box::new(ALPCompressor) as _),
            2 => Action::Compress(Box::new(BitPackedCompressor) as _),
            3 => Action::Compress(Box::new(DictCompressor) as _),
            4 => Action::Compress(Box::new(FoRCompressor) as _),
            5 => Action::Compress(Box::new(RoaringBoolCompressor) as _),
            6 => Action::Compress(Box::new(RoaringIntCompressor) as _),
            7 => Action::Compress(Box::new(DEFAULT_RUN_END_COMPRESSOR) as _),
            8 => Action::Compress(Box::new(SparseCompressor) as _),
            9 => Action::Compress(Box::new(ZigZagCompressor) as _),
            _ => Action::NoOp,
        };

        Ok(Self { array, action })
    }
}
