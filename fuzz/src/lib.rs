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
    pub actions: Vec<Action>,
}

impl std::fmt::Debug for FuzzArrayAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FuzzArrayAction")
            .field("action", &self.actions)
            .field("array", &self.array)
            .finish()
    }
}

#[derive()]
pub enum Action {
    Compress(&'static dyn EncodingCompressor),
    Slice(Range<usize>),
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
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
            1 => Action::Compress(&ALPCompressor),
            2 => Action::Compress(&BitPackedCompressor),
            3 => Action::Compress(&DictCompressor),
            4 => Action::Compress(&FoRCompressor),
            5 => Action::Compress(&RoaringBoolCompressor),
            6 => Action::Compress(&RoaringIntCompressor),
            7 => Action::Compress(&DEFAULT_RUN_END_COMPRESSOR),
            8 => Action::Compress(&SparseCompressor),
            9 => Action::Compress(&ZigZagCompressor),
            _ => unreachable!(),
        };

        Ok(Self {
            array,
            actions: vec![action],
        })
    }
}
