use vortex::encoding::ids;
use vortex::impl_encoding;
use vortex_error::VortexResult;

impl_encoding!("vortex.alprd", ids::ALP_RD, ALPRD);

pub struct ALPRDMetadata {
    exception_count: u32,
    right_bit_width: u8,
}

impl ALPRDArray {
    pub fn try_new() -> VortexResult<Self> {
        todo!("this")
    }
}
