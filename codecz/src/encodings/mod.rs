use enum_display::EnumDisplay;

mod error;
pub use error::CodecError;
pub mod alp;
pub mod ffor;
pub mod ree;
pub mod zigzag;

pub use spiral_alloc::{AlignedAllocator, AlignedVec, ALIGNED_ALLOCATOR, SPIRAL_ALIGNMENT};

pub(crate) type ByteBuffer = codecz_sys::ByteBuffer_t;
pub(crate) type WrittenBuffer = codecz_sys::WrittenBuffer_t;
pub(crate) type OneBufferResult = codecz_sys::OneBufferResult_t;
pub(crate) type TwoBufferResult = codecz_sys::TwoBufferResult_t;

#[derive(Debug, PartialEq, EnumDisplay)]
pub enum Codec {
    ALP,
    FFoR,
    REE,
    ZigZag,
}

#[derive(Debug, PartialEq, EnumDisplay)]
pub enum CodecFunction {
    Prelude,
    Encode,
    Decode,
    CollectExceptions,
    EncodeSingle,
    DecodeSingle,
}

mod test {
    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_alignment() {
        assert_eq!(
            codecz_sys::SPIRAL_ALIGNMENT as usize,
            spiral_alloc::SPIRAL_ALIGNMENT,
        );
        assert_eq!(
            codecz_sys::SPIRAL_ALIGNMENT as usize,
            super::ALIGNED_ALLOCATOR.min_alignment(),
        );
        assert!(arrow_buffer::alloc::ALIGNMENT >= 64);
    }
}
