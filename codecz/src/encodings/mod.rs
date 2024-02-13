use enum_display::EnumDisplay;

mod error;
pub use error::CodecError;
pub mod alp;
pub mod ffor;
pub mod ree;
pub mod zigzag;

pub type AlignedAllocator = codecz_sys::alloc::AlignedAllocator;
pub type AlignedVec<T> = codecz_sys::alloc::AlignedVec<T>;

pub const ALIGNED_ALLOCATOR: AlignedAllocator = AlignedAllocator::default();
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
}
