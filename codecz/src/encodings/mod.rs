use enum_display::EnumDisplay;

mod error;
pub use error::CodecError;
pub mod alp;
pub mod ree;

pub type AlignedAllocator = codecz_sys::alloc::AlignedAllocator;
pub type AlignedVec<T> = codecz_sys::alloc::AlignedVec<T>;

pub(crate) const ALIGNED_ALLOCATOR: AlignedAllocator = AlignedAllocator::default();
pub(crate) type WrittenBuffer = codecz_sys::WrittenBuffer_t;
pub(crate) type ByteBuffer = codecz_sys::ByteBuffer_t;

#[derive(Debug, PartialEq, EnumDisplay)]
pub enum Codec {
    REE,
    ALP,
}

#[derive(Debug, PartialEq, EnumDisplay)]
pub enum CodecFunction {
    Prelude,
    Encode,
    Decode,
}
