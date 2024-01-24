use codecz_sys::alloc::AlignedAllocator;
use enum_display::EnumDisplay;

mod error;
pub use error::CodecError;
pub mod ree;

pub(crate) const ALIGNED_ALLOCATOR: AlignedAllocator = AlignedAllocator::default();

#[derive(Debug, PartialEq, EnumDisplay)]
pub enum Codec {
    REE,
}

#[derive(Debug, PartialEq, EnumDisplay)]
pub enum CodecFunction {
    Encode,
    Decode,
}
