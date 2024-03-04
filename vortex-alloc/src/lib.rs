mod alloc;

pub use alloc::*;

pub const VORTEX_ALIGNMENT: usize = 128;
pub const ALIGNED_ALLOCATOR: AlignedAllocator = AlignedAllocator::with_default_alignment();
