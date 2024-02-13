mod alloc;

pub use alloc::*;

pub const SPIRAL_ALIGNMENT: usize = 128;
pub const ALIGNED_ALLOCATOR: AlignedAllocator = AlignedAllocator::with_default_alignment();
