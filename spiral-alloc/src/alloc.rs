use core::ptr::NonNull;

use super::SPIRAL_ALIGNMENT;
use allocator_api2::alloc::*;
use allocator_api2::vec::Vec;

#[derive(Copy, Clone, Debug)]
pub struct AlignedAllocator {
    min_alignment: usize,
}

impl AlignedAllocator {
    pub const fn with_default_alignment() -> Self {
        assert!(SPIRAL_ALIGNMENT.is_power_of_two());
        Self {
            min_alignment: SPIRAL_ALIGNMENT,
        }
    }

    pub fn min_alignment(&self) -> usize {
        self.min_alignment
    }

    pub fn is_aligned_to<T>(&self, ptr: *const T) -> bool {
        ptr.align_offset(self.min_alignment) == 0
    }

    fn ensure_min_alignment(&self, layout: Layout) -> Result<Layout, LayoutError> {
        layout.align_to(self.min_alignment)
    }
}

impl Default for AlignedAllocator {
    fn default() -> Self {
        Self::with_default_alignment()
    }
}

unsafe impl Allocator for AlignedAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global.allocate(self.ensure_min_alignment(layout).map_err(|_| AllocError)?)
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        Global.deallocate(ptr, self.ensure_min_alignment(layout).unwrap())
    }
}

pub type AlignedVec<T> = Vec<T, AlignedAllocator>;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_alignment() {
        let alloc = AlignedAllocator::with_default_alignment();
        assert_eq!(SPIRAL_ALIGNMENT, 128);
        assert_eq!(alloc.min_alignment, SPIRAL_ALIGNMENT);

        let ptr = alloc
            .allocate(core::alloc::Layout::new::<u8>())
            .unwrap()
            .as_ptr() as *const u8; // Cast to thin pointer
        assert_eq!(ptr.align_offset(alloc.min_alignment), 0_usize);
    }
}
