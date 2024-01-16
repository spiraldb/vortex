use core::alloc::Layout;
use core::alloc::LayoutError;
use core::ptr::NonNull;

use allocator_api2::alloc::*;
use allocator_api2::vec::Vec;

use super::SPIRAL_ALIGNMENT;

#[derive(Copy, Clone, Default, Debug)]
pub struct AlignedAllocator {
    min_alignment: usize,
}

impl AlignedAllocator {
    pub const fn default() -> Self {
        Self {
            // 128-byte aligned
            min_alignment: SPIRAL_ALIGNMENT as usize,
        }
    }

    pub fn with_min_alignment(min_alignment: usize) -> Result<Self, LayoutError> {
        let layout = Layout::from_size_align(1, min_alignment);
        layout.map(|_| Self { min_alignment })
    }

    pub fn is_aligned_to<T>(&self, ptr: *const T) -> bool {
        ptr as usize % self.min_alignment == 0_usize
    }

    fn ensure_min_alignment(&self, layout: Layout) -> Result<Layout, LayoutError> {
        if self.min_alignment > layout.align() {
            layout.align_to(self.min_alignment)
        } else {
            Ok(layout)
        }
    }
}

unsafe impl Allocator for AlignedAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global.allocate(self.ensure_min_alignment(layout).unwrap())
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
        let alloc = AlignedAllocator::default();
        assert_eq!(alloc.min_alignment, 128_usize);

        let ptr = alloc
            .allocate(core::alloc::Layout::new::<u8>())
            .unwrap()
            .as_ptr() as *const u8; // Cast to thin pointer
        assert_eq!(ptr as usize % alloc.min_alignment, 0_usize);
    }
}
