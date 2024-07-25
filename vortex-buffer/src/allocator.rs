use std::alloc::{AllocError, Allocator, Global, Layout};
use std::ptr::NonNull;

pub struct MinAlignmentAllocator {
    min_alignment: usize,
}

unsafe impl Allocator for MinAlignmentAllocator {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        Global.allocate(
            layout
                .align_to(self.min_alignment)
                .map_err(|_| AllocError)?,
        )
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        let layout = layout.align_to(self.min_alignment).expect(
            "align_to failed, which can only happen if self.min_alignment is not a power of two",
        );
        unsafe { Global.deallocate(ptr, layout) }
    }
}

impl MinAlignmentAllocator {
    pub const fn new(min_alignment: usize) -> Self {
        let min_alignment = min_alignment.next_power_of_two();
        Self { min_alignment }
    }
}
