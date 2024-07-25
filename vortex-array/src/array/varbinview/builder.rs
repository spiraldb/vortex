use core::ptr::NonNull;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use arrow_buffer::{Buffer as ArrowBuffer, NullBufferBuilder};
use vortex_buffer::Buffer;
use vortex_dtype::{DType, PType};

use crate::array::primitive::PrimitiveArray;
use crate::array::varbinview::{BinaryView, Inlined, Ref, VarBinViewArray, VIEW_SIZE};
use crate::validity::Validity;
use crate::{ArrayData, IntoArray, IntoArrayData, ToArray};

// BinaryView has 8 byte alignment (because that's what's in the arrow spec), but arrow-rs
// erroneously requires 16 byte alignment.

pub struct VarBinViewBuilder<T: AsRef<[u8]>> {
    views: Vec<BinaryView>,
    nulls: NullBufferBuilder,
    completed: Vec<ArrayData>,
    in_progress: Vec<u8>,
    block_size: u32,
    phantom: PhantomData<T>,
}

impl<T: AsRef<[u8]>> VarBinViewBuilder<T> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            views: Vec::with_capacity(capacity),
            nulls: NullBufferBuilder::new(capacity),
            completed: Vec::new(),
            in_progress: Vec::new(),
            block_size: 16 * 1024,
            phantom: Default::default(),
        }
    }

    #[inline]
    pub fn push(&mut self, value: Option<T>) {
        match value {
            None => self.push_null(),
            Some(v) => self.push_value(v),
        }
    }

    #[inline]
    pub fn push_value(&mut self, value: T) {
        let vbytes = value.as_ref();
        if self.in_progress.len() + vbytes.len() > self.in_progress.capacity() {
            let done = mem::replace(
                &mut self.in_progress,
                Vec::with_capacity(vbytes.len().max(self.block_size as usize)),
            );
            if !done.is_empty() {
                assert!(self.completed.len() < u32::MAX as usize);
                self.completed
                    .push(PrimitiveArray::from(done).into_array_data());
            }
        }

        if vbytes.len() > BinaryView::MAX_INLINED_SIZE {
            self.views.push(BinaryView {
                _ref: Ref::new(
                    vbytes.len() as u32,
                    vbytes[0..4].try_into().unwrap(),
                    self.completed.len() as u32,
                    self.in_progress.len() as u32,
                ),
            });
            self.in_progress.extend_from_slice(vbytes);
        } else {
            self.views.push(BinaryView {
                inlined: Inlined::new(vbytes),
            });
        }
        self.nulls.append_non_null();
    }

    #[inline]
    pub fn push_null(&mut self) {
        self.views.push(BinaryView {
            inlined: Inlined::new(b""),
        });
        self.nulls.append_null();
    }

    pub fn finish(mut self, dtype: DType) -> VarBinViewArray {
        let mut completed = self
            .completed
            .into_iter()
            .map(|d| d.into_array())
            .collect::<Vec<_>>();
        if !self.in_progress.is_empty() {
            completed.push(PrimitiveArray::from(self.in_progress).into_array());
        }

        let nulls = self.nulls.finish();
        let validity = if dtype.is_nullable() {
            nulls.map(Validity::from).unwrap_or(Validity::AllValid)
        } else {
            assert!(nulls.is_none(), "dtype and validity mismatch");
            Validity::NonNullable
        };

        // convert Vec<BinaryView> to Vec<u8> which can be stored as an array
        // have to ensure that we use the correct allocator at deallocation time
        let views: Buffer = unsafe {
            let mut views_clone = mem::take(&mut self.views);
            let buf = ArrowBuffer::from_custom_allocation(
                NonNull::new_unchecked(views_clone.as_mut_ptr() as *mut u8),
                views_clone.len() * VIEW_SIZE,
                Arc::new(views_clone),
            );
            Buffer::Arrow(buf)
        };

        VarBinViewArray::try_new(
            PrimitiveArray::new(views, PType::U8, Validity::NonNullable).to_array(),
            completed,
            dtype,
            validity,
        )
        .unwrap()
    }
}
