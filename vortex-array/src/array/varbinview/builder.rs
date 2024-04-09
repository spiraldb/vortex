use std::marker::PhantomData;
use std::mem;
use std::mem::ManuallyDrop;

use arrow_buffer::NullBufferBuilder;
use vortex_schema::DType;

use crate::array::primitive::PrimitiveArray;
use crate::array::varbinview::{BinaryView, Inlined, Ref, VarBinViewArray, VIEW_SIZE};
use crate::array::{Array, ArrayRef, IntoArray};
use crate::validity::Validity;

pub struct VarBinViewBuilder<T: AsRef<[u8]>> {
    views: Vec<BinaryView>,
    nulls: NullBufferBuilder,
    completed: Vec<ArrayRef>,
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
    pub fn append(&mut self, value: Option<T>) {
        match value {
            None => self.append_null(),
            Some(v) => self.append_value(v),
        }
    }

    #[inline]
    pub fn append_value(&mut self, value: T) {
        let vbytes = value.as_ref();
        if self.in_progress.len() + vbytes.len() > self.in_progress.capacity() {
            let done = mem::replace(
                &mut self.in_progress,
                Vec::with_capacity(vbytes.len().max(self.block_size as usize)),
            );
            if !done.is_empty() {
                assert!(self.completed.len() < u32::MAX as usize);
                self.completed.push(PrimitiveArray::from(done).into_array());
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
    pub fn append_null(&mut self) {
        self.views.push(BinaryView {
            inlined: Inlined::new("".as_bytes()),
        });
        self.nulls.append_null();
    }

    pub fn finish(&mut self, dtype: DType) -> VarBinViewArray {
        let mut completed = mem::take(&mut self.completed);
        if !self.in_progress.is_empty() {
            completed.push(PrimitiveArray::from(mem::take(&mut self.in_progress)).into_array());
        }

        let nulls = self.nulls.finish();
        let validity = if dtype.is_nullable() {
            Some(
                nulls
                    .map(Validity::from)
                    .unwrap_or_else(|| Validity::Valid(self.views.len())),
            )
        } else {
            assert!(nulls.is_none(), "dtype and validity mismatch");
            None
        };

        let views_u8: Vec<u8> = unsafe {
            let mut views_clone = ManuallyDrop::new(mem::take(&mut self.views));
            Vec::from_raw_parts(
                views_clone.as_mut_ptr() as _,
                views_clone.len() * VIEW_SIZE,
                views_clone.capacity() * VIEW_SIZE,
            )
        };

        VarBinViewArray::try_new(views_u8.into_array(), completed, dtype, validity).unwrap()
    }
}
