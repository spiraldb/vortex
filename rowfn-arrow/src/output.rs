// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Arrow-owned allocations published without an intermediate primitive collector.

use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, FixedSizeListArray, PrimitiveArray};
use arrow_array::types::ArrowPrimitiveType;
use arrow_buffer::{BooleanBuffer, Buffer, MutableBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field};
use rowfn::{BooleanOutput, Host, OutputBinding, OutputBuffer};
use rowfn::kernels::bit::{collect_bool_words, collect_bool_words_multiversioned};
use rowfn::kernels::lane_kernels::{IndexedSource, IndexedSourceExt};
use rowfn::sink::ListOutput;

use super::{ArrowHost, ArrowPrimitive};

/// Stable Arrow allocation whose length stays zero until its initialized prefix is published.
pub struct PrimitiveOutput<T> { buffer: MutableBuffer, slots: usize, marker: PhantomData<T> }
// SAFETY: MutableBuffer owns aligned storage, never reads uninitialized bytes, and does not move
// its allocation when the wrapper moves. The exposed slot count remains fixed until publication.
unsafe impl<T: ArrowPrimitive> OutputBuffer<T> for PrimitiveOutput<T> {
    type Finished = ArrayRef;
    fn slots(&mut self) -> &mut [MaybeUninit<T>] {
        // SAFETY: allocation reserved slots * size_of<T> bytes with Arrow's native alignment.
        // MaybeUninit does not require the bytes to be initialized, and the borrow is exclusive.
        unsafe { std::slice::from_raw_parts_mut(self.buffer.as_mut_ptr().cast(), self.slots) }
    }
    unsafe fn finish(mut self, len: usize) -> ArrayRef {
        assert!(len <= self.slots);
        // SAFETY: the caller initialized the published prefix. Allocation checked byte capacity.
        unsafe { self.buffer.set_len(len * size_of::<T>()) };
        let values = ScalarBuffer::<T>::new(self.buffer.into(), 0, len);
        Arc::new(PrimitiveArray::<T::Arrow>::new(values, None))
    }
}
impl<T: ArrowPrimitive> OutputBinding<T> for ArrowHost {
    type Buffer = PrimitiveOutput<T>;
    fn output_type() -> Field { Field::new("result", T::Arrow::DATA_TYPE, false) }
    fn allocate(rows: usize, _: &mut ()) -> Result<Self::Buffer, ArrowError> {
        let bytes = rows.checked_mul(size_of::<T>()).ok_or_else(|| Self::error("output byte count exceeds usize"))?;
        Ok(PrimitiveOutput { buffer: MutableBuffer::new(bytes), slots: rows, marker: PhantomData })
    }

    fn build_from<S: IndexedSource>(source: S, apply: impl Fn(S::Item) -> T,
        ctx: &mut ()) -> Result<ArrayRef, ArrowError> {
        let rows = source.len();
        let mut output = <Self as OutputBinding<T>>::allocate(rows, ctx)?;
        if S::PREFER_LINEAR {
            source.map_into_linear(&mut output.slots()[..rows], apply);
        } else {
            source.map_into(&mut output.slots()[..rows], apply);
        }
        // SAFETY: every published slot was initialized by the loop.
        Ok(unsafe { output.finish(rows) })
    }
}

/// Byte-per-row temporary storage for selected Boolean traversal.
pub struct BooleanOutputBuffer(Vec<MaybeUninit<bool>>);
// SAFETY: the fixed-length MaybeUninit vector preserves slots and safely abandons any prefix.
unsafe impl OutputBuffer<bool> for BooleanOutputBuffer {
    type Finished = ArrayRef;
    fn slots(&mut self) -> &mut [MaybeUninit<bool>] { &mut self.0 }
    unsafe fn finish(self, len: usize) -> ArrayRef {
        assert!(len <= self.0.len());
        let values = self.0;
        let packed = pack::<true>(len, |index| {
            // SAFETY: the caller initialized the prefix, and packing only requests this prefix.
            unsafe { values[index].assume_init() }
        });
        Arc::new(BooleanArray::new(packed, None))
    }
}
impl OutputBinding<bool> for ArrowHost {
    type Buffer = BooleanOutputBuffer;
    fn output_type() -> Field { Field::new("result", DataType::Boolean, false) }
    fn allocate(rows: usize, _: &mut ()) -> Result<Self::Buffer, ArrowError> {
        Ok(BooleanOutputBuffer(vec![MaybeUninit::uninit(); rows]))
    }
    fn build_from<S: IndexedSource>(source: S, apply: impl Fn(S::Item) -> bool,
        ctx: &mut ()) -> Result<ArrayRef, ArrowError> {
        Self::collect_bool::<false>(source.len(), |index| {
            // SAFETY: the collector supplies only indices below the retained source length.
            apply(unsafe { source.get_unchecked(index) })
        }, ctx)
    }
}

fn pack<const MULTIVERSIONED: bool>(rows: usize, apply: impl FnMut(usize) -> bool) -> BooleanBuffer {
    let word_count = rows.div_ceil(64);
    let mut buffer = MutableBuffer::from_len_zeroed(word_count * 8);
    let words = buffer.typed_data_mut::<u64>();
    if MULTIVERSIONED { collect_bool_words_multiversioned(words, rows, apply); }
    else { collect_bool_words(words, rows, apply); }
    // Arrow bitmaps are byte sequences in little-endian bit order, including on big-endian hosts.
    for word in words { *word = word.to_le(); }
    BooleanBuffer::new(Buffer::from(buffer), 0, rows)
}
// SAFETY: the shared packing kernels invoke callbacks only within the supplied logical length.
unsafe impl BooleanOutput for ArrowHost {
    fn collect_bool<const MULTIVERSIONED: bool>(rows: usize, apply: impl FnMut(usize) -> bool,
        _: &mut ()) -> Result<ArrayRef, ArrowError> {
        Ok(Arc::new(BooleanArray::new(pack::<MULTIVERSIONED>(rows, apply), None)))
    }
}
impl<T: ArrowPrimitive> ListOutput<T> for ArrowHost {
    fn list_type(width: usize) -> Result<Field, ArrowError> {
        let width = i32::try_from(width).map_err(|_| Self::error("Arrow list width must fit i32"))?;
        Ok(Field::new("result", DataType::FixedSizeList(Arc::new(Field::new("item", T::Arrow::DATA_TYPE, false)), width), false))
    }
    fn finish_list(children: ArrayRef, rows: usize, width: usize, _: &mut ()) -> Result<ArrayRef, ArrowError> {
        let width = i32::try_from(width).map_err(|_| Self::error("Arrow list width must fit i32"))?;
        let child = Arc::new(Field::new("item", T::Arrow::DATA_TYPE, false));
        Ok(Arc::new(FixedSizeListArray::try_new_with_length(child, width, children, None, rows)?))
    }
}
