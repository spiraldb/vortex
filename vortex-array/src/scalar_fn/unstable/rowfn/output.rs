// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Vortex-owned output allocation, sinks, and zero-copy primitive publication.

use std::mem::MaybeUninit;
use std::sync::Arc;

use ::rowfn::{BooleanOutput, OutputBinding, OutputBuffer};
use ::rowfn::kernels::lane_kernels::IndexedSource;
use ::rowfn::sink::{ListOutput, OutputSink, Utf8Output, WriteUtf8};
use vortex_buffer::{BitBuffer, BufferAllocatorRef};
use vortex_error::{VortexResult, vortex_err};

use super::VortexHost;
use crate::{ArrayRef, ExecutionCtx, IntoArray};
use crate::arrays::{BoolArray, FixedSizeListArray};
use crate::dtype::{DType, NativePType, Nullability};
use crate::scalar_fn::unstable::row::{self as legacy, OutputElement, ViewLen};
use crate::validity::Validity;

/// The existing allocator-aware storage and its publication resource.
pub struct VortexBuffer<T: OutputElement> {
    values: T::Buffer,
    allocator: BufferAllocatorRef,
}
// SAFETY: the existing OutputBuffer contract preserves slots and permits partial abandonment.
unsafe impl<T: OutputElement> OutputBuffer<T> for VortexBuffer<T> {
    type Finished = ArrayRef;
    fn slots(&mut self) -> &mut [MaybeUninit<T>] { legacy::OutputBuffer::slots(&mut self.values) }
    unsafe fn finish(self, len: usize) -> ArrayRef {
        // SAFETY: the caller supplies exactly the initialized prefix required by both contracts.
        unsafe { legacy::OutputBuffer::finish(self.values, len, &self.allocator) }
    }
}
impl<T: NativePType> OutputBinding<T> for VortexHost {
    type Buffer = VortexBuffer<T>;
    fn output_type() -> DType { T::element_dtype() }
    fn allocate(rows: usize, ctx: &mut ExecutionCtx) -> VortexResult<Self::Buffer> {
        Ok(VortexBuffer { values: T::with_capacity(rows, ctx.allocator()), allocator: ctx.allocator().clone() })
    }
}
impl OutputBinding<bool> for VortexHost {
    type Buffer = VortexBuffer<bool>;
    fn output_type() -> DType { bool::element_dtype() }
    fn allocate(rows: usize, ctx: &mut ExecutionCtx) -> VortexResult<Self::Buffer> {
        Ok(VortexBuffer { values: bool::with_capacity(rows, ctx.allocator()), allocator: ctx.allocator().clone() })
    }
    fn build_from<S: IndexedSource>(source: S, apply: impl Fn(S::Item) -> bool,
        ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        Ok(bool::build_from(source, apply, ctx.allocator()))
    }
}
// SAFETY: the shared packing kernels invoke callbacks only within the supplied logical length.
unsafe impl BooleanOutput for VortexHost {
    fn collect_bool<const MULTIVERSIONED: bool>(rows: usize, apply: impl FnMut(usize) -> bool,
        ctx: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        let bits = if MULTIVERSIONED {
            BitBuffer::collect_bool_multiversioned_in(rows, apply, ctx.allocator().clone())
        } else {
            BitBuffer::collect_bool_in(rows, apply, ctx.allocator().clone())
        };
        Ok(BoolArray::new(bits, Validity::NonNullable).into_array())
    }
}
impl<T: NativePType> ListOutput<T> for VortexHost {
    fn list_type(width: usize) -> VortexResult<DType> {
        let width = u32::try_from(width).map_err(|_| vortex_err!("list width must fit u32"))?;
        Ok(DType::FixedSizeList(Arc::new(T::element_dtype()), width, Nullability::NonNullable))
    }
    fn finish_list(children: ArrayRef, rows: usize, width: usize, _: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        let width = u32::try_from(width).map_err(|_| vortex_err!("list width must fit u32"))?;
        Ok(FixedSizeListArray::try_new(children, width, Validity::NonNullable, rows)?.into_array())
    }
}

/// Reuses the existing output-owned UTF-8 sink and its execution allocator.
pub struct VortexUtf8Sink(legacy::Utf8Sink);
impl WriteUtf8 for legacy::Utf8Writer<'_> {
    fn write(self, value: &str) { self.write(value); }
    fn write_parts(self, parts: &[&str]) { self.write_parts(parts); }
}
impl Utf8Output for VortexHost { type Sink = VortexUtf8Sink; }
// SAFETY: delegation retains the existing initialized UTF-8 row mapping and ownership contract.
unsafe impl OutputSink<VortexHost> for VortexUtf8Sink {
    type Params = ();
    type Rows<'a> = <legacy::Utf8Sink as legacy::OutputSink>::Rows<'a>;
    type Row<'a> = legacy::Utf8Writer<'a>;
    type WriteToken = ();
    fn storage_type(params: &()) -> VortexResult<DType> {
        Ok(<legacy::Utf8Sink as legacy::OutputSink>::storage_dtype(params))
    }
    fn allocate(rows: usize, params: &(), ctx: &mut ExecutionCtx) -> VortexResult<Self> {
        <legacy::Utf8Sink as legacy::OutputSink>::with_capacity(rows, params, ctx.allocator()).map(Self)
    }
    fn rows(&mut self) -> Self::Rows<'_> { legacy::OutputSink::rows(&mut self.0) }
    fn len(rows: &Self::Rows<'_>) -> usize { rows.len() }
    fn initialize(rows: &mut Self::Rows<'_>) { <legacy::Utf8Sink as legacy::OutputSink>::initialize_skipped_rows(rows); }
    unsafe fn row<'a>(rows: &'a mut Self::Rows<'_>, index: usize) -> Self::Row<'a> {
        // SAFETY: both row contracts require the same validated index and exact row mapping.
        unsafe { <legacy::Utf8Sink as legacy::OutputSink>::row_unchecked(rows, index) }
    }
    unsafe fn finish(self, _: &mut ExecutionCtx) -> VortexResult<ArrayRef> {
        // SAFETY: every row remains an initialized view into output-owned storage.
        unsafe { legacy::OutputSink::finish(self.0) }
    }
}
