// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Scalar slots retain host storage while sharing exact-row initialization contracts.

use std::mem::MaybeUninit;

use crate::{HostResult, OutputBinding, OutputBuffer};
use super::OutputSink;

/// Evidence that the exact scalar row supplied to this callback remains initialized.
///
/// ```compile_fail
/// let token = rowfn::sink::InitializedElement(());
/// ```
///
/// Safe code cannot obtain a token from a local slot unrelated to the callback:
///
/// ```compile_fail
/// let mut unrelated = std::mem::MaybeUninit::uninit();
/// let token = rowfn::sink::InitializedElement::write(&mut unrelated, 1i64);
/// ```
#[must_use = "return the token from the callback that initialized this row"]
pub struct InitializedElement(());

impl InitializedElement {
    /// Initialize the callback's scalar slot.
    ///
    /// # Safety
    /// `row` must be the exact slot supplied to the current callback. Return this token from that
    /// callback and preserve the slot's initialization until it returns. Another slot's token
    /// cannot authorize publication. Violating this contract can cause undefined behavior.
    #[inline]
    pub unsafe fn write<T>(row: &mut MaybeUninit<T>, value: T) -> Self {
        row.write(value);
        Self(())
    }
}

/// An uninitialized scalar sink backed by the host's owned-output allocation.
pub struct UninitElementSink<H: OutputBinding<T>, T: Default + 'static> {
    values: H::Buffer,
    rows: usize,
}

// SAFETY: each slice slot is distinct. The token's unsafe constructor binds it to the callback row.
// OutputBuffer preserves initialized contents and permits abandoning any partial initialization.
unsafe impl<H: OutputBinding<T>, T: Default + 'static> OutputSink<H>
    for UninitElementSink<H, T>
where H::Buffer: 'static {
    type Params = ();
    type Rows<'a> = &'a mut [MaybeUninit<T>];
    type Row<'a> = &'a mut MaybeUninit<T>;
    type WriteToken = InitializedElement;
    fn storage_type(_: &()) -> HostResult<H, H::NativeType> {
        const { assert!(!std::mem::needs_drop::<T>()); }
        Ok(H::output_type())
    }
    fn allocate(rows: usize, _: &(), ctx: &mut H::Context) -> HostResult<H, Self> {
        const { assert!(!std::mem::needs_drop::<T>()); }
        Ok(Self { values: H::allocate(rows, ctx)?, rows })
    }
    fn rows(&mut self) -> Self::Rows<'_> { &mut self.values.slots()[..self.rows] }
    fn len(rows: &Self::Rows<'_>) -> usize { rows.len() }
    fn initialize(rows: &mut Self::Rows<'_>) {
        for row in rows.iter_mut() { row.write(T::default()); }
    }
    #[inline]
    unsafe fn row<'a>(rows: &'a mut Self::Rows<'_>, index: usize) -> Self::Row<'a> {
        // SAFETY: the executor bounds the index by the retained slice length.
        unsafe { rows.get_unchecked_mut(index) }
    }
    unsafe fn finish(self, _: &mut H::Context) -> HostResult<H, H::Column> {
        // SAFETY: the executor has established initialization of every published slot.
        Ok(unsafe { self.values.finish(self.rows) })
    }
}
