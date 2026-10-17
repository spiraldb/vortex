// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Fixed-width row initialization, including explicit row counts for zero-width lists.

use std::mem::MaybeUninit;

use crate::{HostResult, OutputBinding, OutputBuffer};
use super::OutputSink;

/// Host publication of fixed-size lists with non-null children.
pub trait ListOutput<T: Copy + Default + 'static>: OutputBinding<T> {
    /// Non-nullable list metadata for this child type and width.
    fn list_type(width: usize) -> HostResult<Self, Self::NativeType>;
    /// Wrap initialized children, retaining explicit row count when width is zero.
    fn finish_list(children: Self::Column, rows: usize, width: usize, ctx: &mut Self::Context)
        -> HostResult<Self, Self::Column>;
}

/// Evidence that the entire fixed-size row supplied to this callback remains initialized.
///
/// ```compile_fail
/// let token = rowfn::sink::InitializedRow(());
/// ```
#[must_use = "return the token from the callback that initialized this row"]
pub struct InitializedRow(());

impl InitializedRow {
    /// Fill every child of the exact callback row, including an empty row.
    ///
    /// # Safety
    /// `row` must be the entire row supplied to this callback, not a subslice or another row.
    /// Return this token and preserve every child's initialization until the callback returns.
    #[inline]
    pub unsafe fn fill<T>(row: &mut [MaybeUninit<T>], mut value: impl FnMut(usize) -> T) -> Self {
        for (index, slot) in row.iter_mut().enumerate() { slot.write(value(index)); }
        Self(())
    }
}

/// Runtime-width output backed by the host's primitive allocation.
pub struct FixedSizeListSink<H: ListOutput<T>, T: Copy + Default + 'static> {
    values: H::Buffer,
    rows: usize,
    width: usize,
}

/// One borrowed view with a row count independent of its number of child elements.
pub struct ListRows<'a, T> {
    values: &'a mut [MaybeUninit<T>],
    rows: usize,
    width: usize,
}

// SAFETY: allocation checks multiplication, and every row maps to its own width-element range.
// Zero-width rows have no elements to alias. Tokens require the entire exact callback row.
unsafe impl<H: ListOutput<T>, T: Copy + Default + 'static> OutputSink<H> for FixedSizeListSink<H, T>
where H::Buffer: 'static {
    type Params = usize;
    type Rows<'a> = ListRows<'a, T>;
    type Row<'a> = &'a mut [MaybeUninit<T>];
    type WriteToken = InitializedRow;
    fn storage_type(width: &usize) -> HostResult<H, H::NativeType> {
        H::list_type(*width)
    }
    fn allocate(rows: usize, width: &usize, ctx: &mut H::Context) -> HostResult<H, Self> {
        let len = rows.checked_mul(*width).ok_or_else(|| H::error("list child count exceeds usize"))?;
        Ok(Self { values: H::allocate(len, ctx)?, rows, width: *width })
    }
    fn rows(&mut self) -> Self::Rows<'_> {
        ListRows { values: &mut self.values.slots()[..self.rows * self.width], rows: self.rows, width: self.width }
    }
    fn len(rows: &Self::Rows<'_>) -> usize { rows.rows }
    fn initialize(rows: &mut Self::Rows<'_>) {
        for row in rows.values.iter_mut() { row.write(T::default()); }
    }
    #[inline]
    unsafe fn row<'a>(rows: &'a mut Self::Rows<'_>, index: usize) -> Self::Row<'a> {
        let start = index * rows.width;
        // SAFETY: allocation checked rows * width, and the caller bounds index by rows.
        unsafe { rows.values.get_unchecked_mut(start..start + rows.width) }
    }
    unsafe fn finish(self, ctx: &mut H::Context) -> HostResult<H, H::Column> {
        // SAFETY: each entire child row is initialized, including skipped placeholders.
        let children = unsafe { self.values.finish(self.rows * self.width) };
        H::finish_list(children, self.rows, self.width, ctx)
    }
}
