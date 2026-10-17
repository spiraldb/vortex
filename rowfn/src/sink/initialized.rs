// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Initialized scalar output for callbacks that need a safe mutable row.
//!
//! Every slot contains a default value before a callback runs. Safe callbacks cannot remove that
//! initialization, so success needs no initialization token. The uninitialized sink remains useful
//! when avoiding this initial fill justifies its stricter callback contract.

use std::slice;

use crate::HostResult;
use crate::OutputBinding;
use crate::OutputBuffer;

use super::OutputSink;

/// An initialized scalar sink that lends safe mutable values to row callbacks.
///
/// Allocation writes [`Default::default`] into every output slot. A callback can replace its value
/// and return `()` or a `Result<(), H::Error>`. Successful callbacks that do not write leave the
/// default value. Values that require destruction are unsupported, as with owned output.
pub struct ElementSink<H: OutputBinding<T>, T: Default + 'static> {
    values: H::Buffer,
    rows: usize,
}

// SAFETY: allocation initializes every slot before it can be borrowed as T. OutputBuffer preserves
// that initialization across views and moves. Safe mutable T access cannot deinitialize a slot or
// alter another row. Partial abandonment is safe, and finish publishes only the initialized prefix.
unsafe impl<H: OutputBinding<T>, T: Default + 'static> OutputSink<H> for ElementSink<H, T> {
    type Params = ();
    type Rows<'a> = &'a mut [T];
    type Row<'a> = &'a mut T;
    type WriteToken = ();

    fn storage_type(_: &()) -> HostResult<H, H::NativeType> {
        const {
            assert!(!std::mem::needs_drop::<T>());
        }

        Ok(H::output_type())
    }

    fn allocate(rows: usize, _: &(), ctx: &mut H::Context) -> HostResult<H, Self> {
        const {
            assert!(!std::mem::needs_drop::<T>());
        }
        let mut values = H::allocate(rows, ctx)?;

        for slot in &mut values.slots()[..rows] {
            slot.write(T::default());
        }

        Ok(Self { values, rows })
    }

    fn rows(&mut self) -> Self::Rows<'_> {
        let slots = &mut self.values.slots()[..self.rows];
        // SAFETY: allocation initialized this prefix. OutputBuffer retains its slot mapping and
        // contents, and previous safe row borrows cannot remove initialization. MaybeUninit<T>
        // has T's layout, and this exclusive slice borrow covers exactly the initialized prefix.
        unsafe { slice::from_raw_parts_mut(slots.as_mut_ptr().cast::<T>(), slots.len()) }
    }

    fn len(rows: &Self::Rows<'_>) -> usize {
        rows.len()
    }

    fn initialize(_: &mut Self::Rows<'_>) {}

    unsafe fn row<'a>(rows: &'a mut Self::Rows<'_>, index: usize) -> Self::Row<'a> {
        // SAFETY: the caller bounds index by this retained slice length.
        unsafe { rows.get_unchecked_mut(index) }
    }

    unsafe fn finish(self, _: &mut H::Context) -> HostResult<H, H::Column> {
        // SAFETY: every slot in this prefix was initialized at allocation and stays initialized.
        Ok(unsafe { self.values.finish(self.rows) })
    }
}
