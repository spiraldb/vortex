// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Optional native bindings for the portable function domains.
//!
//! The traits and timestamp row kind belong to this crate, so these implementations satisfy
//! Rust's orphan rules without a new host wrapper or a mandatory function catalog in either host.

use rowfn::RowView;

use crate::TimestampTicks;

#[cfg(feature = "arrow")]
mod arrow;

#[cfg(feature = "vortex")]
mod vortex;

/// A borrowed tick slice with timestamp semantics established by its host binding.
pub struct TicksView<'a>(&'a [i64]);

// SAFETY: the retained slice has stable initialized values at every index below its length.
unsafe impl<'a> RowView<'a, TimestampTicks> for TicksView<'a> {
    fn len(&self) -> usize {
        self.0.len()
    }

    unsafe fn get_unchecked(&self, index: usize) -> i64 {
        // SAFETY: the caller bounds index by this retained slice length.
        unsafe { *self.0.get_unchecked(index) }
    }
}
