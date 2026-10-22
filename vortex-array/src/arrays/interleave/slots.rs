// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use super::Interleave;
use crate::ArrayRef;
use crate::array_slots;

/// Slot layout of an interleave: two selectors followed by its value arrays.
#[array_slots(Interleave)]
pub(super) struct InterleaveSlots {
    /// The value-array index for each output row.
    #[slot(0)]
    pub array_indices: ArrayRef,
    /// The row index within the selected value array for each output row.
    #[slot(1)]
    pub row_indices: ArrayRef,
    /// The independent source arrays selected by the two selectors.
    #[slot(2..)]
    pub values: Vec<ArrayRef>,
}
