// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::ArrayView;
use vortex_array::IntoArray;
use vortex_array::arrays::slice::SliceReduce;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;

use crate::Pco;
use crate::PcoArrayExt;

impl SliceReduce for Pco {
    fn slice(array: ArrayView<'_, Self>, range: Range<usize>) -> VortexResult<Option<ArrayRef>> {
        vortex_ensure!(
            range.start <= range.end && range.end <= array.len(),
            "Pco slice range exceeds array bounds"
        );
        let unsliced_validity = array.unsliced_validity();
        Ok(Some(
            Pco::try_new(
                array.dtype().clone(),
                array._slice(range.start, range.end),
                unsliced_validity,
            )?
            .into_array(),
        ))
    }
}
