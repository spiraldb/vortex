// (c) Copyright 2024 Fulcrum Technologies, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::array::Array;
use crate::error::VortexResult;
use crate::polars::IntoPolarsSeries;
use crate::polars::IntoPolarsValue;
use crate::scalar::Scalar;
use polars_core::prelude::*;
use polars_ops::prelude::*;

pub enum SearchSortedSide {
    Left,
    Right,
}

impl From<SearchSortedSide> for polars_ops::prelude::SearchSortedSide {
    fn from(side: SearchSortedSide) -> Self {
        match side {
            SearchSortedSide::Left => polars_ops::prelude::SearchSortedSide::Left,
            SearchSortedSide::Right => polars_ops::prelude::SearchSortedSide::Right,
        }
    }
}

pub fn search_sorted_usize(
    indices: &dyn Array,
    index: usize,
    side: SearchSortedSide,
) -> VortexResult<usize> {
    let enc_scalar: Box<dyn Scalar> = index.into();
    // Convert index into correctly typed Arrow scalar.
    let enc_scalar = enc_scalar.cast(indices.dtype())?;

    let series: Series = indices.iter_arrow().into_polars();
    Ok(search_sorted(
        &series,
        &Series::from_any_values("needle", &[enc_scalar.into_polars()], true)?,
        side.into(),
        false,
    )?
    .get(0)
    .unwrap() as usize)
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::array::ArrayRef;

    #[test]
    fn test_searchsorted_scalar() {
        let haystack: ArrayRef = vec![1, 2, 3].into();

        assert_eq!(
            search_sorted_usize(haystack.as_ref(), 0, SearchSortedSide::Left).unwrap(),
            0
        );
        assert_eq!(
            search_sorted_usize(haystack.as_ref(), 1, SearchSortedSide::Left).unwrap(),
            0
        );
        assert_eq!(
            search_sorted_usize(haystack.as_ref(), 1, SearchSortedSide::Right).unwrap(),
            1
        );
        assert_eq!(
            search_sorted_usize(haystack.as_ref(), 4, SearchSortedSide::Left).unwrap(),
            3
        );
    }
}
