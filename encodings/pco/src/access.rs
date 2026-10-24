// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Operation-owned selected-page values and validity. No decoded state lives in array metadata.

use std::collections::BTreeMap;

use pco::data_types::NumberType;
use pco::match_number_enum;
use vortex_array::ArrayView;
use vortex_array::ExecutionCtx;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::half;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::Pco;
use crate::PcoArrayExt;
use crate::array::number_type_from_ptype;

const RANK_STRIDE: usize = 512;

#[derive(Default)]
pub(crate) struct AccessState {
    legacy: Option<RankMask>,
    pages: BTreeMap<usize, CachedPage>,
}

struct CachedPage {
    mask: Option<RankMask>,
    decoded: Option<PrimitiveArray>,
}

struct RankMask {
    mask: Mask,
    ranks: Vec<usize>,
}

impl RankMask {
    fn new(mask: Mask) -> Self {
        let ranks = match &mask {
            Mask::Values(values) => {
                let bits = values.bit_buffer();
                let mut ranks = Vec::with_capacity(bits.len().div_ceil(RANK_STRIDE));
                let mut count = 0;
                for start in (0..bits.len()).step_by(RANK_STRIDE) {
                    ranks.push(count);
                    count += bits.count_range(start, (start + RANK_STRIDE).min(bits.len()));
                }
                ranks
            }
            _ => Vec::new(),
        };
        Self { mask, ranks }
    }

    fn value_index(&self, row: usize) -> Option<usize> {
        match &self.mask {
            Mask::AllTrue(_) => Some(row),
            Mask::AllFalse(_) => None,
            Mask::Values(values) => {
                let bits = values.bit_buffer();
                bits.value(row).then(|| {
                    let block = row / RANK_STRIDE;
                    self.ranks[block] + bits.count_range(block * RANK_STRIDE, row)
                })
            }
        }
    }
}

impl AccessState {
    /// Resolve an unsliced logical row to a physical page and compact within-page index.
    pub fn resolve(
        &mut self,
        array: ArrayView<'_, Pco>,
        row: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<(usize, usize)>> {
        vortex_ensure!(
            row < array.unsliced_n_rows(),
            "Pco row exceeds array bounds"
        );
        let index = array.page_index()?;
        if index.logical && !index.pages.is_empty() {
            let page_idx = index
                .pages
                .partition_point(|page| page.rows.as_ref().is_some_and(|range| range.end <= row));
            let page = index
                .pages
                .get(page_idx)
                .ok_or_else(|| vortex_err!("Pco logical page index does not cover row"))?;
            let rows = page
                .rows
                .as_ref()
                .ok_or_else(|| vortex_err!("Pco logical page range is missing"))?;
            if !self.pages.contains_key(&page_idx) {
                let mask = array
                    .unsliced_validity()
                    .slice(rows.clone())?
                    .execute_mask(rows.len(), ctx)?;
                vortex_ensure_eq!(
                    mask.true_count(),
                    page.values.len(),
                    "Pco logical page validity count does not match stored values"
                );
                self.pages.insert(
                    page_idx,
                    CachedPage {
                        mask: Some(RankMask::new(mask)),
                        decoded: None,
                    },
                );
            }
            return Ok(self.pages[&page_idx]
                .mask
                .as_ref()
                .and_then(|mask| mask.value_index(row - rows.start))
                .map(|value| (page_idx, value)));
        }

        // Legacy or unknown optional metadata: execute/rank the full validity once per operation,
        // never once per requested index. Empty physical arrays also take this checked path.
        if self.legacy.is_none() {
            self.legacy = Some(RankMask::new(
                array
                    .unsliced_validity()
                    .execute_mask(array.unsliced_n_rows(), ctx)?,
            ));
        }
        let Some(value) = self.legacy.as_ref().and_then(|mask| mask.value_index(row)) else {
            return Ok(None);
        };
        let page_idx = index.pages.partition_point(|page| page.values.end <= value);
        let page = index.pages.get(page_idx).ok_or_else(|| {
            vortex_err!("Pco validity contains more valid rows than stored values")
        })?;
        self.pages.entry(page_idx).or_insert(CachedPage {
            mask: None,
            decoded: None,
        });
        Ok(Some((page_idx, value - page.values.start)))
    }

    pub fn values(
        &mut self,
        array: ArrayView<'_, Pco>,
        page_idx: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<&PrimitiveArray> {
        let cached = self
            .pages
            .get_mut(&page_idx)
            .ok_or_else(|| vortex_err!("Pco page must be resolved before decoding"))?;
        if cached.decoded.is_none() {
            let index = array.page_index()?;
            let page = index
                .pages
                .get(page_idx)
                .ok_or_else(|| vortex_err!("Pco page is missing"))?;
            let buffer = array
                .pages
                .get(page_idx)
                .ok_or_else(|| vortex_err!("Pco compressed page is missing"))?;
            let decoded = match_number_enum!(
                number_type_from_ptype(array.dtype().as_ptype()),
                NumberType<T> => {
                    crate::probe::decode_page::<T>(array, page.chunk, page.values.len(), buffer.as_slice(), ctx)?
                }
            );
            cached.decoded = Some(decoded);
        }
        cached
            .decoded
            .as_ref()
            .ok_or_else(|| vortex_err!("Pco page decoding did not produce values"))
    }
}
