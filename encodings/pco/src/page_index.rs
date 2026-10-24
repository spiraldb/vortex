// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Immutable physical-page descriptors shared by sliced owners and access operations.

use std::ops::Range;

use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_ensure_eq;
use vortex_error::vortex_err;

use crate::PcoMetadata;

#[derive(Debug)]
pub(crate) struct Page {
    pub chunk: usize,
    pub values: Range<usize>,
    pub rows: Option<Range<usize>>,
}

#[derive(Debug)]
pub(crate) struct PageIndex {
    pub pages: Vec<Page>,
    pub logical: bool,
}

impl PageIndex {
    pub fn try_new(metadata: &PcoMetadata, rows: usize) -> VortexResult<Self> {
        let logical = metadata
            .logical_pages
            .as_ref()
            .filter(|index| index.version == 1);
        let page_count = metadata.chunks.iter().try_fold(0usize, |count, chunk| {
            count
                .checked_add(chunk.pages.len())
                .ok_or_else(|| vortex_err!("Pco page count overflows usize"))
        })?;
        if let Some(index) = logical {
            vortex_ensure_eq!(
                index.ends.len(),
                page_count,
                "Pco logical page count does not match physical pages"
            );
        }
        let mut pages = Vec::with_capacity(page_count);
        let mut value_start = 0usize;
        let mut row_start = 0usize;
        for (chunk, info) in metadata.chunks.iter().enumerate() {
            for page in &info.pages {
                let n = page.n_values as usize;
                vortex_ensure!(n != 0, "Pco contains an empty physical page");
                let end = value_start
                    .checked_add(n)
                    .ok_or_else(|| vortex_err!("Pco value prefix overflows usize"))?;
                vortex_ensure!(end <= rows, "Pco value prefix exceeds logical row count");
                let logical_rows = if let Some(index) = logical {
                    let stop = usize::try_from(index.ends[pages.len()])?;
                    vortex_ensure!(
                        stop > row_start && stop <= rows,
                        "Pco logical page ends are not strictly increasing within array bounds"
                    );
                    vortex_ensure!(
                        stop - row_start >= n,
                        "Pco page has fewer logical rows than values"
                    );
                    let range = row_start..stop;
                    row_start = stop;
                    Some(range)
                } else {
                    None
                };
                pages.push(Page {
                    chunk,
                    values: value_start..end,
                    rows: logical_rows,
                });
                value_start = end;
            }
        }
        if logical.is_some() && page_count != 0 {
            vortex_ensure_eq!(
                row_start,
                rows,
                "Pco logical page ends do not cover all rows"
            );
        }
        Ok(Self {
            pages,
            logical: logical.is_some(),
        })
    }
}
