// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Row domains and live demand views attached to I/O uses.

use std::ops::Range;
use std::sync::Arc;

use parking_lot::RwLock;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_mask::Mask;

/// A stable view of the demand for a range of rows.
///
/// Clones and slices observe later refinements of the same mask. A request retains this handle
/// instead of freezing the mask when it is registered. Rows use the owning operator's coordinates.
#[derive(Clone, Debug)]
pub struct DemandRef {
    rows: Range<u64>,
    offset: usize,
    mask: Arc<RwLock<Mask>>,
}

impl DemandRef {
    /// The rows covered by this view, in the owning operator's coordinates.
    pub fn range(&self) -> &Range<u64> {
        &self.rows
    }

    /// The current selection over this view's rows.
    pub fn snapshot(&self) -> Mask {
        let mask = self.mask.read();
        mask.slice(self.offset..self.offset + self.len())
    }

    fn len(&self) -> usize {
        // Every view is a slice of a mask, whose length already fits usize.
        usize::try_from(self.rows.end - self.rows.start)
            .vortex_expect("demand view length fits its backing mask")
    }

    /// Narrow this view's demand. Rows already ruled out cannot become wanted again.
    pub fn refine(&self, keep: &Mask) -> VortexResult<()> {
        vortex_ensure!(
            keep.len() == self.len(),
            "demand length does not match its row domain"
        );
        if keep.all_true() {
            return Ok(());
        }
        let mut mask = self.mask.write();
        let end = self.offset + self.len();
        let refined = &mask.slice(self.offset..end) & keep;
        *mask = if self.offset == 0 && end == mask.len() {
            refined
        } else {
            Mask::concat(
                [
                    mask.slice(0..self.offset),
                    refined,
                    mask.slice(end..mask.len()),
                ]
                .iter(),
            )?
        };
        Ok(())
    }
}

/// An operator's row range and the live demand for those rows, supplied at construction.
///
/// Row-preserving children share the domain. Chunk children take a slice and rebase its row
/// coordinates; dictionary values have a separate domain from their codes.
#[derive(Clone, Debug)]
pub struct RowDomain {
    demand: DemandRef,
}

impl RowDomain {
    /// Create a domain with one demand entry per row.
    pub fn new(rows: Range<u64>, demand: Mask) -> VortexResult<Self> {
        vortex_ensure!(
            rows.end.checked_sub(rows.start) == Some(demand.len() as u64),
            "row domain {rows:?} does not match demand length {}",
            demand.len()
        );
        Ok(Self {
            demand: DemandRef {
                rows,
                offset: 0,
                mask: Arc::new(RwLock::new(demand)),
            },
        })
    }

    /// The operator's rows in its local coordinates.
    pub fn range(&self) -> &Range<u64> {
        self.demand.range()
    }

    /// A live demand view for a range inside this domain.
    pub fn demand(&self, rows: Range<u64>) -> VortexResult<DemandRef> {
        vortex_ensure!(
            self.range().start <= rows.start
                && rows.start <= rows.end
                && rows.end <= self.range().end,
            "demand range {rows:?} lies outside row domain {:?}",
            self.range()
        );
        Ok(DemandRef {
            offset: self.demand.offset
                + usize::try_from(rows.start - self.range().start)
                    .vortex_expect("demand view offset fits its parent mask"),
            rows,
            mask: Arc::clone(&self.demand.mask),
        })
    }

    /// The current demand over this whole domain.
    pub fn snapshot(&self) -> Mask {
        self.demand.snapshot()
    }

    /// Narrow the demand over this whole domain.
    pub fn refine(&self, keep: &Mask) -> VortexResult<()> {
        self.demand.refine(keep)
    }

    /// A child domain that shares the demand for these rows.
    pub fn slice(&self, rows: Range<u64>) -> VortexResult<Self> {
        Ok(Self {
            demand: self.demand(rows)?,
        })
    }

    /// Express the same rows starting at `start` in a child's coordinate system.
    pub fn rebase(mut self, start: u64) -> VortexResult<Self> {
        let end = start
            .checked_add(self.range().end - self.range().start)
            .ok_or_else(|| vortex_err!("row domain overflows u64"))?;
        self.demand.rows = start..end;
        Ok(self)
    }
}
