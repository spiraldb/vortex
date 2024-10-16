use std::mem;

use vortex::array::StructArray;
use vortex::validity::Validity;
use vortex::{Array, IntoArray};
use vortex_dtype::FieldNames;
use vortex_error::{vortex_err, VortexExpect, VortexResult};

use crate::layouts::read::selection::{RowRange, RowSelector};
use crate::layouts::read::{LayoutReader, ReadResult};
use crate::layouts::{Messages, RangeResult, RowFilter};

#[derive(Debug)]
pub struct ColumnBatchReader {
    names: FieldNames,
    children: Vec<Box<dyn LayoutReader>>,
    arrays: Vec<Option<Array>>,
}

impl ColumnBatchReader {
    pub fn new(names: FieldNames, children: Vec<Box<dyn LayoutReader>>) -> Self {
        let arrays = vec![None; children.len()];
        Self {
            names,
            children,
            arrays,
        }
    }

    pub fn read_next(&mut self, selection: RowSelector) -> VortexResult<Option<ReadResult>> {
        let mut messages = Vec::new();
        for (i, child_array) in self
            .arrays
            .iter_mut()
            .enumerate()
            .filter(|(_, a)| a.is_none())
        {
            match self.children[i].read_next(selection.clone())? {
                Some(rr) => match rr {
                    ReadResult::ReadMore(message) => {
                        messages.extend(message);
                    }
                    ReadResult::Batch(a) => *child_array = Some(a),
                },
                None => {
                    debug_assert!(
                        self.arrays.iter().all(Option::is_none),
                        "Expected layout {}({i}) to produce an array but it was empty",
                        self.names[i]
                    );
                    return Ok(None);
                }
            }
        }

        if messages.is_empty() {
            let child_arrays = mem::replace(&mut self.arrays, vec![None; self.children.len()])
                .into_iter()
                .enumerate()
                .map(|(i, a)| a.ok_or_else(|| vortex_err!("Missing child array at index {i}")))
                .collect::<VortexResult<Vec<_>>>()?;
            let len = child_arrays.first().map(|l| l.len()).unwrap_or(0);
            Ok(Some(ReadResult::Batch(
                StructArray::try_new(self.names.clone(), child_arrays, len, Validity::NonNullable)?
                    .into_array(),
            )))
        } else {
            Ok(Some(ReadResult::ReadMore(messages)))
        }
    }

    pub fn advance(&mut self, up_to_row: usize) -> VortexResult<Messages> {
        self.arrays = vec![None; self.children.len()];
        let mut messages = Vec::new();
        for c in self.children.iter_mut() {
            messages.extend(c.advance(up_to_row)?);
        }
        Ok(messages)
    }
}

#[derive(Debug)]
pub struct FilterLayoutReader {
    reader: ColumnBatchReader,
    row_filter: RowFilter,
    row_offset: usize,
    length: usize,
}

impl FilterLayoutReader {
    pub fn new(
        reader: ColumnBatchReader,
        row_filter: RowFilter,
        row_offset: usize,
        length: usize,
    ) -> Self {
        Self {
            reader,
            row_filter,
            row_offset,
            length,
        }
    }

    fn own_range(&self) -> RowSelector {
        RowSelector::new(
            vec![RowRange::new(self.row_offset, self.length)],
            self.length,
        )
    }
}

impl LayoutReader for FilterLayoutReader {
    fn read_next(&mut self, selection: RowSelector) -> VortexResult<Option<ReadResult>> {
        self.reader.read_next(selection)
    }

    fn read_range(&mut self) -> VortexResult<Option<RangeResult>> {
        match self.reader.read_next(self.own_range())? {
            None => Ok(None),
            Some(rr) => match rr {
                ReadResult::ReadMore(m) => Ok(Some(RangeResult::ReadMore(m))),
                ReadResult::Batch(b) => {
                    let filter_result = self.row_filter.evaluate(&b)?;
                    let selector = filter_result.with_dyn(|a| {
                        a.as_bool_array()
                            .ok_or_else(|| vortex_err!("Must be a bool array"))
                            .map(|b| {
                                RowSelector::new(
                                    b.maybe_null_slices_iter()
                                        .map(|(begin, end)| {
                                            RowRange::new(
                                                begin + self.row_offset,
                                                end + self.row_offset,
                                            )
                                        })
                                        .collect(),
                                    self.row_offset + b.len(),
                                )
                            })
                    })?;
                    self.row_offset += b.len();
                    Ok(Some(RangeResult::Range(selector)))
                }
            },
        }
    }

    fn advance(&mut self, up_to_row: usize) -> VortexResult<Messages> {
        self.reader.advance(up_to_row)
    }
}

#[derive(Debug)]
pub struct ColumnBatchFilter {
    children: Vec<Box<dyn LayoutReader>>,
    selectors: Vec<Option<RowSelector>>,
}

impl ColumnBatchFilter {
    pub fn new(children: Vec<Box<dyn LayoutReader>>) -> Self {
        let selectors = vec![None; children.len()];
        Self {
            children,
            selectors,
        }
    }

    pub fn read_more_ranges(&mut self) -> VortexResult<Option<RangeResult>> {
        let mut messages = Vec::new();
        for (i, child_selector) in self
            .selectors
            .iter_mut()
            .enumerate()
            .filter(|(_, a)| a.is_none())
        {
            match self.children[i].read_range()? {
                Some(rr) => match rr {
                    RangeResult::ReadMore(message) => {
                        messages.extend(message);
                    }
                    // TODO(robert): Advance here on empty range by batch_size
                    RangeResult::Range(rs) => *child_selector = Some(rs),
                },
                None => {
                    debug_assert!(
                        self.selectors.iter().all(Option::is_none),
                        "Expected layout to produce an array but it was empty"
                    );
                    return Ok(None);
                }
            }
        }

        if messages.is_empty() {
            let selectors = mem::replace(&mut self.selectors, vec![None; self.children.len()])
                .into_iter()
                .enumerate()
                .map(|(i, a)| a.ok_or_else(|| vortex_err!("Missing child array at index {i}")))
                .collect::<VortexResult<Vec<_>>>()?;
            let mut selector_iter = selectors.into_iter();
            let mut current = selector_iter
                .next()
                .vortex_expect("Must have at least one child");
            for next_filter in selector_iter {
                if current.is_empty() {
                    return self.read_more_ranges();
                }
                current = current.intersect(&next_filter);
            }
            Ok(Some(RangeResult::Range(current)))
        } else {
            Ok(Some(RangeResult::ReadMore(messages)))
        }
    }

    pub fn advance(&mut self, up_to_row: usize) -> VortexResult<Messages> {
        self.selectors = self
            .selectors
            .iter()
            .map(|s| s.as_ref().map(|s| s.advance(up_to_row)))
            .collect();
        let mut messages = Vec::new();
        for c in self.children.iter_mut() {
            messages.extend(c.advance(up_to_row)?);
        }
        Ok(messages)
    }
}
