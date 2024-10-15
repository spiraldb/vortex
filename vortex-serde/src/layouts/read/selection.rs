use std::cmp::{max, min};
use std::ops::{Range, RangeInclusive};
use std::slice::Iter;
use std::vec::IntoIter;

use vortex::array::ChunkedArray;
use vortex::compute::slice;
use vortex::{Array, ArrayDType, IntoArray, IntoArrayVariant};
use vortex_error::VortexResult;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RowRange {
    pub begin: usize,
    pub end: usize,
}

impl RowRange {
    pub fn new(begin: usize, end: usize) -> Self {
        RowRange { begin, end }
    }

    pub fn len(&self) -> usize {
        self.end - self.begin
    }
}

impl From<Range<usize>> for RowRange {
    fn from(value: Range<usize>) -> Self {
        RowRange::new(value.start, value.end)
    }
}

impl From<RangeInclusive<usize>> for RowRange {
    fn from(value: RangeInclusive<usize>) -> Self {
        RowRange::new(*value.start(), value.end() + 1)
    }
}

impl From<(usize, usize)> for RowRange {
    fn from(value: (usize, usize)) -> Self {
        RowRange::new(value.0, value.1)
    }
}

/// Sorted list of row ranges to be included when reading vortex files
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowSelector {
    ranges: Vec<RowRange>,
    length: usize,
}

impl RowSelector {
    pub fn from_ranges(
        ranges: impl IntoIterator<Item = impl Into<RowRange>>,
        length: usize,
    ) -> Self {
        Self {
            ranges: ranges.into_iter().map(Into::into).collect(),
            length,
        }
    }

    pub fn new(ranges: Vec<RowRange>, length: usize) -> Self {
        Self { ranges, length }
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    pub fn length(&self) -> usize {
        self.length
    }

    pub fn iter(&self) -> Iter<RowRange> {
        self.ranges.iter()
    }

    pub fn intersect(&self, other: &RowSelector) -> Self {
        if self.ranges.is_empty() || other.ranges.is_empty() {
            return RowSelector::default();
        }

        let mut new_ranges: Vec<RowRange> = Vec::new();
        let mut i = 0;
        let mut j = 0;
        while i < self.ranges.len() && j < other.ranges.len() {
            let own_range = self.ranges[i];
            let other_range = other.ranges[j];

            if own_range.end > other_range.begin && other_range.end > own_range.begin {
                let new_end = min(own_range.end, other_range.end);
                let new_begin = max(own_range.begin, other_range.begin);
                new_ranges
                    .last_mut()
                    .filter(|range| range.end >= new_begin)
                    .map(|range| {
                        range.end = new_end;
                    })
                    .unwrap_or_else(|| new_ranges.push(RowRange::new(new_begin, new_end)));
            }

            if own_range.end < other_range.end {
                i += 1;
            } else {
                j += 1;
            }
        }

        RowSelector::new(new_ranges, min(self.length, other.length))
    }

    pub fn slice_array(&self, array: impl AsRef<Array>) -> VortexResult<Option<Array>> {
        let array = array.as_ref();
        let mut chunks = self
            .ranges
            .iter()
            .filter(|r| r.begin < array.len())
            .map(|r| RowRange::new(r.begin, min(r.end, array.len())))
            .map(|r| slice(array, r.begin, r.end))
            .collect::<VortexResult<Vec<_>>>()?;
        match chunks.len() {
            0 | 1 => Ok(chunks.pop()),
            _ => {
                let dtype = chunks[0].dtype().clone();
                ChunkedArray::try_new(chunks, dtype)
                    .and_then(|c| c.into_bool())
                    .map(IntoArray::into_array)
                    .map(Some)
            }
        }
    }

    pub fn offset(&self, offset: usize) -> RowSelector {
        RowSelector::new(
            self.ranges
                .iter()
                .filter(|rr| rr.end > offset)
                .map(|rr| {
                    RowRange::new(
                        if offset > rr.begin {
                            0
                        } else {
                            rr.begin - offset
                        },
                        rr.end - offset,
                    )
                })
                .collect(),
            self.length - offset,
        )
    }

    pub fn advance(&self, up_to_row: usize) -> RowSelector {
        RowSelector::new(
            self.ranges
                .iter()
                .filter(|rr| rr.end > up_to_row)
                .map(|rr| RowRange::new(max(up_to_row, rr.begin), rr.end))
                .collect(),
            self.length,
        )
    }
}

impl Extend<RowRange> for RowSelector {
    fn extend<T: IntoIterator<Item = RowRange>>(&mut self, iter: T) {
        self.ranges.extend(iter)
    }
}

impl Extend<RowSelector> for RowSelector {
    fn extend<T: IntoIterator<Item = RowSelector>>(&mut self, iter: T) {
        for r in iter {
            self.ranges.extend(r)
        }
    }
}

impl IntoIterator for RowSelector {
    type Item = RowRange;
    type IntoIter = IntoIter<RowRange>;

    fn into_iter(self) -> Self::IntoIter {
        self.ranges.into_iter()
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use rstest::rstest;

    use crate::layouts::read::selection::RowSelector;

    #[rstest]
    #[case(RowSelector::from_ranges(vec![0..2, 9..10], 10), RowSelector::from_ranges(vec![0..1], 10), RowSelector::from_ranges(vec![0..1], 10))]
    #[case(RowSelector::from_ranges(vec![5..8, 9..10], 10), RowSelector::from_ranges(vec![2..5], 10), RowSelector::new(vec![], 10))]
    #[case(RowSelector::from_ranges(vec![0..4], 10), RowSelector::from_ranges(vec![0..1, 2..3, 3..5], 10), RowSelector::from_ranges(vec![0..1, 2..4], 10))]
    #[case(RowSelector::from_ranges(vec![0..3, 5..6], 10), RowSelector::from_ranges(vec![2..6], 10), RowSelector::from_ranges(vec![2..3, 5..6], 10))]
    fn intersection(
        #[case] first: RowSelector,
        #[case] second: RowSelector,
        #[case] expected: RowSelector,
    ) {
        assert_eq!(first.intersect(&second), expected);
    }
}
