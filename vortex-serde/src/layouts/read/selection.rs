use std::cmp::{max, min};
use std::ops::{Range, RangeInclusive};
use std::slice::Iter;
use std::vec::IntoIter;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RowRange {
    pub begin: usize,
    pub end: usize,
}

impl RowRange {
    pub fn new(begin: usize, end: usize) -> Self {
        RowRange { begin, end }
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

/// Sorted list of row ranges to be included when reading vortex files
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RowSelector {
    ranges: Vec<RowRange>,
}

impl RowSelector {
    pub fn new(ranges: Vec<RowRange>) -> Self {
        Self { ranges }
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

        RowSelector::new(new_ranges)
    }
}

impl<I: Into<RowRange>> FromIterator<I> for RowSelector {
    fn from_iter<T: IntoIterator<Item = I>>(iter: T) -> Self {
        RowSelector::new(iter.into_iter().map(Into::into).collect())
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
    #[case(RowSelector::from_iter(vec![0..2, 9..10]), RowSelector::from_iter(vec![0..1]), RowSelector::from_iter(vec![0..1]))]
    #[case(RowSelector::from_iter(vec![5..8, 9..10]), RowSelector::from_iter(vec![2..5]), RowSelector::default())]
    #[case(RowSelector::from_iter(vec![0..4]), RowSelector::from_iter(vec![0..1, 2..3, 3..5]), RowSelector::from_iter(vec![0..1, 2..4]))]
    #[case(RowSelector::from_iter(vec![0..3, 5..6]), RowSelector::from_iter(vec![2..6]), RowSelector::from_iter(vec![2..3, 5..6]))]
    fn intersection(
        #[case] first: RowSelector,
        #[case] second: RowSelector,
        #[case] expected: RowSelector,
    ) {
        assert_eq!(first.intersect(&second), expected);
    }
}
