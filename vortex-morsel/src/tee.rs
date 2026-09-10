// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! The mask buffer: one mask producer, many readers, no shared node.
//!
//! The mask producer (a conjunct or the morsel's own demand) is an ordinary owned child of the
//! root. The mask it produces lives here, in the context, one buffer per tee in the plan. The
//! root pushes pieces in; every filter reads its own rows out with its own cursor. Both sides
//! hold `&mut Cx` when they run, so nothing here is shared or reference-counted.
//!
//! A piece is dropped when every reader that covers it has taken it. Readers register their
//! coverage during look-ahead, before any value is pulled, so a piece knows how many servings it
//! owes the moment it arrives.

use std::ops::Range;

use vortex_error::VortexResult;
use vortex_error::vortex_ensure;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::node::Batch;
use crate::node::Value;

/// The mask of one filtered scan, buffered between its producer and its readers.
#[derive(Debug, Default)]
pub struct MaskBuffer {
    /// Root coordinates of every reader registered this morsel.
    readers: Vec<Range<u64>>,
    /// Root coordinate the next piece must start at; the morsel start before the first piece.
    produced_end: u64,
    /// Whether the producer has nothing more this morsel.
    finished: bool,
    /// Ascending, non-overlapping; a fully served piece leaves a gap.
    pieces: Vec<Piece>,
}

#[derive(Debug)]
struct Piece {
    coverage: Range<u64>,
    mask: Mask,
    /// Rows handed to readers so far, counting each reader's overlap separately.
    served: u64,
    /// Rows owed: the sum of every registered reader's overlap with `coverage`.
    expected: u64,
}

impl MaskBuffer {
    /// An empty mask buffer whose first piece starts at `morsel_start` in root coordinates.
    pub fn new(morsel_start: u64) -> Self {
        Self {
            produced_end: morsel_start,
            ..Self::default()
        }
    }

    /// Declare that a reader will pull `coverage` (root coordinates) this morsel. Readers call
    /// this exactly once per morsel, during the first look-ahead call.
    pub fn register(&mut self, coverage: Range<u64>) {
        for piece in &mut self.pieces {
            piece.expected += overlap(&piece.coverage, &coverage);
        }
        self.readers.push(coverage);
    }

    /// Append the next piece from the producer. Only the root calls this.
    pub fn push(&mut self, piece: Batch) -> VortexResult<()> {
        vortex_ensure!(
            piece.coverage.start == self.produced_end,
            "mask pieces must be contiguous: expected row {}, got {:?}",
            self.produced_end,
            piece.coverage
        );
        let mask = piece.value.into_mask()?;
        vortex_ensure!(
            mask.len() as u64 == piece.coverage.end - piece.coverage.start,
            "mask piece length {} does not match its coverage {:?}",
            mask.len(),
            piece.coverage
        );
        let expected = self
            .readers
            .iter()
            .map(|reader| overlap(&piece.coverage, reader))
            .sum();
        self.produced_end = piece.coverage.end;
        self.pieces.push(Piece {
            coverage: piece.coverage,
            mask,
            served: 0,
            expected,
        });
        Ok(())
    }

    /// The producer has nothing more this morsel.
    pub fn finish(&mut self) {
        self.finished = true;
    }

    /// The first root row the producer has not delivered yet.
    pub fn produced_end(&self) -> u64 {
        self.produced_end
    }

    /// A prefix of `a..b`, clipped to the piece covering `a`.
    ///
    /// `None` means row `a` has not been produced yet. A request for rows below `produced_end`
    /// that finds no piece is a request for rows every reader already took, and is an error.
    pub fn serve(&mut self, a: u64, b: u64) -> VortexResult<Option<Batch>> {
        let Some(index) = self
            .pieces
            .iter()
            .position(|piece| piece.coverage.contains(&a))
        else {
            if a < self.produced_end {
                return Err(vortex_err!(
                    "mask rows from {a} were already served to every reader"
                ));
            }
            if self.finished {
                return Err(vortex_err!(
                    "mask ended at row {} before row {a}",
                    self.produced_end
                ));
            }
            return Ok(None);
        };
        let piece = &mut self.pieces[index];
        let end = b.min(piece.coverage.end);
        let lo = usize::try_from(a - piece.coverage.start)
            .map_err(|_| vortex_err!("mask offset exceeds usize"))?;
        let hi = usize::try_from(end - piece.coverage.start)
            .map_err(|_| vortex_err!("mask offset exceeds usize"))?;
        let mask = if lo == 0 && hi == piece.mask.len() {
            piece.mask.clone()
        } else {
            piece.mask.slice(lo..hi)
        };
        piece.served += end - a;
        debug_assert!(
            piece.served <= piece.expected,
            "a reader pulled mask rows it never registered"
        );
        if piece.served == piece.expected {
            // Every reader that covers these rows has them.
            self.pieces.remove(index);
        }
        Ok(Some(Batch {
            coverage: a..end,
            value: Value::Mask(mask),
        }))
    }
}

fn overlap(left: &Range<u64>, right: &Range<u64>) -> u64 {
    left.end
        .min(right.end)
        .saturating_sub(left.start.max(right.start))
}

#[cfg(test)]
mod tests {
    use vortex_error::VortexExpect;
    use vortex_error::VortexResult;
    use vortex_mask::Mask;

    use super::MaskBuffer;
    use crate::node::Batch;
    use crate::node::Value;

    fn piece(range: std::ops::Range<u64>) -> Batch {
        let len = usize::try_from(range.end - range.start).vortex_expect("test range fits usize");
        Batch {
            coverage: range,
            value: Value::Mask(Mask::from_iter((0..len).map(|row| row % 2 == 0))),
        }
    }

    #[test]
    fn piece_is_dropped_on_the_last_registered_serving() -> VortexResult<()> {
        let mut buffer = MaskBuffer::default();
        buffer.register(0..10);
        buffer.register(0..4);
        buffer.register(4..10);
        buffer.push(piece(0..10))?;

        let first = buffer.serve(0, 10)?.expect("produced");
        assert_eq!(first.coverage, 0..10);
        assert_eq!(buffer.pieces.len(), 1);
        buffer.serve(0, 4)?.expect("produced");
        assert_eq!(buffer.pieces.len(), 1);
        let last = buffer.serve(4, 10)?.expect("produced");
        assert_eq!(last.coverage, 4..10);
        assert!(buffer.pieces.is_empty());
        assert!(buffer.serve(0, 10).is_err(), "rows were already served");
        Ok(())
    }

    #[test]
    fn serve_clips_to_one_piece_and_waits_for_the_rest() -> VortexResult<()> {
        let mut buffer = MaskBuffer::default();
        buffer.register(0..10);
        buffer.push(piece(0..6))?;

        let first = buffer.serve(0, 10)?.expect("produced");
        assert_eq!(first.coverage, 0..6);
        assert!(buffer.serve(6, 10)?.is_none(), "not produced yet");
        buffer.push(piece(6..10))?;
        let second = buffer.serve(6, 10)?.expect("produced");
        assert_eq!(second.coverage, 6..10);
        assert!(buffer.pieces.is_empty());
        Ok(())
    }

    #[test]
    fn readers_registered_after_a_push_still_count() -> VortexResult<()> {
        let mut buffer = MaskBuffer::default();
        buffer.register(0..10);
        buffer.push(piece(0..10))?;
        buffer.register(0..10);
        buffer.serve(0, 10)?.expect("produced");
        assert_eq!(buffer.pieces.len(), 1, "the second reader has not pulled");
        buffer.serve(0, 10)?.expect("produced");
        assert!(buffer.pieces.is_empty());
        Ok(())
    }

    #[test]
    fn rejects_gaps_and_short_producers() -> VortexResult<()> {
        let mut buffer = MaskBuffer::default();
        buffer.register(0..10);
        assert!(
            buffer.push(piece(2..10)).is_err(),
            "pieces must be contiguous"
        );
        buffer.push(piece(0..4))?;
        buffer.finish();
        assert!(buffer.serve(4, 10).is_err(), "the producer ended early");
        Ok(())
    }
}
