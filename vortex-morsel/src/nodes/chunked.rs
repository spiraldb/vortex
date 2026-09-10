// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::ops::Range;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::dtype::DType;
use vortex_error::VortexExpect;
use vortex_error::VortexResult;
use vortex_error::vortex_err;
use vortex_mask::Mask;

use crate::demand::RowDomain;
use crate::node::Batch;
use crate::node::Child;
use crate::node::Cx;
use crate::node::LookAhead;
use crate::node::Operator;
use crate::node::Step;
use crate::node::concat_parts;

/// One overlap between the morsel's range and a chunk.
#[derive(Clone, Debug)]
struct Cut {
    chunk: usize,
    /// The slice of the hint that covers this overlap.
    mask_range: Range<usize>,
}

/// Chunked has no runtime existence beyond cutting: it turns one range into per-chunk ranges and
/// wraps the children's outputs back up in chunk order.
///
/// The cut is `partition_point` plus a walk of the overlapping chunks — chunks outside the
/// morsel are arithmetic that never ran, not objects that were created and discarded. Look-ahead visits all cut children even if some block; completed children are skipped on
/// subsequent calls. Execution keeps its own cut cursor.
pub struct ChunkedExec {
    /// Cumulative chunk offsets, ending at the row count.
    chunk_offsets: Arc<[u64]>,
    /// One slot per chunk; `None` where a range-scoped plan left the chunk unmaterialized.
    children: Vec<Option<Child>>,
    dtype: DType,

    domain: RowDomain,
    cuts: Vec<Cut>,
    state: State,
}

enum State {
    Pulling { cut: usize, parts: Vec<ArrayRef> },
    Emitted,
}

impl ChunkedExec {
    /// Build a chunked operator from cumulative chunk offsets and one child per chunk.
    pub fn new(
        chunk_offsets: Arc<[u64]>,
        children: Vec<Option<Child>>,
        dtype: DType,
        domain: RowDomain,
    ) -> VortexResult<Self> {
        debug_assert_eq!(chunk_offsets.len(), children.len() + 1);
        let mut op = Self {
            chunk_offsets,
            children,
            dtype,
            domain,
            cuts: Vec::new(),
            state: State::Pulling {
                cut: 0,
                parts: Vec::new(),
            },
        };
        op.cut()?;
        Ok(op)
    }

    fn cut(&mut self) -> VortexResult<()> {
        if self.domain.range().is_empty() {
            return Ok(());
        }

        let offsets = &self.chunk_offsets;
        let first = offsets
            .partition_point(|&offset| offset <= self.domain.range().start)
            .saturating_sub(1);
        let mut mask_start = 0usize;
        for chunk in first..offsets.len().saturating_sub(1) {
            let chunk_start = offsets[chunk];
            let chunk_end = offsets[chunk + 1];
            if chunk_start >= self.domain.range().end {
                break;
            }
            let overlap_start = self.domain.range().start.max(chunk_start);
            let overlap_end = self.domain.range().end.min(chunk_end);
            if overlap_start >= overlap_end {
                continue;
            }
            let len = usize::try_from(overlap_end - overlap_start)
                .vortex_expect("chunk overlap fits usize");
            if self.children[chunk].is_none() {
                return Err(vortex_err!(
                    "morsel range {:?} reaches chunk {chunk}, which was not materialized in the plan",
                    self.domain.range()
                ));
            }
            self.cuts.push(Cut {
                chunk,
                mask_range: mask_start..mask_start + len,
            });
            mask_start += len;
        }
        Ok(())
    }
}

impl Operator for ChunkedExec {
    fn row_domain(&self) -> &RowDomain {
        &self.domain
    }

    fn look_ahead(&mut self, cx: &mut Cx<'_>) -> VortexResult<LookAhead> {
        let mut result = LookAhead::Complete;
        for cut in &self.cuts {
            let child = self.children[cut.chunk]
                .as_mut()
                .vortex_expect("cut chunk is built");
            result = result.merge(child.look_ahead(cx)?);
        }
        Ok(result)
    }

    fn next(&mut self, hint: &Mask, cx: &mut Cx<'_>) -> VortexResult<Step> {
        match &mut self.state {
            State::Emitted => Ok(Step::Finished),
            State::Pulling { cut, parts } => {
                // Every cut is pulled with its slice of the parent's hint. A cut nobody wants
                // comes back as placeholder rows from its leaf, or empty from the filter above
                // it, so the concatenation is consistent without this operator knowing which
                // chunks were read.
                while *cut < self.cuts.len() {
                    let c = &self.cuts[*cut];
                    let child_hint = slice_mask(hint, c.mask_range.clone());
                    let child = self.children[c.chunk]
                        .as_mut()
                        .vortex_expect("cut chunk is built");
                    match child.next(&child_hint, cx)? {
                        Step::Batch(batch) => {
                            let array = batch.value.into_array()?;
                            if !array.is_empty() {
                                parts.push(array);
                            }
                            *cut += 1;
                        }
                        Step::Blocked => return Ok(Step::Blocked),
                        Step::Finished => {
                            return Err(vortex_err!("chunk {} produced no value", c.chunk));
                        }
                    }
                }
                let array = concat_parts(std::mem::take(parts), &self.dtype)?;
                self.state = State::Emitted;
                Ok(Step::Batch(Batch::array(
                    self.domain.range().clone(),
                    array,
                )))
            }
        }
    }

    fn close(&mut self, cx: &mut Cx<'_>) {
        for child in self.children.iter_mut().flatten() {
            child.close(cx);
        }
        self.state = State::Emitted;
    }

    fn describe(&self) -> String {
        let offsets: Vec<String> = self.chunk_offsets.iter().map(u64::to_string).collect();
        format!(
            "Chunked({} chunks at {})",
            self.chunk_offsets.len().saturating_sub(1),
            offsets.join(" ")
        )
    }
}

/// Slice a mask, preserving the all-true / all-false fast paths.
pub(crate) fn slice_mask(mask: &Mask, range: Range<usize>) -> Mask {
    if range.start == 0 && range.end == mask.len() {
        return mask.clone();
    }
    mask.slice(range)
}
