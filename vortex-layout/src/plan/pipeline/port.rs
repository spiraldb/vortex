// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Ports: queues of batches with one sink and one source, kept in an arena the scan owns so a
//! port can outlive the pipeline or split at either end.
//!
//! A port has a consumer for as long as anything can be pushed into it: its source goes only
//! once the sink has closed the port and the source has drained it, or once the port is
//! detached from its sink's outlets, so no push ever reaches a port nobody will read.

use std::collections::VecDeque;

use vortex_array::ArrayRef;

use crate::plan::pipeline::DEFAULT_CAPACITY;

/// Identifies a port in the scan's arena.
pub(crate) type PortId = usize;

/// Identifies a pipeline in the scan's table.
pub(crate) type PipelineId = usize;

/// The source end of a port: who takes its batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SourceEnd {
    /// Nobody yet: a share's port waiting for the pipeline it was made for, or a port whose
    /// source has gone.
    Unclaimed,
    /// The source of this pipeline, which reads the port as one of its inlets.
    Pipeline(PipelineId),
    /// The scan, reading the output of the split's stage running in this slot.
    Scan(usize),
}

/// A queue of batches with one sink and one source.
///
/// Its buffer is allocated once, for the capacity. Only an unbounded port, a share's waiting for a
/// source compiled later, grows past the default.
pub(crate) struct Queue {
    batches: VecDeque<ArrayRef>,
    /// Batches the queue may hold before its sink is blocked.
    capacity: usize,
    /// The sink will push nothing more.
    closed: bool,
    /// The pipeline whose sink pushes into this port; none once closed.
    pub(crate) sink: Option<PipelineId>,
    pub(crate) source: SourceEnd,
    /// The source will take nothing more. Set only once nothing more can be pushed, so the
    /// slot is freed as soon as the port is also closed.
    source_gone: bool,
    /// The slot is free for reuse.
    free: bool,
}

impl Queue {
    fn new(capacity: usize, sink: Option<PipelineId>, source: SourceEnd) -> Self {
        Self {
            batches: VecDeque::with_capacity(capacity.clamp(1, DEFAULT_CAPACITY)),
            capacity: capacity.max(1),
            closed: false,
            sink,
            source,
            source_gone: false,
            free: false,
        }
    }

    pub(crate) fn has_room(&self) -> bool {
        self.batches.len() < self.capacity
    }

    pub(crate) fn closed(&self) -> bool {
        self.closed
    }

    /// Whether the sink is never blocked by it.
    pub(crate) fn unbounded(&self) -> bool {
        self.capacity == usize::MAX
    }

    pub(crate) fn len(&self) -> usize {
        self.batches.len()
    }

    pub(crate) fn is_free(&self) -> bool {
        self.free
    }

    /// Whether the source would find something new: a batch, or the close.
    pub(crate) fn readable(&self) -> bool {
        !self.batches.is_empty() || self.closed
    }

    pub(crate) fn pop(&mut self) -> Option<ArrayRef> {
        self.batches.pop_front()
    }
}

/// The reading side of a port, borrowed for one call.
pub struct Inlet<'a> {
    queue: &'a mut Queue,
}

impl Inlet<'_> {
    /// The oldest batch, in place, so the source can slice it down without removing it.
    pub fn peek_mut(&mut self) -> Option<&mut ArrayRef> {
        self.queue.batches.front_mut()
    }

    /// The oldest batch, removed. Removing a batch is what frees its slot.
    pub fn take(&mut self) -> Option<ArrayRef> {
        self.queue.batches.pop_front()
    }

    /// Whether the sink has finished. Closed and empty is the end of the inlet.
    pub fn closed(&self) -> bool {
        self.queue.closed
    }

    /// Whether no batch is queued.
    pub fn is_empty(&self) -> bool {
        self.queue.batches.is_empty()
    }

    /// Batches queued.
    pub fn len(&self) -> usize {
        self.queue.batches.len()
    }

    /// Whether the sink can push no more until the source takes a batch. An unbounded inlet
    /// counts as full at the default capacity, so a source waiting for it to fill still runs.
    pub fn full(&self) -> bool {
        self.queue.batches.len() >= self.queue.capacity.min(DEFAULT_CAPACITY)
    }

    /// Rows queued, over every batch.
    pub fn rows(&self) -> usize {
        self.queue.batches.iter().map(|batch| batch.len()).sum()
    }
}

/// A growable table that never moves its entries: entries live in blocks of fixed size, so
/// adding one never copies the others, as a vector's growth does.
pub(crate) struct Slab<T> {
    blocks: Vec<Vec<T>>,
    len: usize,
}

const BLOCK: usize = 64;

impl<T> Default for Slab<T> {
    fn default() -> Self {
        Self {
            blocks: Vec::new(),
            len: 0,
        }
    }
}

impl<T> Slab<T> {
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Adds `value`, returning its index.
    pub(crate) fn push(&mut self, value: T) -> usize {
        if self.len.is_multiple_of(BLOCK) {
            self.blocks.push(Vec::with_capacity(BLOCK));
        }
        let index = self.len;
        self.blocks[index / BLOCK].push(value);
        self.len += 1;
        index
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> {
        self.blocks.iter().flatten()
    }
}

impl<T> std::ops::Index<usize> for Slab<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        &self.blocks[index / BLOCK][index % BLOCK]
    }
}

impl<T> std::ops::IndexMut<usize> for Slab<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        &mut self.blocks[index / BLOCK][index % BLOCK]
    }
}

/// Every port of a scan. Slots are reused once a port's sink has closed it and its source has
/// gone, so no pipeline still refers to it.
#[derive(Default)]
pub(crate) struct Arena {
    queues: Slab<Queue>,
    free: Vec<PortId>,
}

impl Arena {
    pub(crate) fn create(
        &mut self,
        capacity: usize,
        sink: Option<PipelineId>,
        source: SourceEnd,
    ) -> PortId {
        match self.free.pop() {
            Some(id) => {
                // A freed slot keeps its buffer, so reusing it allocates only for a larger port.
                let queue = &mut self.queues[id];
                let capacity = capacity.max(1);
                queue.batches.reserve_exact(capacity.min(DEFAULT_CAPACITY));
                queue.capacity = capacity;
                queue.closed = false;
                queue.sink = sink;
                queue.source = source;
                queue.source_gone = false;
                queue.free = false;
                id
            }
            None => self.queues.push(Queue::new(capacity, sink, source)),
        }
    }

    pub(crate) fn get(&self, id: PortId) -> &Queue {
        &self.queues[id]
    }

    pub(crate) fn get_mut(&mut self, id: PortId) -> &mut Queue {
        &mut self.queues[id]
    }

    pub(crate) fn inlet(&mut self, id: PortId) -> Inlet<'_> {
        Inlet {
            queue: &mut self.queues[id],
        }
    }

    pub(crate) fn has_room(&self, id: PortId) -> bool {
        self.queues[id].has_room()
    }

    /// Appends a batch. Empty batches are dropped.
    pub(crate) fn push(&mut self, id: PortId, batch: ArrayRef) {
        let queue = &mut self.queues[id];
        debug_assert!(
            !queue.source_gone && !queue.closed,
            "push into port {id} with no consumer"
        );
        if !batch.is_empty() {
            queue.batches.push_back(batch);
        }
    }

    /// The sink will push nothing more.
    pub(crate) fn close(&mut self, id: PortId) {
        let queue = &mut self.queues[id];
        queue.closed = true;
        queue.sink = None;
        self.maybe_free(id);
    }

    /// The source will take nothing more: what is queued is dropped. Nothing more may be
    /// pushed, so the sink must have closed the port or be about to, as a retiring pipeline's
    /// inlets' sinks have.
    pub(crate) fn drop_source(&mut self, id: PortId) {
        let queue = &mut self.queues[id];
        queue.source_gone = true;
        queue.source = SourceEnd::Unclaimed;
        queue.batches.clear();
        self.maybe_free(id);
    }

    fn maybe_free(&mut self, id: PortId) {
        let queue = &mut self.queues[id];
        if queue.closed && queue.source_gone && !queue.free {
            queue.free = true;
            queue.batches.clear();
            self.free.push(id);
        }
    }

    /// Every port slot, free or not, with its id.
    #[cfg(debug_assertions)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = (PortId, &Queue)> {
        self.queues.iter().enumerate()
    }

    /// Ports in use.
    #[cfg(test)]
    pub(crate) fn live(&self) -> usize {
        self.queues.len() - self.free.len()
    }
}
