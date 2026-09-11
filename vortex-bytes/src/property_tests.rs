// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Property-based tests for the allocation primitives.
//!
//! The example-based tests in [`super::tests`] pin down the cases we reasoned about while writing
//! this module. These check the invariants that have to hold for *every* sequence of operations,
//! which is where a hand-written test is least likely to look.

use hegel::TestCase;
use hegel::generators as gs;

use super::SharedBytes;
use super::UniqueBytes;
use crate::Alignment;

/// Alignments a buffer can realistically be asked for. Every one is a power of two, which
/// [`Alignment::new`] requires.
const ALIGNMENTS: [usize; 6] = [1, 2, 8, 64, 256, 4096];

fn draw_alignment(tc: &TestCase) -> Alignment {
    Alignment::new(tc.draw(gs::sampled_from(&ALIGNMENTS[..])))
}

fn draw_bytes(tc: &TestCase, max: usize) -> Vec<u8> {
    tc.draw(gs::vecs(gs::integers::<u8>()).max_size(max))
}

/// An allocation always satisfies the alignment it was asked for, whatever its size.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn allocations_are_aligned(tc: TestCase) {
    let alignment = draw_alignment(&tc);
    let capacity = tc.draw(gs::integers::<usize>().min_value(0).max_value(1 << 16));

    let bytes = UniqueBytes::with_capacity(capacity, alignment);
    assert!(alignment.is_ptr_aligned(bytes.as_ptr()));
    assert!(bytes.capacity() >= capacity);
    assert_eq!(bytes.len(), 0);

    let zeroed = UniqueBytes::zeroed(capacity, alignment);
    assert!(alignment.is_ptr_aligned(zeroed.as_ptr()));
    assert_eq!(zeroed.len(), capacity);
    assert!(zeroed.as_slice().iter().all(|&b| b == 0));
}

/// Growing a window never loses data and never weakens its alignment, however many times it has
/// to reallocate along the way.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn growth_preserves_contents_and_alignment(tc: TestCase) {
    let alignment = draw_alignment(&tc);
    let chunks: Vec<Vec<u8>> = tc.draw(
        gs::vecs(gs::vecs(gs::integers::<u8>()).max_size(64))
            .min_size(1)
            .max_size(16),
    );

    let mut bytes = UniqueBytes::with_capacity(0, alignment);
    let mut model: Vec<u8> = Vec::new();
    for chunk in &chunks {
        bytes.extend_from_slice(chunk);
        model.extend_from_slice(chunk);

        assert_eq!(bytes.as_slice(), model.as_slice());
        assert!(alignment.is_ptr_aligned(bytes.as_ptr()));
        assert!(bytes.capacity() >= bytes.len());
    }
}

/// Slicing a shared window agrees with slicing the equivalent `&[u8]`, and every slice keeps the
/// region alive.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn slices_agree_with_the_equivalent_slice(tc: TestCase) {
    let data = draw_bytes(&tc, 512);
    let mut bytes = UniqueBytes::with_capacity(data.len(), Alignment::none());
    bytes.extend_from_slice(&data);
    let shared = bytes.freeze();

    let begin = tc.draw(gs::integers::<usize>().min_value(0).max_value(data.len()));
    let end = tc.draw(
        gs::integers::<usize>()
            .min_value(begin)
            .max_value(data.len()),
    );

    let sliced = shared.slice(begin, end);
    assert_eq!(sliced.as_slice(), &data[begin..end]);
    assert_eq!(sliced.len(), end - begin);

    // A non-empty slice shares the region, so neither handle is unique. An empty slice owns
    // nothing, so it does not hold the region at all.
    if end > begin {
        assert!(!shared.is_unique());
        assert!(!sliced.is_unique());
    } else {
        assert!(sliced.is_unique());
    }
}

/// `slice_ref` recovers exactly the window a `&[u8]` borrowed from the buffer points at.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn slice_ref_recovers_the_subslice(tc: TestCase) {
    let data = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(1).max_size(512));
    let mut bytes = UniqueBytes::with_capacity(data.len(), Alignment::none());
    bytes.extend_from_slice(&data);
    let shared = bytes.freeze();

    let begin = tc.draw(gs::integers::<usize>().min_value(0).max_value(data.len()));
    let end = tc.draw(
        gs::integers::<usize>()
            .min_value(begin)
            .max_value(data.len()),
    );

    let subset = &shared.as_slice()[begin..end];
    let recovered = shared.slice_ref(subset);
    assert_eq!(recovered.as_slice(), &data[begin..end]);
}

/// A window adopted from a `Vec<T>` hands the same allocation back out, for any element type
/// whose layout it was built with.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn vec_round_trip_returns_the_same_allocation(tc: TestCase) {
    let values: Vec<u32> = tc.draw(gs::vecs(gs::integers::<u32>()).max_size(1024));
    let expected = values.clone();
    let ptr = values.as_ptr();
    let capacity = values.capacity();

    let bytes = UniqueBytes::from_vec(values);
    assert_eq!(bytes.len(), expected.len() * size_of::<u32>());

    let recovered = bytes
        .try_into_vec::<u32>()
        .expect("adopted from a Vec<u32>");
    assert_eq!(recovered, expected);
    if capacity > 0 {
        assert_eq!(recovered.as_ptr(), ptr, "the allocation must not move");
    }
}

/// An over-aligned region can never be handed to a `Vec`, which would free it with `T`'s
/// alignment. A region allocated with exactly `T`'s alignment always can.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn only_exactly_aligned_regions_become_vecs(tc: TestCase) {
    let alignment = draw_alignment(&tc);
    let len = tc.draw(gs::integers::<usize>().min_value(0).max_value(256));

    let mut bytes = UniqueBytes::with_capacity(len * size_of::<u32>(), alignment);
    bytes.extend_from_slice(&vec![0u8; len * size_of::<u32>()]);

    let exactly_aligned = alignment == Alignment::of::<u32>();
    // A zero-length window is always handed over as an empty `Vec`, whatever it was aligned to.
    let expected = exactly_aligned || len == 0;
    assert_eq!(bytes.try_into_vec::<u32>().is_ok(), expected);
}

/// Splitting a window in two and rejoining it restores the original contents, wherever the split
/// falls.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn split_off_then_unsplit_is_the_identity(tc: TestCase) {
    let alignment = draw_alignment(&tc);
    let data = draw_bytes(&tc, 512);

    let mut bytes = UniqueBytes::with_capacity(data.len(), alignment);
    bytes.extend_from_slice(&data);
    let capacity = bytes.capacity();

    // Both halves keep the alignment promise, so the split has to fall on a multiple of it.
    let steps = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(capacity / alignment.as_usize()),
    );
    let at = steps * alignment.as_usize();
    let other = bytes.split_off(at);

    // The two windows partition the original one.
    assert_eq!(bytes.capacity() + other.capacity(), capacity);
    assert_eq!(bytes.len() + other.len(), data.len());
    assert_eq!(bytes.as_slice(), &data[..bytes.len()]);
    assert_eq!(other.as_slice(), &data[bytes.len()..]);

    bytes.unsplit(other);
    assert_eq!(bytes.as_slice(), data.as_slice());
    assert!(alignment.is_ptr_aligned(bytes.as_ptr()));
}

/// Advancing gives up the front of a window and nothing else.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn advance_drops_only_the_front(tc: TestCase) {
    let data = draw_bytes(&tc, 512);
    let mut bytes = UniqueBytes::with_capacity(data.len(), Alignment::none());
    bytes.extend_from_slice(&data);

    let cnt = tc.draw(gs::integers::<usize>().min_value(0).max_value(data.len()));
    let capacity = bytes.capacity();

    bytes.advance(cnt);
    assert_eq!(bytes.as_slice(), &data[cnt..]);
    assert_eq!(bytes.capacity(), capacity - cnt);

    let mut shared = {
        let mut b = UniqueBytes::with_capacity(data.len(), Alignment::none());
        b.extend_from_slice(&data);
        b.freeze()
    };
    shared.advance(cnt);
    assert_eq!(shared.as_slice(), &data[cnt..]);
}

/// Exclusive ownership can be recovered exactly when no other handle exists, and the recovered
/// window covers the rest of the region.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn ownership_is_recoverable_only_when_unique(tc: TestCase) {
    let data = draw_bytes(&tc, 256);
    let clones = tc.draw(gs::integers::<usize>().min_value(0).max_value(3));

    let mut bytes = UniqueBytes::with_capacity(data.len(), Alignment::none());
    bytes.extend_from_slice(&data);
    let shared = bytes.freeze();

    let held: Vec<SharedBytes> = (0..clones).map(|_| shared.clone()).collect();
    // An empty window owns no region, so cloning it cannot make it shared.
    let unique = held.is_empty() || data.is_empty();
    assert_eq!(shared.is_unique(), unique);

    match shared.try_into_unique() {
        Ok(unique_bytes) => {
            assert!(unique);
            assert_eq!(unique_bytes.as_slice(), data.as_slice());
            assert!(unique_bytes.capacity() >= unique_bytes.len());
        }
        Err(shared) => {
            assert!(!unique);
            assert_eq!(shared.as_slice(), data.as_slice());
        }
    }
    drop(held);
}

/// A model-based check over whole sequences of operations: whatever order they come in, the
/// window's bytes must equal the model's, and it must never lose its alignment.
struct WindowModel {
    bytes: UniqueBytes,
    model: Vec<u8>,
    alignment: Alignment,
}

#[hegel::state_machine]
impl WindowModel {
    #[rule]
    fn extend(&mut self, tc: TestCase) {
        let chunk = tc.draw(gs::vecs(gs::integers::<u8>()).max_size(128));
        self.bytes.extend_from_slice(&chunk);
        self.model.extend_from_slice(&chunk);
    }

    #[rule]
    fn reserve(&mut self, tc: TestCase) {
        let additional = tc.draw(gs::integers::<usize>().min_value(0).max_value(4096));
        self.bytes.reserve(additional);
        assert!(self.bytes.capacity() >= self.bytes.len() + additional);
    }

    /// Advance by a multiple of the alignment, which is all `advance` accepts.
    #[rule]
    fn advance(&mut self, tc: TestCase) {
        let steps = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(self.model.len() / self.alignment.as_usize()),
        );
        let cnt = steps * self.alignment.as_usize();
        self.bytes.advance(cnt);
        self.model.drain(..cnt);
    }

    #[rule]
    fn truncate(&mut self, tc: TestCase) {
        let len = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(self.model.len()),
        );
        // SAFETY: shrinking a window cannot expose uninitialised bytes.
        unsafe { self.bytes.set_len(len) };
        self.model.truncate(len);
    }

    /// Split the window and immediately drop the far half, which is what makes the near half
    /// eligible to reclaim the region on its next growth.
    #[rule]
    fn split_and_drop(&mut self, tc: TestCase) {
        let at = self.draw_split_point(&tc);
        drop(self.bytes.split_off(at));
        self.model.truncate(at);
    }

    /// Split the window and put it straight back together.
    #[rule]
    fn split_and_unsplit(&mut self, tc: TestCase) {
        let at = self.draw_split_point(&tc);
        let other = self.bytes.split_off(at);
        self.bytes.unsplit(other);
    }

    /// A split point within the capacity, on a multiple of the alignment as `split_off` requires.
    fn draw_split_point(&self, tc: &TestCase) -> usize {
        let steps = tc.draw(
            gs::integers::<usize>()
                .min_value(0)
                .max_value(self.bytes.capacity() / self.alignment.as_usize()),
        );
        steps * self.alignment.as_usize()
    }

    /// Freeze the window and take it straight back. Nothing else holds it, so this must succeed
    /// and must not move the data.
    #[rule]
    fn freeze_and_thaw(&mut self, _: TestCase) {
        let ptr = self.bytes.as_ptr();
        let bytes = std::mem::replace(&mut self.bytes, UniqueBytes::empty());
        let thawed = bytes
            .freeze()
            .try_into_unique()
            .unwrap_or_else(|_| panic!("sole handle to the region"));
        assert_eq!(thawed.as_ptr(), ptr, "freeze/thaw must not move the data");
        self.bytes = thawed;
    }

    /// Freeze the window and hold a second handle to it, so the region is genuinely shared.
    #[rule]
    fn freeze_while_shared(&mut self, _: TestCase) {
        let bytes = std::mem::replace(&mut self.bytes, UniqueBytes::empty());
        let shared = bytes.freeze();
        let clone = shared.clone();
        assert_eq!(clone.as_slice(), shared.as_slice());
        assert!(!shared.is_unique() || shared.is_empty());
        drop(clone);
        self.bytes = shared
            .try_into_unique()
            .unwrap_or_else(|_| panic!("sole handle again"));
    }

    #[invariant]
    fn contents_match_the_model(&self, _: TestCase) {
        assert_eq!(self.bytes.as_slice(), self.model.as_slice());
    }

    #[invariant]
    fn length_never_exceeds_capacity(&self, _: TestCase) {
        assert!(self.bytes.len() <= self.bytes.capacity());
        assert_eq!(self.bytes.len(), self.model.len());
    }

    #[invariant]
    fn alignment_is_never_weakened(&self, _: TestCase) {
        assert!(
            self.alignment.is_ptr_aligned(self.bytes.as_ptr()),
            "window lost its {} alignment",
            self.alignment
        );
    }
}

#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn window_operations_agree_with_a_vec(tc: TestCase) {
    let alignment = draw_alignment(&tc);
    hegel::stateful::run(
        WindowModel {
            bytes: UniqueBytes::with_capacity(0, alignment),
            model: Vec::new(),
            alignment,
        },
        tc,
    );
}
