// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Property-based tests for the buffer API added for foreign and `Vec`-backed memory.
//!
//! These drive the public surface — `from_vec`, `from_owner`, `from_static`, `into_vec`,
//! `try_into_vec`, `try_into_mut`, `is_unique` — where the crate's users meet it. The primitives
//! underneath have their own properties in `src/alloc/property_tests.rs`.

#![expect(clippy::tests_outside_test_module)]

use hegel::TestCase;
use hegel::generators as gs;
use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_buffer::ByteBuffer;

/// Alignments a buffer can realistically be asked for, all powers of two.
const ALIGNMENTS: [usize; 5] = [4, 8, 64, 256, 4096];

fn draw_i32s(tc: &TestCase, max: usize) -> Vec<i32> {
    tc.draw(gs::vecs(gs::integers::<i32>()).max_size(max))
}

/// Adopting a `Vec` and asking for it back returns the same elements in the same allocation.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn vec_round_trip_is_the_identity(tc: TestCase) {
    let values = draw_i32s(&tc, 1024);
    let expected = values.clone();
    let ptr = values.as_ptr();
    let had_allocation = values.capacity() > 0;

    let buffer = Buffer::from_vec(values);
    assert_eq!(buffer.as_slice(), expected.as_slice());
    if had_allocation {
        assert_eq!(buffer.as_ptr(), ptr, "adoption must not copy");
    }

    let recovered = buffer.try_into_vec().unwrap_or_else(|_| unreachable!());
    assert_eq!(recovered, expected);
    if had_allocation {
        assert_eq!(recovered.as_ptr(), ptr, "hand-back must not copy");
    }
}

/// A buffer can be handed back as a `Vec` exactly when it is the only handle to an allocation
/// made with exactly `align_of::<T>()`. `into_vec` produces the same elements either way.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn into_vec_always_agrees_with_the_slice(tc: TestCase) {
    let values = draw_i32s(&tc, 512);
    let over_align = tc.draw(gs::booleans());
    let share = tc.draw(gs::booleans());

    let buffer = if over_align {
        Buffer::copy_from_aligned(&values, Alignment::new(256))
    } else {
        Buffer::from_vec(values.clone())
    };

    let held = share.then(|| buffer.clone());
    let ptr = buffer.as_ptr();
    // Ask the buffer directly rather than inferring from `share`: whether an *empty* buffer owns
    // an allocation at all depends on which constructor made it.
    let unique = buffer.is_unique();
    let exactly_aligned = !over_align;

    // Uniqueness and exact alignment are each necessary, and together sufficient, for the
    // allocation to be handed over. Consume the buffer rather than a clone: cloning it would
    // itself make the allocation shared, which is the very thing under test.
    match buffer.try_into_vec() {
        Ok(recovered) => {
            assert_eq!(recovered, values);
            if !values.is_empty() {
                assert!(unique, "a shared allocation cannot be given away");
                assert!(
                    exactly_aligned,
                    "an over-aligned allocation cannot be freed as a Vec<i32>"
                );
                assert_eq!(recovered.as_ptr(), ptr, "and the hand-over is zero-copy");
            }
        }
        Err(buffer) => {
            assert!(
                !unique || !exactly_aligned || values.is_empty(),
                "a solely-owned, exactly-aligned allocation must be given away"
            );
            // Whichever path it takes, `into_vec` still produces the same elements.
            assert_eq!(buffer.into_vec(), values);
        }
    }
    drop(held);
}

/// Slicing agrees with slicing the equivalent `&[T]`, and preserves the buffer's alignment.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn slices_agree_with_the_equivalent_slice(tc: TestCase) {
    let alignment = Alignment::new(tc.draw(gs::sampled_from(&ALIGNMENTS[..])));
    let values = draw_i32s(&tc, 512);
    let buffer = Buffer::copy_from_aligned(&values, alignment);

    let begin = tc.draw(gs::integers::<usize>().min_value(0).max_value(values.len()));
    let end = tc.draw(
        gs::integers::<usize>()
            .min_value(begin)
            .max_value(values.len()),
    );
    // `slice` requires the start to sit on the buffer's alignment.
    tc.assume(alignment.is_offset_aligned(begin * size_of::<i32>()));

    let sliced = buffer.slice(begin..end);
    assert_eq!(sliced.as_slice(), &values[begin..end]);
    assert_eq!(sliced.alignment(), alignment);
    assert!(sliced.is_aligned(alignment));
}

/// Every constructor that takes an alignment produces a buffer that meets it.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn constructors_honour_their_alignment(tc: TestCase) {
    let alignment = Alignment::new(tc.draw(gs::sampled_from(&ALIGNMENTS[..])));
    let values = draw_i32s(&tc, 256);

    let copied = Buffer::copy_from_aligned(&values, alignment);
    assert!(copied.is_aligned(alignment));
    assert_eq!(copied.as_slice(), values.as_slice());

    let zeroed = Buffer::<i32>::zeroed_aligned(values.len(), alignment);
    assert!(zeroed.is_aligned(alignment));
    assert_eq!(zeroed.len(), values.len());

    // A buffer grown from empty must land on its alignment too, however little goes into it.
    let mut built = BufferMut::<i32>::empty_aligned(alignment);
    built.extend_from_slice(&values);
    assert!(built.freeze().is_aligned(alignment));
}

/// Adopting an owner is zero-copy, keeps the owner's bytes, and stays read-only.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn adopted_owners_are_read_only_and_uncopied(tc: TestCase) {
    let values = tc.draw(gs::vecs(gs::integers::<i32>()).min_size(1).max_size(256));
    let owner: std::sync::Arc<[i32]> = std::sync::Arc::from(values.clone());
    let ptr = owner.as_ptr();

    let buffer = Buffer::from_owner(std::sync::Arc::clone(&owner));
    assert_eq!(buffer.as_ptr(), ptr, "adoption must not copy");
    assert_eq!(buffer.as_slice(), values.as_slice());

    // We only ever had shared access, so becoming mutable must copy rather than write through.
    let mutable = buffer.into_mut();
    assert_ne!(mutable.as_ptr(), ptr);
    assert_eq!(owner.as_ref(), values.as_slice(), "the owner is untouched");
}

/// An owner that hands over exclusive, writable access stays writable in place.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn writable_owners_are_mutated_in_place(tc: TestCase) {
    let values = tc.draw(gs::vecs(gs::integers::<i32>()).min_size(1).max_size(256));
    let boxed: Box<[i32]> = values.clone().into_boxed_slice();
    let ptr = boxed.as_ptr();

    let mut buffer = BufferMut::from_owner(boxed);
    assert_eq!(buffer.as_ptr(), ptr, "adoption must not copy");

    let index = tc.draw(
        gs::integers::<usize>()
            .min_value(0)
            .max_value(values.len() - 1),
    );
    let value = tc.draw(gs::integers::<i32>());
    buffer[index] = value;

    assert_eq!(buffer.as_ptr(), ptr, "mutation must not copy either");
    assert_eq!(buffer[index], value);

    // Freezing and taking it back is also in place, as long as nothing else holds it.
    let frozen = buffer.freeze();
    assert!(frozen.is_unique());
    let thawed = frozen.try_into_mut().unwrap_or_else(|_| unreachable!());
    assert_eq!(thawed.as_ptr(), ptr);
}

/// A buffer is recoverable as mutable exactly when nothing else holds its allocation, and the
/// contents survive either way.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn mutability_is_recoverable_only_when_unique(tc: TestCase) {
    let values = draw_i32s(&tc, 256);
    let clones = tc.draw(gs::integers::<usize>().min_value(0).max_value(3));

    let buffer = Buffer::from_vec(values.clone());
    let held: Vec<Buffer<i32>> = (0..clones).map(|_| buffer.clone()).collect();

    // A `Vec` with no elements owns no allocation, so cloning it cannot make it shared.
    let unique = held.is_empty() || values.is_empty();
    assert_eq!(buffer.is_unique(), unique);

    // `into_mut` always succeeds; it is in place exactly when the buffer was the only handle.
    // Consume the buffer rather than a clone, which would itself make it shared.
    let ptr = buffer.as_ptr();
    let mutable = buffer.into_mut();
    assert_eq!(mutable.as_slice(), values.as_slice());
    if !values.is_empty() {
        assert_eq!(
            mutable.as_ptr() == ptr,
            unique,
            "a solely-owned buffer must be reused, a shared one copied"
        );
    }
    drop(held);
}

/// A `'static` slice is borrowed rather than copied, and can never be written through.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn static_buffers_are_borrowed_and_immutable(tc: TestCase) {
    static VALUES: [i32; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
    let len = tc.draw(gs::integers::<usize>().min_value(1).max_value(VALUES.len()));

    let buffer = Buffer::from_static(&VALUES[..len]);
    assert_eq!(buffer.as_ptr(), VALUES.as_ptr(), "must not copy");
    assert_eq!(buffer.as_slice(), &VALUES[..len]);

    // Static memory carries no refcount, so we can never claim to be its only handle...
    assert!(!buffer.is_unique());
    // ... and it is never writable.
    assert!(buffer.try_into_mut().is_err());
}

/// Equality, ordering and hashing are all defined over the bytes, so buffers built different ways
/// from the same elements are indistinguishable.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn equality_follows_the_elements(tc: TestCase) {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hash;
    use std::hash::Hasher;

    let left = draw_i32s(&tc, 128);
    let right = draw_i32s(&tc, 128);

    // The same elements, reached three different ways.
    let copied = Buffer::copy_from(&left);
    let adopted = Buffer::from_vec(left.clone());
    let over_aligned = Buffer::copy_from_aligned(&left, Alignment::new(256));

    assert_eq!(copied, adopted);
    assert_eq!(copied, over_aligned);

    let hash = |b: &Buffer<i32>| {
        let mut h = DefaultHasher::new();
        b.hash(&mut h);
        h.finish()
    };
    assert_eq!(hash(&copied), hash(&adopted));
    assert_eq!(hash(&copied), hash(&over_aligned));

    // Ordering is the elements', not the bytes': `[-1i32] < [0i32]` even though its bytes are
    // larger.
    let other = Buffer::copy_from(&right);
    assert_eq!(
        copied.cmp(&other),
        left.as_slice().cmp(right.as_slice()),
        "ordering must follow the elements"
    );
}

/// A `bytes::Bytes` round trip preserves the pointer and the contents in both directions.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn bytes_round_trip_is_zero_copy(tc: TestCase) {
    let values: Vec<u8> = tc.draw(gs::vecs(gs::integers::<u8>()).min_size(1).max_size(512));
    let buffer = ByteBuffer::from(values.clone());
    let ptr = buffer.as_ptr();

    let bytes = buffer.into_bytes();
    assert_eq!(bytes.as_ptr(), ptr);
    assert_eq!(bytes.as_ref(), values.as_slice());

    let back = ByteBuffer::from(bytes);
    assert_eq!(back.as_ptr(), ptr);
    assert_eq!(back.as_slice(), values.as_slice());
}

/// Splitting a mutable buffer and rejoining it restores the original contents.
#[hegel::test]
#[cfg_attr(miri, ignore)] // hegel's engine uses file IO that Miri cannot run
fn split_off_then_unsplit_is_the_identity(tc: TestCase) {
    let alignment = Alignment::new(tc.draw(gs::sampled_from(&ALIGNMENTS[..])));
    let values = draw_i32s(&tc, 256);

    let mut buffer = BufferMut::<i32>::copy_from_aligned(&values, alignment);
    let capacity = buffer.capacity();
    let at = tc.draw(gs::integers::<usize>().min_value(0).max_value(capacity));
    // `split_off` requires the split point to sit on the buffer's alignment.
    tc.assume(alignment.is_offset_aligned(at * size_of::<i32>()));

    let tail = buffer.split_off(at);
    assert_eq!(buffer.len() + tail.len(), values.len());

    buffer.unsplit(tail);
    assert_eq!(buffer.as_slice(), values.as_slice());
    assert!(buffer.freeze().is_aligned(alignment));
}
