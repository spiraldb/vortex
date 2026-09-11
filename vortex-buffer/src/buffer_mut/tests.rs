// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use crate::Alignment;
use crate::BufferMut;
use crate::buffer_mut;

#[test]
fn capacity() {
    let mut n = 57;
    let mut buf = BufferMut::<i32>::with_capacity_aligned(n, Alignment::new(1024));
    assert!(buf.capacity() >= 57);

    while n > 0 {
        buf.push(0);
        assert!(buf.capacity() >= n);
        n -= 1
    }

    assert_eq!(buf.alignment(), Alignment::new(1024));
}

#[test]
fn growth_preserves_alignment_and_values() {
    let alignment = Alignment::new(4096);
    let mut buffer = BufferMut::<u64>::with_capacity_aligned(1, alignment);

    for value in 0..10_000 {
        buffer.push(value);
        assert!(alignment.is_offset_aligned(buffer.as_ptr().addr()));
    }

    assert_eq!(buffer.as_slice(), (0..10_000).collect::<Vec<_>>());
}

#[test]
fn growth_seeds_and_doubles_logical_capacity() {
    // No preferred over-alignment, so the region carries no padding for `reserve` to reclaim
    // and growth is exactly the doubling policy.
    let mut buffer =
        BufferMut::<u8>::with_capacity_preferred_aligned(0, Alignment::of::<u8>(), None);

    buffer.push(0);
    let capacity = buffer.capacity();
    assert_eq!(capacity, Alignment::DEFAULT_ALIGNMENT.as_usize());

    buffer.reserve(capacity);
    assert_eq!(buffer.capacity(), capacity * 2);
}

#[test]
fn growth_keeps_live_data() {
    let mut buffer = BufferMut::<u32>::with_capacity(1);
    let capacity = buffer.capacity();
    buffer.extend(std::iter::repeat_n(7, capacity));

    buffer.push(u32::MAX);

    assert!(buffer.capacity() > capacity);
    assert!(buffer.alignment().is_ptr_aligned(buffer.as_ptr()));
    assert_eq!(&buffer[..capacity], vec![7; capacity]);
    assert_eq!(buffer[capacity], u32::MAX);
}

#[test]
fn raising_logical_alignment_preserves_capacity() {
    let buffer = BufferMut::<u8>::with_capacity_preferred_aligned(1, Alignment::of::<u8>(), None);
    let capacity = buffer.capacity();

    let mut buffer = buffer.aligned(Alignment::new(2));

    assert_eq!(buffer.capacity(), capacity);
    buffer.extend(0..100);
    assert!(Alignment::new(2).is_ptr_aligned(buffer.as_ptr()));
    assert_eq!(buffer.as_slice(), (0..100).collect::<Vec<_>>());
}

#[test]
fn from_iter() {
    let buf = BufferMut::from_iter([0, 10, 20, 30]);
    assert_eq!(buf.as_slice(), &[0, 10, 20, 30]);
}

#[test]
fn try_from_trusted_len_iter_ok() {
    let buf = BufferMut::<i32>::try_from_trusted_len_iter(
        [0, 10, 20, 30].iter().map(|&v| Ok::<_, ()>(v)),
    )
    .unwrap();
    assert_eq!(buf.as_slice(), &[0, 10, 20, 30]);
}

#[test]
fn try_from_trusted_len_iter_err() {
    let result: Result<BufferMut<i32>, &'static str> = BufferMut::try_from_trusted_len_iter(
        [0, 10, 20, 30]
            .iter()
            .map(|&v| if v == 20 { Err("bad") } else { Ok(v) }),
    );
    assert_eq!(result.err(), Some("bad"));
}

#[test]
fn try_extend_trusted_retains_values_before_error() {
    let mut buf = BufferMut::from_iter([0, 10]);
    let result = buf.try_extend_trusted([Ok(20), Err("bad"), Ok(30)].into_iter());

    assert_eq!(result, Err("bad"));
    assert_eq!(buf.as_slice(), &[0, 10, 20]);
}

#[test]
fn extend() {
    let mut buf = BufferMut::empty();
    buf.extend([0i32, 10, 20, 30]);
    buf.extend([40, 50, 60]);
    assert_eq!(buf.as_slice(), &[0, 10, 20, 30, 40, 50, 60]);
}

#[test]
fn push() {
    let mut buf = BufferMut::empty();
    buf.push(1);
    buf.push(2);
    buf.push(3);
    assert_eq!(buf.as_slice(), &[1, 2, 3]);
}

#[test]
fn push_n() {
    let mut buf = BufferMut::empty();
    buf.push_n(0, 100);
    assert_eq!(buf.as_slice(), &[0; 100]);
}

#[test]
fn as_mut() {
    let mut buf = buffer_mut![0, 1, 2];
    // Uses DerefMut
    buf[1] = 0;
    // Uses as_mut
    buf.as_mut()[2] = 0;
    assert_eq!(buf.as_slice(), &[0, 0, 0]);
}

#[test]
fn map_each() {
    let buf = buffer_mut![0i32, 1, 2];
    // Add one, and cast to an unsigned u32 in the same closure
    let buf = buf.map_each_in_place(|i| (i + 1) as u32);
    assert_eq!(buf.as_slice(), &[1u32, 2, 3]);
}

#[test]
fn buffer_mut_zeroed() {
    const LEN: usize = 17;

    let mut buf = BufferMut::<u32>::zeroed(LEN);

    assert_eq!(
        buf.as_ptr().align_offset(Alignment::of::<u32>().as_usize()),
        0
    );
    assert_eq!(buf.as_slice(), &[0; LEN]);

    buf[3] = 7;
    assert_eq!(buf.as_slice()[3], 7);
}

#[test]
fn buffer_mut_zeroed_aligned() {
    const LEN: usize = 17;
    let alignment = Alignment::new(64);

    let mut buf = BufferMut::<u32>::zeroed_aligned(LEN, alignment);

    assert_eq!(buf.as_ptr().align_offset(alignment.as_usize()), 0);
    assert_eq!(buf.as_slice(), &[0; LEN]);

    buf[3] = 7;
    assert_eq!(buf.as_slice()[3], 7);
}

// --- Storage behaviour this branch adds -------------------------------------------------------

#[test]
fn from_vec_is_zero_copy_and_mutable() {
    let vec = vec![1u32, 2, 3];
    let ptr = vec.as_ptr();
    let mut buffer = BufferMut::from_vec(vec);
    assert_eq!(buffer.as_ptr(), ptr);
    buffer.push(4);
    assert_eq!(buffer.as_slice(), [1, 2, 3, 4]);
}

#[test]
fn into_vec_copies_when_over_aligned() {
    let buffer = BufferMut::<u32>::copy_from([1, 2, 3]);
    assert!(buffer.as_ptr().addr().is_multiple_of(256));
    let vec = buffer.into_vec();
    assert_eq!(vec, [1, 2, 3]);
}

#[test]
fn try_into_vec_rejects_over_aligned() {
    let buffer = BufferMut::<u32>::copy_from([1, 2, 3]);
    assert!(buffer.try_into_vec().is_err());
}

#[test]
fn from_owner_is_mutable() {
    let mut owner = vec![1u8, 2, 3, 4];
    let ptr = owner.as_mut_ptr();
    let mut buffer = BufferMut::from_owner(owner);
    buffer[0] = 9;
    assert_eq!(buffer.as_ptr(), ptr.cast_const());
    assert_eq!(buffer.as_slice(), [9, 2, 3, 4]);
    // Growing past the owner's length moves out of it.
    buffer.extend_from_slice(&[5, 6, 7, 8, 9]);
    assert_eq!(buffer.as_slice(), [9, 2, 3, 4, 5, 6, 7, 8, 9]);
}

#[test]
fn split_off_and_unsplit_is_in_place() {
    let mut buffer = BufferMut::<u8>::with_capacity(64);
    buffer.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
    let ptr = buffer.as_ptr();
    let tail = buffer.split_off(4);
    assert_eq!(buffer.as_slice(), [1, 2, 3, 4]);
    assert_eq!(tail.as_slice(), [5, 6, 7, 8]);
    assert_eq!(tail.as_ptr(), ptr.wrapping_add(4));
    buffer.unsplit(tail);
    assert_eq!(buffer.as_slice(), [1, 2, 3, 4, 5, 6, 7, 8]);
    assert_eq!(
        buffer.as_ptr(),
        ptr,
        "adjacent halves merge without copying"
    );
}

#[test]
fn unsplit_copies_when_not_adjacent() {
    let mut first = BufferMut::<u8>::copy_from([1, 2]);
    let second = BufferMut::<u8>::copy_from([3, 4]);
    first.unsplit(second);
    assert_eq!(first.as_slice(), [1, 2, 3, 4]);
}

#[test]
fn split_off_past_the_length_keeps_both_lengths() {
    let mut buffer = BufferMut::<u32>::with_capacity(8);
    buffer.extend_from_slice(&[1, 2]);
    let tail = buffer.split_off(4);
    assert_eq!(buffer.len(), 2);
    assert!(tail.is_empty());
    assert!(tail.capacity() >= 4);
}

#[test]
fn reserve_reclaims_a_released_sibling() {
    let mut buffer = BufferMut::<u8>::with_capacity(64);
    buffer.extend_from_slice(&[1, 2, 3, 4]);
    let ptr = buffer.as_ptr();
    drop(buffer.split_off(32));
    buffer.reserve(60);
    assert_eq!(
        buffer.as_ptr(),
        ptr,
        "the other half's room is reclaimed in place"
    );
    assert!(buffer.capacity() >= 64);
}

#[test]
fn map_each_rejects_a_wider_alignment() {
    let buffer = BufferMut::<[u8; 8]>::copy_from([[0; 8]; 4]);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        buffer.map_each_in_place(|_| 0u64)
    }));
    assert!(result.is_err(), "u64 is wider-aligned than [u8; 8]");
}

#[test]
fn freeze_does_not_allocate_and_thaw_keeps_capacity() {
    let mut buffer = BufferMut::<u16>::with_capacity(32);
    buffer.extend_from_slice(&[1, 2, 3]);
    let ptr = buffer.as_ptr();
    let frozen = buffer.freeze();
    assert_eq!(frozen.as_ptr(), ptr);
    let thawed = frozen.try_into_mut().expect("unique");
    assert_eq!(thawed.as_ptr(), ptr);
    assert!(thawed.capacity() >= 32);
}
