// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Buf;
use bytes::Bytes;

use crate::Alignment;
use crate::Buffer;
use crate::ByteBuffer;
use crate::buffer;

#[test]
fn align() {
    let buf = buffer![0u8, 1, 2];
    let aligned = buf.aligned(Alignment::new(32));
    assert_eq!(aligned.alignment(), Alignment::new(32));
    assert_eq!(aligned.as_slice(), &[0, 1, 2]);
}

#[test]
fn buffer_iterator_send_sync() {
    fn assert_send_sync<T: Send + Sync>(_: &T) {}

    let mut iter = buffer![0i32, 1, 2, 3].into_iter();
    assert_send_sync(&iter);
    iter.next();
    let remaining: Vec<i32> = std::thread::spawn(move || iter.collect()).join().unwrap();
    assert_eq!(remaining, vec![1, 2, 3]);
}

#[test]
fn slice() {
    let buf = buffer![0, 1, 2, 3, 4];
    assert_eq!(buf.slice(1..3).as_slice(), &[1, 2]);
    assert_eq!(buf.slice(1..=3).as_slice(), &[1, 2, 3]);
}

#[test]
fn slice_unaligned() {
    let buf = buffer![0i32, 1, 2, 3, 4].into_byte_buffer();
    // With a regular slice, this would panic. See [`slice_bad_alignment`].
    let sliced = buf.slice_unaligned(1..2);
    // Verify the slice has the expected length (1 byte from index 1 to 2).
    assert_eq!(sliced.len(), 1);
    // The original buffer has i32 values [0, 1, 2, 3, 4].
    // In little-endian bytes, 0i32 = [0, 0, 0, 0], so byte at index 1 is 0.
    assert_eq!(sliced.as_slice(), &[0]);
}

#[test]
#[should_panic]
fn slice_bad_alignment() {
    let buf = buffer![0i32, 1, 2, 3, 4].into_byte_buffer();
    // We should only be able to slice this buffer on 4-byte (i32) boundaries.
    buf.slice(1..2);
}

#[test]
fn bytes_buf() {
    let mut buf = ByteBuffer::copy_from("helloworld".as_bytes());
    assert_eq!(buf.remaining(), 10);
    assert_eq!(buf.chunk(), b"helloworld");

    buf.advance(5);
    assert_eq!(buf.remaining(), 5);
    assert_eq!(buf.as_slice(), b"world");
    assert_eq!(buf.chunk(), b"world");
}

#[test]
fn buffer_zeroed() {
    const LEN: usize = 17;

    let buf = Buffer::<u32>::zeroed(LEN);

    assert!(buf.is_aligned(Alignment::of::<u32>()));
    assert_eq!(buf.as_slice(), &[0; LEN]);
}

#[test]
fn buffer_zeroed_aligned() {
    const LEN: usize = 17;
    let alignment = Alignment::new(64);

    let buf = Buffer::<u32>::zeroed_aligned(LEN, alignment);

    assert!(buf.is_aligned(alignment));
    assert_eq!(buf.as_slice(), &[0; LEN]);
}

#[test]
fn copy_from_over_aligns_to_default() {
    let values = [1u32, 2, 3];
    let buf = Buffer::<u32>::copy_from(values);

    // The buffer reports the scalar type's alignment, ...
    assert_eq!(buf.alignment(), Alignment::of::<u32>());
    // ... but the underlying allocation is over-aligned to DEFAULT_ALIGNMENT.
    assert!(buf.is_aligned(Alignment::DEFAULT_ALIGNMENT));
    assert_eq!(buf.as_slice(), &values);
}

#[test]
fn zeroed_over_aligns_to_default() {
    const LEN: usize = 17;

    let buf = Buffer::<u32>::zeroed(LEN);

    assert_eq!(buf.alignment(), Alignment::of::<u32>());
    assert!(buf.is_aligned(Alignment::DEFAULT_ALIGNMENT));
    assert_eq!(buf.as_slice(), &[0; LEN]);
}

#[test]
fn from_vec() {
    let vec = vec![1, 2, 3, 4, 5];
    let buff = Buffer::from(vec.clone());
    assert!(buff.is_aligned(Alignment::of::<i32>()));
    assert_eq!(vec, buff.as_ref());
}

#[test]
fn from_vec_adopts_allocation() {
    let mut vec = Vec::with_capacity(16);
    vec.extend([1u32, 2, 3, 4, 5]);
    let ptr = vec.as_ptr();
    let capacity = vec.capacity();

    let buffer = Buffer::from(vec);
    assert_eq!(buffer.as_ptr(), ptr);

    let Ok(mut buffer) = buffer.try_into_mut() else {
        panic!("Vec-backed buffer should be uniquely owned")
    };
    assert_eq!(buffer.capacity(), capacity);

    buffer.extend(6..=32);
    assert_eq!(buffer.as_slice(), (1..=32).collect::<Vec<_>>());
    // Growth keeps a `Vec`-compatible layout, so the allocation can still be handed back.
    let vec = buffer
        .try_into_vec()
        .expect("a grown Vec-backed buffer still has a Vec's layout");
    assert_eq!(vec, (1..=32).collect::<Vec<_>>());
}

#[test]
fn byte_owner_preserves_slice_and_lifetime() {
    struct Owner {
        values: Vec<u8>,
        drops: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.values[1..4]
        }
    }

    impl Drop for Owner {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let owner = Owner {
        values: vec![0, 1, 2, 3, 4],
        drops: Arc::clone(&drops),
    };
    let ptr = owner.as_ref().as_ptr();
    let buffer = ByteBuffer::from(Bytes::from_owner(owner));
    assert_eq!(buffer.as_ptr(), ptr);
    assert_eq!(buffer.as_slice(), [1, 2, 3]);
    let view = buffer.slice(1..);
    drop(buffer);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    assert_eq!(view.as_slice(), [2, 3]);
    drop(view);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[test]
fn bytes_round_trip_reuses_owner() {
    let bytes = Bytes::from_static(&[1, 2, 3, 4]);
    let ptr = bytes.as_ptr();

    let buffer = ByteBuffer::from(bytes);
    assert!(buffer.bytes.owner::<Bytes>().is_some());
    let bytes = buffer.into_bytes();

    assert_eq!(bytes.as_ptr(), ptr);
    assert_eq!(bytes.as_ref(), &[1, 2, 3, 4]);
}

#[test]
fn external_try_into_mut_preserves_backing() {
    let buffer = ByteBuffer::from(Bytes::from_static(&[1, 2, 3, 4]));
    let ptr = buffer.as_ptr();

    let Err(buffer) = buffer.try_into_mut() else {
        panic!("external buffer became mutable")
    };

    assert_eq!(buffer.as_ptr(), ptr);
    assert!(
        buffer.bytes.owner::<Bytes>().is_some(),
        "a failed thaw hands the buffer back intact"
    );
}

#[test]
fn from_u8_vec_preserves_capacity() {
    let mut vec = Vec::with_capacity(16);
    vec.extend([1u8, 2, 3]);

    let buffer = Buffer::from(vec);
    let Ok(buffer) = buffer.try_into_mut() else {
        panic!("Vec-backed buffer should be uniquely owned")
    };
    assert_eq!(buffer.capacity(), 16);
}

#[test]
fn sliced_buffer_into_mut_has_safe_capacity() {
    let mut original = crate::BufferMut::with_capacity(128);
    original.extend(0u32..100);
    let original = original.freeze();
    let sliced = original.slice(64..96);
    drop(original);

    let Ok(mut sliced) = sliced.try_into_mut() else {
        panic!("uniquely owned slice should become mutable")
    };
    let ptr = sliced.as_ptr();
    let capacity = sliced.capacity();
    sliced.push_n(0, capacity - sliced.len());
    assert_eq!(sliced.len(), capacity);
    assert_eq!(sliced.as_ptr(), ptr);
    sliced.push(42);
    assert_eq!(&sliced[..32], (64u32..96).collect::<Vec<_>>());
    assert_eq!(&sliced[32..capacity], vec![0; capacity - 32]);
    assert_eq!(sliced[capacity], 42);
}

#[test]
fn from_vec_preserves_drop_glue() {
    struct DropValue(Arc<AtomicUsize>);

    impl Drop for DropValue {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let values = (0..3)
        .map(|_| DropValue(Arc::clone(&drops)))
        .collect::<Vec<_>>();
    let buffer = Buffer::from(values);

    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(buffer);
    assert_eq!(drops.load(Ordering::Relaxed), 3);
}

#[test]
fn empty_aligned_max_alignment() {
    // Empty buffers are backed by a static and must satisfy any valid alignment.
    let buf = Buffer::<u8>::empty_aligned(Alignment::MAX);
    assert!(buf.is_empty());
    assert!(buf.is_aligned(Alignment::MAX));
}

#[test]
fn empty_owns_nothing() {
    let empty = Buffer::<u8>::empty();
    assert!(empty.is_unique());
    assert!(empty.try_into_vec().is_ok_and(|v| v.is_empty()));
}

#[test]
fn empty_slice_preserves_alignment() {
    let buf = Buffer::<u64>::zeroed_aligned(8, Alignment::new(64));
    let sliced = buf.slice(0..0);
    assert!(sliced.is_empty());
    assert_eq!(sliced.alignment(), Alignment::new(64));
    assert!(sliced.is_aligned(Alignment::new(64)));
}

#[test]
fn empty_into_mut_preserves_alignment() {
    let buf = Buffer::<u8>::empty_aligned(Alignment::new(64));
    let buf_mut = buf.into_mut();
    assert_eq!(buf_mut.alignment(), Alignment::new(64));
    assert!(buf_mut.is_empty());
}

#[test]
fn test_slice_unaligned_end_pos() {
    let data = vec![0u8; 2];
    // Overalign the u8 vector.
    let aligned_buffer = Buffer::copy_from_aligned(&data, Alignment::new(8));
    // Previously, `Buffer::slice` incorrectly asserted that the end position
    // must be aligned. That assertion has been removed such that the end
    // position can be arbitrary and only the beginning of the slice needs
    // to be aligned.
    aligned_buffer.slice(0..1);
}

#[test]
fn test_empty_equality() {
    let a = Buffer::<u16>::empty();
    let b = Buffer::<u16>::empty();

    assert_eq!(a, b);
}

// --- Storage behaviour this branch adds -------------------------------------------------------

#[test]
fn auto_traits_follow_the_element_type() {
    const fn assert_send_sync<T: Send + Sync>() {}
    const fn assert_send<T: Send>() {}

    assert_send_sync::<Buffer<u8>>();
    assert_send_sync::<Buffer<i64>>();
    assert_send_sync::<crate::BufferMut<i64>>();
    // `Cell` is `Send` but not `Sync`. A uniquely owned buffer of it is still `Send`, exactly as
    // a `Vec<Cell<u8>>` is; a shared one is not, which the `compile_fail` doctest on `Buffer`
    // pins down.
    assert_send::<crate::BufferMut<std::cell::Cell<u8>>>();
}

#[test]
fn from_vec_is_zero_copy() {
    let vec = vec![1i32, 2, 3, 4, 5];
    let ptr = vec.as_ptr();
    let buf = Buffer::from(vec);
    assert_eq!(buf.as_ptr(), ptr);
}

#[test]
fn vec_round_trip_is_zero_copy() {
    let vec = vec![1u64, 2, 3, 4];
    let ptr = vec.as_ptr();
    let buf = Buffer::from(vec);
    assert!(buf.is_unique());
    let vec = buf.try_into_vec().expect("sole owner of a Vec allocation");
    assert_eq!(vec.as_ptr(), ptr);
    assert_eq!(vec, [1, 2, 3, 4]);
}

#[test]
fn vec_round_trip_copies_when_shared() {
    let buf = Buffer::from(vec![1u64, 2, 3, 4]);
    let other = buf.clone();
    assert!(!buf.is_unique());
    assert!(buf.clone().try_into_vec().is_err());
    // `into_vec` still delivers, by copying.
    assert_eq!(buf.into_vec(), [1, 2, 3, 4]);
    assert_eq!(other.as_slice(), [1, 2, 3, 4]);
}

#[test]
fn t_aligned_buffers_are_handed_back_as_vecs() {
    // Asking for no over-alignment allocates exactly a `Vec<u32>`'s layout, so even a buffer
    // Vortex built itself can be given away.
    let buffer = Buffer::<u32>::copy_from_preferred_aligned(
        [1, 2, 3, 4, 5, 6, 7, 8],
        Alignment::of::<u32>(),
        None,
    );
    let ptr = buffer.as_ptr();
    let vec = buffer
        .try_into_vec()
        .expect("T-aligned region has a Vec layout");
    assert_eq!(vec.as_ptr(), ptr);
    assert_eq!(vec, [1, 2, 3, 4, 5, 6, 7, 8]);
}

#[test]
fn over_aligned_buffers_copy_into_vecs() {
    let buffer = Buffer::<u32>::copy_from([1, 2, 3, 4]);
    assert!(buffer.is_aligned(Alignment::DEFAULT_ALIGNMENT));
    assert!(buffer.clone().try_into_vec().is_err());
    assert_eq!(buffer.into_vec(), [1, 2, 3, 4]);
}

#[test]
fn foreign_buffer_is_mutable_when_unique() {
    let owner = vec![1u32, 2, 3];
    let ptr = owner.as_ptr();
    let buffer = crate::BufferMut::from_owner(owner).freeze();
    let shared = buffer.clone();
    let Err(buffer) = buffer.try_into_mut() else {
        panic!("a shared foreign buffer must not become mutable")
    };
    drop(shared);
    let mut buffer = buffer.try_into_mut().expect("unique again");
    buffer[0] = 10;
    assert_eq!(buffer.as_ptr(), ptr, "writes land in the adopted memory");
    assert_eq!(buffer.as_slice(), [10, 2, 3]);
}

#[test]
fn from_owner_is_read_only() {
    let shared: Arc<[i32]> = Arc::from(vec![1, 2, 3]);
    let buffer = Buffer::from_owner(Arc::clone(&shared));
    assert_eq!(buffer.as_ptr(), shared.as_ptr());
    assert!(buffer.try_into_mut().is_err());
}

#[test]
fn from_static_is_zero_copy_and_never_unique() {
    static VALUES: [u16; 4] = [1, 2, 3, 4];
    let buffer = Buffer::from_static(&VALUES);
    assert_eq!(buffer.as_ptr(), VALUES.as_ptr());
    assert!(
        !buffer.is_unique(),
        "a borrowed static has no refcount to be unique on"
    );
    assert!(buffer.try_into_mut().is_err());
}

#[test]
fn try_into_mut_recovers_spare_capacity() {
    let mut buffer = crate::BufferMut::<u8>::with_capacity(64);
    buffer.extend_from_slice(&[1, 2, 3]);
    let frozen = buffer.freeze();
    let thawed = frozen.try_into_mut().expect("unique");
    assert_eq!(thawed.len(), 3);
    assert!(thawed.capacity() >= 64);
}

#[test]
fn into_bytes_of_a_bytes_slice_reuses_the_owner() {
    let bytes = Bytes::from(vec![0u8, 1, 2, 3, 4, 5, 6, 7]);
    let ptr = bytes.as_ptr();
    let buffer = ByteBuffer::from(bytes).slice(2..6);
    let bytes = buffer.into_bytes();
    // A slice of an adopted `Bytes` comes back as a slice of the same `Bytes`, not a wrapper.
    assert_eq!(bytes.as_ptr(), ptr.wrapping_add(2));
    assert_eq!(bytes.as_ref(), &[2, 3, 4, 5]);
}

#[test]
fn into_bytes_of_our_own_region_does_not_copy() {
    let buffer = ByteBuffer::copy_from([1u8, 2, 3, 4]);
    let ptr = buffer.as_ptr();
    let bytes = buffer.into_bytes();
    assert_eq!(bytes.as_ptr(), ptr);
    assert_eq!(bytes.as_ref(), &[1, 2, 3, 4]);
}

// --- Zero-sized types --------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Marker;

#[test]
fn zst_buffers_count_without_bytes() {
    let buffer = Buffer::from(vec![Marker; 5]);
    assert_eq!(buffer.len(), 5);
    assert!(buffer.as_bytes().is_empty());
    assert_eq!(buffer.as_slice(), &[Marker; 5]);
    assert_eq!(buffer.slice(1..3).len(), 2);
    assert_eq!(buffer.clone().into_iter().count(), 5);
    assert_eq!(buffer.clone(), buffer);
    assert!(buffer.is_aligned(Alignment::of::<Marker>()));
}

#[test]
fn zst_buffers_round_trip_through_vecs() {
    let buffer = Buffer::from(vec![(); 7]);
    let thawed = buffer.try_into_mut().expect("nothing to share");
    assert_eq!(thawed.len(), 7);
    assert_eq!(thawed.capacity(), usize::MAX);
    let vec = thawed.try_into_vec().expect("a ZST Vec owns nothing");
    assert_eq!(vec.len(), 7);
}

#[test]
fn zst_from_empty_byte_buffer() {
    let buffer = Buffer::<()>::from_byte_buffer(ByteBuffer::empty());
    assert!(buffer.is_empty());
}

#[test]
#[should_panic(expected = "cannot be reinterpreted as the zero-sized type")]
fn zst_from_non_empty_byte_buffer_panics() {
    drop(Buffer::<()>::from_byte_buffer(ByteBuffer::copy_from([1u8])));
}
