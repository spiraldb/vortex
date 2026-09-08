// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

#![cfg(test)]

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use rstest::rstest;
use vortex_buffer::Alignment;
use vortex_buffer::Buffer;
use vortex_buffer::BufferMut;
use vortex_buffer::ByteBuffer;
use vortex_error::VortexResult;
use vortex_error::vortex_err;

#[repr(align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AlignedZst;

#[rstest]
#[case(BufferMut::empty())]
#[case(BufferMut::with_capacity(4))]
#[case(BufferMut::zeroed(3))]
#[case(BufferMut::copy_from([AlignedZst; 2]))]
#[case(BufferMut::from_iter([AlignedZst; 2]))]
#[case(BufferMut::from_trusted_len_iter([AlignedZst; 2].into_iter()))]
fn zero_sized_elements_grow(#[case] mut buffer: BufferMut<AlignedZst>) {
    let initial_len = buffer.len();
    assert_eq!(buffer.capacity(), usize::MAX);
    buffer.reserve(100);
    buffer.push(AlignedZst);
    buffer.push_n(AlignedZst, 2);
    buffer.extend_from_slice(&[AlignedZst; 3]);
    buffer.extend([AlignedZst; 4]);
    buffer.extend_trusted([AlignedZst; 5].into_iter());
    assert_eq!(buffer.as_slice(), vec![AlignedZst; initial_len + 15]);
    assert_eq!(buffer.spare_capacity_mut().len(), usize::MAX - buffer.len());
    assert!(Alignment::of::<AlignedZst>().is_ptr_aligned(buffer.as_ptr()));
    buffer.truncate(2);
    let clone = buffer.clone();
    buffer.clear();
    assert_eq!(clone.len(), 2);
    assert_eq!(buffer.capacity(), usize::MAX);
    assert!(buffer.is_empty());
}

#[rstest]
#[case(Buffer::empty())]
#[case(Buffer::from(vec![AlignedZst; 4]))]
#[case(BufferMut::zeroed(4).freeze())]
fn zero_sized_freeze_thaw(#[case] buffer: Buffer<AlignedZst>) -> VortexResult<()> {
    let len = buffer.len();
    let mut mutable = buffer
        .try_into_mut()
        .map_err(|_| vortex_err!("unique buffer"))?;
    assert_eq!(mutable.len(), len);
    assert_eq!(mutable.capacity(), usize::MAX);
    mutable.push(AlignedZst);
    let frozen = mutable.freeze();
    let view = frozen.slice(1..);
    let mut shared_copy = frozen.clone().into_mut();
    shared_copy.push(AlignedZst);
    assert_eq!(frozen.len(), len + 1);
    assert_eq!(shared_copy.len(), len + 2);
    drop(frozen);
    let mut unique_view = view
        .try_into_mut()
        .map_err(|_| vortex_err!("unique slice"))?;
    assert_eq!(unique_view.len(), len);
    assert_eq!(unique_view.capacity(), usize::MAX);
    unique_view.push(AlignedZst);
    assert_eq!(unique_view.len(), len + 1);
    Ok(())
}

#[test]
fn zero_sized_vec_retains_drop_glue() {
    static DROPS: AtomicUsize = AtomicUsize::new(0);
    #[repr(align(64))]
    struct DropZst;
    impl Drop for DropZst {
        fn drop(&mut self) {
            DROPS.fetch_add(1, Ordering::Relaxed);
        }
    }

    let buffer = Buffer::from(vec![DropZst, DropZst, DropZst]);
    assert_eq!(buffer.len(), 3);
    assert!(Alignment::of::<DropZst>().is_ptr_aligned(buffer.as_ptr()));
    let view = buffer.slice(1..);
    drop(buffer);
    assert_eq!(view.len(), 2);
    assert_eq!(DROPS.load(Ordering::Relaxed), 0);
    drop(view);
    assert_eq!(DROPS.load(Ordering::Relaxed), 3);
}

#[test]
fn zero_sized_byte_views_are_empty() {
    let buffer = BufferMut::<AlignedZst>::zeroed(3);
    let frozen = buffer.clone().freeze();
    assert!(frozen.as_bytes().is_empty());
    assert!(frozen.clone().into_bytes().is_empty());
    assert!(frozen.into_byte_buffer().is_empty());
    let mut bytes = buffer.into_byte_buffer();
    assert!(bytes.is_empty());
    assert_eq!(bytes.capacity(), 0);
    bytes.push(42);
    assert_eq!(bytes.as_slice(), [42]);
}

#[test]
#[should_panic(expected = "buffer capacity overflow")]
fn zero_sized_reserve_checks_length_overflow() {
    let mut buffer = BufferMut::<()>::zeroed(1);
    buffer.reserve(usize::MAX);
}

#[test]
#[should_panic(expected = "slice_ref subset must be contained in the buffer")]
fn zero_sized_slice_ref_rejects_longer_subset() {
    let original = Buffer::<()>::zeroed(4);
    let short = original.slice(..2);
    short.slice_ref(original.as_slice());
}

#[rstest]
#[case(false)]
#[case(true)]
#[should_panic(expected = "cannot infer a zero-sized element count from bytes")]
fn bytes_cannot_determine_zero_sized_length(#[case] use_byte_buffer: bool) {
    if use_byte_buffer {
        Buffer::<()>::from_byte_buffer(ByteBuffer::empty());
    } else {
        Buffer::<()>::from_bytes_aligned(Bytes::new(), Alignment::of::<()>());
    }
}

#[rstest]
#[case(0)]
#[case(1)]
#[case(4)]
fn zero_sized_owned_iterator(#[case] len: usize) {
    let mut iter = Buffer::<AlignedZst>::zeroed(len).into_iter();
    assert_eq!(iter.size_hint(), (len, Some(len)));
    for remaining in (0..len).rev() {
        assert_eq!(iter.next(), Some(AlignedZst));
        assert_eq!(iter.len(), remaining);
    }
    assert_eq!(iter.next(), None);
    assert_eq!(iter.next(), None);
    assert_eq!(iter.size_hint(), (0, Some(0)));
}

#[test]
fn zero_sized_owned_iterator_retains_count_when_collected() {
    let buffer = Buffer::<AlignedZst>::zeroed(5).slice(1..);
    let buffer = Buffer::from_trusted_len_iter(buffer.into_iter());
    assert_eq!(buffer.len(), 4);
    assert_eq!(buffer.into_iter().collect::<Vec<_>>(), vec![AlignedZst; 4]);
}

#[test]
fn zero_sized_owned_iterator_supports_maximum_length() {
    let mut iter = Buffer::<AlignedZst>::zeroed(usize::MAX).into_iter();
    assert_eq!(iter.len(), usize::MAX);
    assert_eq!(iter.next(), Some(AlignedZst));
    assert_eq!(iter.len(), usize::MAX - 1);
}
