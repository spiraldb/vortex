// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use futures::FutureExt;
use futures::TryFutureExt;
use futures::future;
use futures::future::BoxFuture;
use futures::future::Shared;
use vortex_error::SharedVortexResult;
use vortex_error::VortexError;
use vortex_error::VortexResult;
use vortex_error::vortex_panic;
use vortex_mask::Mask;

/// A future that resolves to a mask.
#[derive(Clone)]
pub struct MaskFuture {
    inner: Inner,
    len: usize,
}

#[derive(Clone)]
enum Inner {
    /// A mask that was known when the future was made.
    Ready(Mask),
    Pending(Shared<BoxFuture<'static, SharedVortexResult<Mask>>>),
}

impl MaskFuture {
    /// Create a new MaskFuture from a future that returns a mask.
    pub fn new<F>(len: usize, fut: F) -> Self
    where
        F: Future<Output = VortexResult<Mask>> + Send + 'static,
    {
        Self {
            inner: Inner::Pending(
                fut.inspect(move |r| {
                    if let Ok(mask) = r
                        && mask.len() != len {
                            vortex_panic!("MaskFuture created with future that returned mask of incorrect length (expected {}, got {})", len, mask.len());
                        }
                })
                .map_err(Arc::new)
                .boxed()
                .shared(),
            ),
            len,
        }
    }

    /// Returns the length of the mask.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns true if the mask is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Create a MaskFuture from a ready mask.
    pub fn ready(mask: Mask) -> Self {
        Self {
            len: mask.len(),
            inner: Inner::Ready(mask),
        }
    }

    /// Returns true if the mask is available without waiting.
    ///
    /// This is the case for [`MaskFuture::ready`] and its slices, whose mask was known when they
    /// were made. Polling such a future returns the mask at once, so a reader can check it
    /// before it requests any data, for example to skip rows that are not selected.
    pub fn is_ready(&self) -> bool {
        matches!(self.inner, Inner::Ready(_))
    }

    /// Create a MaskFuture that resolves to a mask with all values set to true.
    pub fn new_true(row_count: usize) -> Self {
        Self::ready(Mask::new_true(row_count))
    }

    /// Create a MaskFuture that resolves to a slice of the original mask.
    pub fn slice(&self, range: Range<usize>) -> Self {
        // Slicing the whole mask is the identity. Cloning shares the existing future instead of
        // allocating another boxed, shared one that would await it only to hand the mask back.
        if range.start == 0 && range.end == self.len {
            return self.clone();
        }

        match &self.inner {
            Inner::Ready(mask) => Self::ready(mask.slice(range)),
            Inner::Pending(inner) => {
                let inner = inner.clone();
                Self::new(range.len(), async move { Ok(inner.await?.slice(range)) })
            }
        }
    }

    /// Run `f` on the result once it is available.
    ///
    /// The result is then pending until polled, even for a ready mask, so that `f` runs lazily.
    pub fn inspect(
        self,
        f: impl FnOnce(&SharedVortexResult<Mask>) + 'static + Send + Sync,
    ) -> Self {
        let len = self.len;
        let shared = match self.inner {
            Inner::Ready(mask) => future::ready(Ok(mask)).boxed().shared(),
            Inner::Pending(inner) => inner,
        };

        Self {
            inner: Inner::Pending(shared.inspect(f).boxed().shared()),
            len,
        }
    }
}

impl Future for MaskFuture {
    type Output = VortexResult<Mask>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        match &mut self.inner {
            Inner::Ready(mask) => std::task::Poll::Ready(Ok(mask.clone())),
            Inner::Pending(inner) => inner.poll_unpin(cx).map_err(VortexError::from),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering;

    use vortex_buffer::BitBuffer;

    use super::*;

    /// Slicing resolves to the same mask the equivalent [`Mask::slice`] would produce, for both
    /// the full range (which takes the identity fast path) and a sub-range.
    #[test]
    fn slice_resolves_to_sliced_mask() -> VortexResult<()> {
        futures::executor::block_on(async {
            let mask = Mask::from_buffer(BitBuffer::from_iter([true, false, true, true, false]));
            let fut = MaskFuture::ready(mask.clone());

            let full = fut.slice(0..mask.len());
            assert_eq!(full.len(), mask.len());
            assert_eq!(full.await?, mask);

            let partial = fut.slice(0..mask.len() - 1);
            assert_eq!(partial.len(), mask.len() - 1);
            assert_eq!(partial.await?, mask.slice(0..mask.len() - 1));
            Ok(())
        })
    }

    #[test]
    fn ready_masks_and_their_slices_are_ready() -> VortexResult<()> {
        let mask = Mask::from_buffer(BitBuffer::from_iter([true, false, true, false]));
        let fut = MaskFuture::ready(mask.clone());
        assert!(fut.is_ready());
        assert!(MaskFuture::new_true(3).is_ready());

        let slice = fut.slice(1..3);
        assert!(slice.is_ready());
        // A ready future returns its mask on the first poll.
        assert_eq!(slice.now_or_never().transpose()?, Some(mask.slice(1..3)));
        Ok(())
    }

    #[test]
    fn pending_masks_and_their_slices_are_not_ready() -> VortexResult<()> {
        let mask = Mask::from_buffer(BitBuffer::from_iter([true, false, true]));
        let pending = MaskFuture::new(3, future::ready(Ok(mask.clone())));
        assert!(!pending.is_ready());

        let slice = pending.slice(0..2);
        assert!(!slice.is_ready());
        assert_eq!(futures::executor::block_on(slice)?, mask.slice(0..2));
        Ok(())
    }

    #[test]
    fn inspect_runs_lazily_on_a_ready_mask() -> VortexResult<()> {
        let seen = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&seen);
        let fut = MaskFuture::new_true(2).inspect(move |result| {
            assert!(result.is_ok());
            flag.store(true, Ordering::SeqCst);
        });

        assert!(!fut.is_ready());
        assert!(!seen.load(Ordering::SeqCst));
        assert_eq!(futures::executor::block_on(fut)?, Mask::new_true(2));
        assert!(seen.load(Ordering::SeqCst));
        Ok(())
    }
}
