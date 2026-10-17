// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Out-of-place lane kernels: read from an [`IndexedSource`] and write into a
//! caller-provided `&mut [MaybeUninit<R>]`.

use std::mem::MaybeUninit;
use std::ops::BitOrAssign;

use crate::bit::Bitmap;

use crate::lane_kernels::CHUNK_LEN;
use crate::lane_kernels::source::IndexedSource;

/// Extension trait providing out-of-place lane-kernel methods on any [`IndexedSource`].
///
/// All methods have default implementations and are inherited via the blanket
/// `impl<S: IndexedSource> IndexedSourceExt for S` below. Bring the trait into
/// scope (`use rowfn_kernels::lane_kernels::IndexedSourceExt;`) to call
/// them with method syntax: `values.try_map_masked_into(&mask, &mut out, f)`.
///
/// Callbacks implement [`Fn`] because each lane must be independent. A callback that mutates
/// captured state introduces a loop-carried dependency that can prevent vectorization.
pub trait IndexedSourceExt: IndexedSource + Sized {
    /// Fallible map with mask-aware error attribution. `f` returns `Option<R>`;
    /// `None` indicates a per-lane failure (e.g. range overflow on a narrowing cast).
    ///
    /// **Null-lane failures are filtered automatically.** The closure is called on
    /// every lane regardless of validity; if a null lane's stored value causes `f(v)`
    /// to return `None`, the kernel does *not* propagate that as `Err`. The per-lane
    /// `is_none()` flags are bit-packed into a `u64` at the lane's position, then
    /// AND-combined with the chunk's validity bitmap, null-lane bits vanish.
    ///
    /// The closure shape is the same as [`try_map_into`] (`Fn(Item) -> Option<R>`);
    /// the mask parameter is what makes this kernel mask-aware. Callers that need to
    /// distinguish null lanes inside the closure (e.g. to short-circuit an expensive
    /// computation) should construct their own per-lane validity check externally; for
    /// the common case, the kernel's automatic filter is sufficient.
    ///
    /// On failure returns `Err(failing_lane_index)`. Lanes whose `f` returned `None`
    /// write `R::default()` into `out`, but the contents of `out` must not be relied
    /// upon when this function returns `Err`.
    ///
    /// [`try_map_into`]: IndexedSourceExt::try_map_into
    ///
    /// # Panics
    ///
    /// Panics if `self.len() != mask.len()` or `out.len() != self.len()`.
    #[inline]
    fn try_map_masked_into<R, F>(
        self,
        mask: &impl Bitmap,
        out: &mut [MaybeUninit<R>],
        f: F,
    ) -> Result<(), usize>
    where
        R: Copy + Default,
        F: Fn(Self::Item) -> Option<R>,
    {
        #[allow(clippy::inline_always)]
        #[inline(always)]
        fn chunk<S, R, F>(
            values: &S,
            out: &mut [MaybeUninit<R>],
            f: &F,
            src_chunk: u64,
            base: usize,
            count: usize,
        ) -> Option<usize>
        where
            S: IndexedSource,
            R: Copy + Default,
            F: Fn(S::Item) -> Option<R>,
        {
            let mut fail_bits: u64 = 0;
            for bit_idx in 0..count {
                let idx = base + bit_idx;
                // SAFETY: caller guarantees base + count <= len.
                let val = unsafe { values.get_unchecked(idx) };
                let opt = f(val);
                fail_bits |= (opt.is_none() as u64) << bit_idx;
                let result = opt.unwrap_or_default();
                unsafe { out.get_unchecked_mut(idx).write(result) };
            }
            let valid_failures = fail_bits & src_chunk;
            (valid_failures != 0).then_some(base + valid_failures.trailing_zeros() as usize)
        }

        let mask = mask.bitmap();
        let values = self;
        let len = values.len();
        assert_eq!(len, mask.len(), "values and mask must have the same length");
        assert_eq!(out.len(), len, "out must have the same length as values");

        let chunks = mask.chunks();
        let chunks_count = len / 64;
        let remainder = len % 64;

        for (chunk_idx, src_chunk) in chunks.iter().enumerate() {
            if let Some(idx) = chunk(&values, out, &f, src_chunk, chunk_idx * 64, 64) {
                return Err(idx);
            }
        }
        if remainder != 0
            && let Some(idx) = chunk(
                &values,
                out,
                &f,
                chunks.remainder_bits(),
                chunks_count * 64,
                remainder,
            )
        {
            return Err(idx);
        }
        Ok(())
    }

    /// Apply `f(value)` lane-by-lane with **no validity awareness at all**, every
    /// closure invocation is treated as "happened", regardless of whether the lane
    /// is null. Use this only when the input is known non-nullable.
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != self.len()`.
    #[inline]
    fn map_into<R, F>(self, out: &mut [MaybeUninit<R>], f: F)
    where
        F: Fn(Self::Item) -> R,
    {
        #[allow(clippy::inline_always)]
        #[inline(always)]
        fn chunk<S, R, F>(values: &S, out: &mut [MaybeUninit<R>], f: &F, base: usize, count: usize)
        where
            S: IndexedSource,
            F: Fn(S::Item) -> R,
        {
            for bit_idx in 0..count {
                let idx = base + bit_idx;
                // SAFETY: caller guarantees base + count <= len.
                let val = unsafe { values.get_unchecked(idx) };
                unsafe { out.get_unchecked_mut(idx).write(f(val)) };
            }
        }

        let values = self;
        let len = values.len();
        assert_eq!(out.len(), len, "out must have the same length as values");

        let chunks_count = len / CHUNK_LEN;
        let remainder = len % CHUNK_LEN;

        for chunk_idx in 0..chunks_count {
            chunk(&values, out, &f, chunk_idx * CHUNK_LEN, CHUNK_LEN);
        }
        if remainder != 0 {
            chunk(&values, out, &f, chunks_count * CHUNK_LEN, remainder);
        }
    }

    /// Collect a dense source with one straight loop.
    ///
    /// This shape gives a host's primitive output binding a simple loop when chunking prevents
    /// useful vectorization. The callback still runs once per row in increasing order.
    #[inline]
    fn map_into_linear<R, F>(self, out: &mut [MaybeUninit<R>], f: F)
    where
        F: Fn(Self::Item) -> R,
    {
        assert_eq!(out.len(), self.len(), "output and source must have the same length");
        for (index, slot) in out.iter_mut().enumerate() {
            // SAFETY: the loop indexes only the validated source length.
            slot.write(f(unsafe { self.get_unchecked(index) }));
        }
    }

    /// Apply the predicate `f(value)` lane-by-lane and bit-pack the results into
    /// `words`, LSB-first, 64 lanes per `u64`.
    ///
    /// This is the kernel shape behind comparison operators: each lane read is an
    /// independent indexed load (drive two columns via [`LaneZip`]) and the 64
    /// per-lane booleans of a chunk reduce into a single word with `OR + shift`,
    /// which the autovectorizer lowers to a vector compare plus movemask.
    ///
    /// Words are written with `=` (not `|=`), so `words` need not be
    /// zero-initialised. Bits at positions `>= self.len()` in the last word are
    /// written as zero.
    ///
    /// Like [`map_into`], this kernel has no validity awareness; pair the packed
    /// bits with a separately computed validity mask.
    ///
    /// [`LaneZip`]: crate::lane_kernels::LaneZip
    /// [`map_into`]: IndexedSourceExt::map_into
    ///
    /// # Panics
    ///
    /// Panics if `words.len() < self.len().div_ceil(64)`.
    #[inline]
    fn map_bits_into<F>(self, words: &mut [u64], f: F)
    where
        F: Fn(Self::Item) -> bool,
    {
        #[allow(clippy::inline_always)]
        #[inline(always)]
        fn chunk<S, F>(values: &S, f: &F, base: usize, count: usize) -> u64
        where
            S: IndexedSource,
            F: Fn(S::Item) -> bool,
        {
            let mut packed: u64 = 0;
            for bit_idx in 0..count {
                // SAFETY: caller guarantees base + count <= len.
                let val = unsafe { values.get_unchecked(base + bit_idx) };
                packed |= (f(val) as u64) << bit_idx;
            }
            packed
        }

        let values = self;
        let len = values.len();
        let num_words = len.div_ceil(64);
        assert!(
            words.len() >= num_words,
            "words slice has {} entries, need at least {num_words}",
            words.len(),
        );

        let full = len / 64;
        let remainder = len % 64;

        for word_idx in 0..full {
            words[word_idx] = chunk(&values, &f, word_idx * 64, 64);
        }
        if remainder != 0 {
            words[full] = chunk(&values, &f, full * 64, remainder);
        }
    }

    /// Split value/failure map with **no validity awareness at all**: write every lane's value
    /// unconditionally and OR-reduce its failure evidence into the return.
    ///
    /// The fastest checked shape, running at the speed of the unchecked [`map_into`] in exchange
    /// for reporting only _that_ some lane failed and never exiting early. Re-run the now known
    /// cold input through [`try_map_into`] or [`try_map_masked_into`] to attribute the failure or
    /// to drop the null-lane ones. The evidence reduces inside the kernel because a captured `&mut`
    /// becomes a loop-carried memory dependence that blocks vectorization.
    ///
    /// Anything other than [`Default`] means failure, and `bool` is the ordinary `Fail`. Wider
    /// words exist for operations where deriving a `bool` costs the vectorization it guards.
    /// **`Fail` must be no wider than `R`**, asserted below, or the reduction rather than the
    /// operation decides how many lanes fit in a vector.
    ///
    /// [`map_into`]: IndexedSourceExt::map_into
    /// [`try_map_into`]: IndexedSourceExt::try_map_into
    /// [`try_map_masked_into`]: IndexedSourceExt::try_map_masked_into
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != self.len()`.
    #[inline]
    fn map_checked_into<R, Fail, Apply>(self, out: &mut [MaybeUninit<R>], apply: Apply) -> Fail
    where
        Fail: Copy + Default + BitOrAssign,
        Apply: Fn(Self::Item) -> (R, Fail),
    {
        const {
            assert!(
                size_of::<Fail>() <= size_of::<R>(),
                "failure evidence must be no wider than the value, or it bounds the vector width"
            )
        };

        let values = self;
        let len = values.len();
        assert_eq!(out.len(), len, "out must have the same length as values");

        let mut failed = Fail::default();
        for idx in 0..len {
            // SAFETY: idx < len by the loop bound, and out.len() == len.
            let val = unsafe { values.get_unchecked(idx) };

            let (result, failure) = apply(val);
            failed |= failure;

            // SAFETY: idx < len == out.len().
            unsafe { out.get_unchecked_mut(idx).write(result) };
        }
        failed
    }

    /// Fallible map with **no validity awareness at all**, every `None` returned
    /// by the closure is treated as a failure, even at null lanes.
    ///
    /// # Use this only for non-nullable inputs.
    ///
    /// For nullable inputs with a fallible closure, use [`try_map_masked_into`],
    /// it has the same value-only closure shape (and the same perf win) but
    /// **correctly suppresses null-lane failures** via per-chunk
    /// `fail_bits & mask_chunk`.
    ///
    /// Using this kernel on a nullable input where a null lane's stored value
    /// would cause `f` to return `None` will produce a spurious `Err`. This is a
    /// correctness footgun on purpose, the name and this doc are how the API
    /// signals "you must know your input has no nulls."
    ///
    /// On failure returns `Err(failing_lane_index)`.
    ///
    /// [`try_map_masked_into`]: IndexedSourceExt::try_map_masked_into
    ///
    /// # Panics
    ///
    /// Panics if `out.len() != self.len()`.
    #[inline]
    fn try_map_into<R, F>(self, out: &mut [MaybeUninit<R>], f: F) -> Result<(), usize>
    where
        R: Copy + Default,
        F: Fn(Self::Item) -> Option<R>,
    {
        /// Returns `true` if any lane in `[base, base+count)` failed (OR-reduced);
        /// the cold attribution path is called at the kernel level so it can be
        /// inlined separately for full vs remainder.
        #[allow(clippy::inline_always)]
        #[inline(always)]
        fn chunk<S, R, F>(
            values: &S,
            out: &mut [MaybeUninit<R>],
            f: &F,
            base: usize,
            count: usize,
        ) -> bool
        where
            S: IndexedSource,
            R: Copy + Default,
            F: Fn(S::Item) -> Option<R>,
        {
            let mut fail_acc: u64 = 0;
            for bit_idx in 0..count {
                let idx = base + bit_idx;
                // SAFETY: caller guarantees base + count <= len.
                let val = unsafe { values.get_unchecked(idx) };
                let opt = f(val);
                fail_acc |= opt.is_none() as u64;
                let result = opt.unwrap_or_default();
                unsafe { out.get_unchecked_mut(idx).write(result) };
            }
            fail_acc != 0
        }

        let values = self;
        let len = values.len();
        assert_eq!(out.len(), len, "out must have the same length as values");

        let chunks_count = len / CHUNK_LEN;
        let remainder = len % CHUNK_LEN;

        for chunk_idx in 0..chunks_count {
            let base = chunk_idx * CHUNK_LEN;
            if chunk(&values, out, &f, base, CHUNK_LEN) {
                return Err(attribute_failure_no_mask(&values, base, CHUNK_LEN, &f));
            }
        }
        if remainder != 0 {
            let base = chunks_count * CHUNK_LEN;
            if chunk(&values, out, &f, base, remainder) {
                return Err(attribute_failure_no_mask(&values, base, remainder, &f));
            }
        }
        Ok(())
    }
}

impl<S: IndexedSource> IndexedSourceExt for S {}

/// Shared cold scan: walks a chunk, returns the first lane index where
/// `lane_fails(bit_idx, value)` returns `true`. Used by
/// [`attribute_failure_no_mask`].
///
/// Caller guarantees `base + chunk_len <= values.len()`.
#[cold]
#[inline(never)]
fn cold_scan<S>(
    values: &S,
    base: usize,
    chunk_len: usize,
    lane_fails: impl Fn(usize /* bit_idx */, S::Item) -> bool,
) -> usize
where
    S: IndexedSource,
{
    for bit_idx in 0..chunk_len {
        let idx = base + bit_idx;
        // SAFETY: caller guarantees idx < values.len().
        let val = unsafe { values.get_unchecked(idx) };
        if lane_fails(bit_idx, val) {
            return idx;
        }
    }
    unreachable!("cold_scan called without a failing lane")
}

/// Cold attribution for the no-mask variant. Replays `f` over the chunk to find
/// the first lane that returns `None`.
#[inline]
fn attribute_failure_no_mask<S, R, F>(values: &S, base: usize, chunk_len: usize, f: &F) -> usize
where
    S: IndexedSource,
    F: Fn(S::Item) -> Option<R>,
{
    cold_scan(values, base, chunk_len, |_bit_idx, val| f(val).is_none())
}
