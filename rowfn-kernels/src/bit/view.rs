// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Validated borrowed access to packed bits without allocation or padding requirements.

/// A host bitmap that can lend its bytes without copying.
pub trait Bitmap {
    /// Borrow the logical bitmap, including its slice offset.
    fn bitmap(&self) -> BitmapView<'_>;
}

/// A stable logical range of bits in borrowed bytes, numbered least-significant bit first.
#[derive(Clone, Copy, Debug)]
pub struct BitmapView<'a> {
    bytes: &'a [u8],
    offset: usize,
    len: usize,
}

impl<'a> BitmapView<'a> {
    /// Borrow `len` bits beginning at `offset`. Panics if the range exceeds the byte slice.
    pub fn new(bytes: &'a [u8], offset: usize, len: usize) -> Self {
        assert!(offset <= usize::MAX - len, "bitmap range must fit in usize");
        let end = offset + len;
        assert!(end.div_ceil(8) <= bytes.len(), "bitmap range exceeds its bytes");

        Self { bytes, offset, len }
    }

    /// Logical bit count.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the logical range is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Read a logical bit. Panics if `index` is outside the view.
    #[inline]
    pub fn value(&self, index: usize) -> bool {
        assert!(index < self.len);
        // SAFETY: the assertion bounds the logical index by this validated view's length.
        unsafe { self.value_unchecked(index) }
    }

    /// Read a logical bit without checking its index.
    ///
    /// # Safety
    /// `index` must be less than this view's logical length.
    #[inline]
    pub unsafe fn value_unchecked(&self, index: usize) -> bool {
        let bit = self.offset + index;
        // SAFETY: construction checked offset + len against the byte slice. The caller bounds
        // index by len, so the addressed byte exists without padding or alignment requirements.
        let byte = unsafe { *self.bytes.get_unchecked(bit / 8) };
        byte & (1 << (bit % 8)) != 0
    }

    /// Borrow a subrange. Panics if the subrange exceeds the view.
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(offset <= self.len && len <= self.len - offset);

        Self::new(self.bytes, self.offset + offset, len)
    }

    /// Word traversal over the logical range.
    pub fn chunks(&self) -> BitmapChunks<'a> {
        BitmapChunks(*self)
    }

    fn word(&self, index: usize, count: usize) -> u64 {
        if count == 0 {
            return 0;
        }

        let bit = self.offset + index;
        let byte = bit / 8;
        let shift = bit % 8;
        let byte_count = (shift + count).div_ceil(8);
        let low_count = byte_count.min(8);
        let mut word = super::read_u64_le(&self.bytes[byte..byte + low_count]) >> shift;
        if byte_count > 8 {
            word |= u64::from(self.bytes[byte + 8]) << (64 - shift);
        }

        word & (u64::MAX >> (64 - count))
    }
}

impl Bitmap for BitmapView<'_> {
    fn bitmap(&self) -> BitmapView<'_> {
        *self
    }
}

/// Full words and a zero-padded final word of a borrowed bitmap.
pub struct BitmapChunks<'a>(BitmapView<'a>);

impl BitmapChunks<'_> {
    /// Iterate only complete 64-bit words.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = u64> + '_ {
        (0..self.0.len / 64).map(|index| self.0.word(index * 64, 64))
    }

    /// Return the remaining bits, with unused high bits set to zero.
    pub fn remainder_bits(&self) -> u64 {
        self.0.word(self.0.len / 64 * 64, self.0.len % 64)
    }
}

#[cfg(test)]
mod tests {
    use super::BitmapView;

    #[test]
    fn unpadded_slices_match_bit_access() {
        for offset in 0usize..16 {
            for len in [0, 1, 7, 8, 63, 64, 65, 127, 128, 129] {
                let bytes: Vec<_> = (0..(offset + len).div_ceil(8))
                    .map(|index| (index * 71 + 29) as u8)
                    .collect();
                let view = BitmapView::new(&bytes, offset, len);
                let chunks = view.chunks();
                let words: Vec<_> = chunks.iter().chain([chunks.remainder_bits()]).collect();
                for index in 0..len {
                    assert_eq!(view.value(index), words[index / 64] >> (index % 64) & 1 != 0);
                }
            }
        }
    }
}
