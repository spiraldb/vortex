// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! LSB-first bit writer and reader.

pub struct BitWriter {
    pub bytes: Vec<u8>,
    acc: u64,
    n: u32,
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl BitWriter {
    pub fn new() -> Self {
        Self { bytes: Vec::new(), acc: 0, n: 0 }
    }

    /// Write the low `width` bits of `v` (width <= 64).
    #[inline]
    pub fn write(&mut self, v: u64, width: u32) {
        if width == 0 {
            return;
        }
        if width > 56 {
            self.write(v & 0xFFFF_FFFF, 32);
            self.write(v >> 32, width - 32);
            return;
        }
        let v = if width == 64 { v } else { v & ((1u64 << width) - 1) };
        self.acc |= v << self.n;
        self.n += width;
        while self.n >= 8 {
            self.bytes.push(self.acc as u8);
            self.acc >>= 8;
            self.n -= 8;
        }
    }

    pub fn bit_len(&self) -> usize {
        self.bytes.len() * 8 + self.n as usize
    }

    pub fn finish(mut self) -> Vec<u8> {
        if self.n > 0 {
            self.bytes.push(self.acc as u8);
        }
        self.bytes
    }
}

/// Reads from a byte slice that must be followed by at least 8 readable bytes (callers pad).
pub struct BitReader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> BitReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    #[inline(always)]
    pub fn peek(&self) -> u64 {
        let byte = self.pos >> 3;
        let w = u64::from_le_bytes(self.bytes[byte..byte + 8].try_into().unwrap());
        w >> (self.pos & 7)
    }

    #[inline(always)]
    pub fn skip(&mut self, n: u32) {
        self.pos += n as usize;
    }

    /// Read `width` bits (width <= 64).
    #[inline(always)]
    pub fn read(&mut self, width: u32) -> u64 {
        if width == 0 {
            return 0;
        }
        if width > 56 {
            let lo = self.read(32);
            let hi = self.read(width - 32);
            return lo | (hi << 32);
        }
        let v = self.peek() & ((1u64 << width) - 1);
        self.pos += width as usize;
        v
    }

    pub fn bit_pos(&self) -> usize {
        self.pos
    }
}

pub fn pad(mut v: Vec<u8>) -> Vec<u8> {
    v.extend_from_slice(&[0; 16]);
    v
}

/// Bits needed to store `v`.
pub fn bits(v: u64) -> u32 {
    64 - v.leading_zeros()
}
