// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Adaptive binary range coder (LZMA's), coding each symbol as a path through a binary tree of
//! adaptive bit probabilities. `order1` conditions the tree on the previous symbol. Models reset
//! at every block so blocks stay independently decodable, and decoding is inherently serial.

use crate::SymbolCoder;
use crate::bits::bits;

const PROB_BITS: u32 = 11;
const ONE: u16 = 1 << PROB_BITS;

struct Enc {
    low: u64,
    range: u32,
    cache: u8,
    cache_size: u64,
    out: Vec<u8>,
}

impl Enc {
    fn new() -> Self {
        Self { low: 0, range: u32::MAX, cache: 0, cache_size: 1, out: Vec::new() }
    }

    fn shift_low(&mut self) {
        if (self.low as u32) < 0xFF00_0000 || (self.low >> 32) != 0 {
            let carry = (self.low >> 32) as u8;
            let mut temp = self.cache;
            loop {
                self.out.push(temp.wrapping_add(carry));
                temp = 0xFF;
                self.cache_size -= 1;
                if self.cache_size == 0 {
                    break;
                }
            }
            self.cache = ((self.low as u32) >> 24) as u8;
        }
        self.cache_size += 1;
        self.low = u64::from(self.low as u32) << 8 & 0xFFFF_FFFF;
    }

    #[inline]
    fn bit(&mut self, p: &mut u16, bit: u32, shift: u32) {
        let bound = (self.range >> PROB_BITS) * u32::from(*p);
        if bit == 0 {
            self.range = bound;
            *p += (ONE - *p) >> shift;
        } else {
            self.low += u64::from(bound);
            self.range -= bound;
            *p -= *p >> shift;
        }
        while self.range < 1 << 24 {
            self.range <<= 8;
            self.shift_low();
        }
    }

    fn finish(mut self) -> Vec<u8> {
        for _ in 0..5 {
            self.shift_low();
        }
        self.out
    }
}

struct Dec<'a> {
    code: u32,
    range: u32,
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Dec<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        let mut code = 0u32;
        for &b in &bytes[1..5] {
            code = (code << 8) | u32::from(b);
        }
        Self { code, range: u32::MAX, bytes, pos: 5 }
    }

    #[inline(always)]
    fn bit(&mut self, p: &mut u16, shift: u32) -> u32 {
        let bound = (self.range >> PROB_BITS) * u32::from(*p);
        let bit = if self.code < bound {
            self.range = bound;
            *p += (ONE - *p) >> shift;
            0
        } else {
            self.code -= bound;
            self.range -= bound;
            *p -= *p >> shift;
            1
        };
        if self.range < 1 << 24 {
            self.range <<= 8;
            self.code = (self.code << 8) | u32::from(self.bytes[self.pos]);
            self.pos += 1;
        }
        bit
    }
}

pub struct AdaptiveBinary {
    pub order1: bool,
    pub shift: u32,
    depth: u32,
    alphabet: usize,
}

impl AdaptiveBinary {
    pub fn new(order1: bool, shift: u32) -> Self {
        Self { order1, shift, depth: 0, alphabet: 0 }
    }

    fn contexts(&self) -> usize {
        let tree = 1usize << self.depth;
        if self.order1 { tree * self.alphabet } else { tree }
    }
}

impl SymbolCoder for AdaptiveBinary {
    fn name(&self) -> String {
        format!("adaptive-o{}", u32::from(self.order1))
    }

    fn prepare(&mut self, counts: &[u32]) -> usize {
        self.alphabet = counts.len();
        self.depth = bits(counts.len().saturating_sub(1) as u64).max(1);
        // Only the alphabet size is stored.
        2
    }

    fn encode(&self, syms: &[u16]) -> Vec<u8> {
        let tree = 1usize << self.depth;
        let mut probs = vec![ONE / 2; self.contexts()];
        let mut enc = Enc::new();
        let mut prev = 0usize;
        for &s in syms {
            let base = if self.order1 { prev * tree } else { 0 };
            let mut ctx = 1usize;
            for i in (0..self.depth).rev() {
                let b = (u32::from(s) >> i) & 1;
                enc.bit(&mut probs[base + ctx], b, self.shift);
                ctx = ctx * 2 + b as usize;
            }
            prev = s as usize;
        }
        enc.finish()
    }

    fn decode(&self, bytes: &[u8], out: &mut [u16]) {
        let tree = 1usize << self.depth;
        let mut probs = vec![ONE / 2; self.contexts()];
        let mut dec = Dec::new(bytes);
        let mut prev = 0usize;
        for o in out.iter_mut() {
            let base = if self.order1 { prev * tree } else { 0 };
            let mut ctx = 1usize;
            for _ in 0..self.depth {
                let b = dec.bit(&mut probs[base + ctx], self.shift);
                ctx = ctx * 2 + b as usize;
            }
            let s = ctx - tree;
            *o = s as u16;
            prev = s;
        }
    }
}
