// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Static rANS with `LANES` interleaved 32-bit states sharing one stream of 16-bit words
//! (value i uses lane i % LANES). The final states are the block header.

use crate::SymbolCoder;
use crate::freq_table_bytes;
use crate::normalize;

const RANS_L: u32 = 1 << 16;

pub struct Rans {
    pub lanes: usize,
    pub scale_bits: u32,
    freq: Vec<u32>,
    cum: Vec<u32>,
    /// Per slot: symbol, freq, cum packed for one load.
    slots: Vec<(u16, u16, u16)>,
}

impl Rans {
    pub fn new(lanes: usize, scale_bits: u32) -> Self {
        Self { lanes, scale_bits, freq: vec![], cum: vec![], slots: vec![] }
    }
}

impl SymbolCoder for Rans {
    fn name(&self) -> String {
        format!("rans{}x{}", self.scale_bits, self.lanes)
    }

    fn prepare(&mut self, counts: &[u32]) -> usize {
        self.freq = normalize(counts, self.scale_bits);
        self.cum = Vec::with_capacity(counts.len());
        let mut c = 0;
        self.slots = vec![(0, 0, 0); 1 << self.scale_bits];
        for (s, &f) in self.freq.iter().enumerate() {
            self.cum.push(c);
            for slot in c..c + f {
                // freq can be 1 << scale_bits only for a single symbol; store it as 0 (wraps)
                // and special-case nothing: x>>scale * 65536 would overflow u16 so keep u32 math.
                self.slots[slot as usize] = (s as u16, f as u16, c as u16);
            }
            c += f;
        }
        freq_table_bytes(&self.freq)
    }

    fn encode(&self, syms: &[u16]) -> Vec<u8> {
        let mut states = vec![RANS_L; self.lanes];
        let mut words: Vec<u16> = Vec::with_capacity(syms.len());
        for (i, &s) in syms.iter().enumerate().rev() {
            let x = &mut states[i % self.lanes];
            let f = self.freq[s as usize];
            let x_max = (u64::from(RANS_L >> self.scale_bits) << 16) * u64::from(f);
            if u64::from(*x) >= x_max {
                words.push(*x as u16);
                *x >>= 16;
            }
            *x = ((*x / f) << self.scale_bits) + (*x % f) + self.cum[s as usize];
        }
        let mut out = Vec::with_capacity(self.lanes * 4 + words.len() * 2);
        for x in &states {
            out.extend_from_slice(&x.to_le_bytes());
        }
        for w in words.iter().rev() {
            out.extend_from_slice(&w.to_le_bytes());
        }
        out
    }

    fn decode(&self, bytes: &[u8], out: &mut [u16]) {
        let lanes = self.lanes;
        let mut states: Vec<u32> = (0..lanes)
            .map(|l| u32::from_le_bytes(bytes[l * 4..l * 4 + 4].try_into().unwrap()))
            .collect();
        let mut pos = lanes * 4;
        let mask = (1u32 << self.scale_bits) - 1;
        let single = self.freq.iter().any(|&f| f == 1 << self.scale_bits);
        for (i, o) in out.iter_mut().enumerate() {
            let x = &mut states[i % lanes];
            let slot = *x & mask;
            let (s, f, c) = self.slots[slot as usize];
            let f = if single { 1u32 << self.scale_bits } else { u32::from(f) };
            *x = f * (*x >> self.scale_bits) + slot - u32::from(c);
            if *x < RANS_L {
                *x = (*x << 16) | u32::from(u16::from_le_bytes([bytes[pos], bytes[pos + 1]]));
                pos += 2;
            }
            *o = s;
        }
    }
}
