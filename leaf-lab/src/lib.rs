// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Leaf entropy-coder lab: alternative symbol coders for the bin ids of a binned leaf, and
//! parametric codes for leaves coded directly.
#![allow(clippy::all)]

pub mod bins;
pub mod bits;
pub mod golomb;
pub mod huff;
pub mod rans;
pub mod rc;
pub mod tunstall;

/// A static or adaptive coder over a small symbol alphabet, used per independently decodable
/// block. `prepare` builds the model for a chunk and returns its serialized table size.
pub trait SymbolCoder {
    fn name(&self) -> String;
    fn prepare(&mut self, counts: &[u32]) -> usize;
    /// Serialized block, including every per-block header the decoder needs.
    fn encode(&self, syms: &[u16]) -> Vec<u8>;
    /// `bytes` is padded with at least 16 readable bytes past the block.
    fn decode(&self, bytes: &[u8], out: &mut [u16]);
}

/// Bytes to serialize normalized frequencies: a presence bitmap over the alphabet plus each
/// nonzero frequency in its own bit length with a 4-bit length prefix.
pub fn freq_table_bytes(norm: &[u32]) -> usize {
    let bits: usize = norm.len()
        + norm
            .iter()
            .filter(|&&f| f > 0)
            .map(|&f| 4 + bits::bits(u64::from(f)) as usize)
            .sum::<usize>();
    bits.div_ceil(8)
}

/// Normalize `counts` to sum to `1 << scale_bits`, keeping every nonzero count at least 1.
pub fn normalize(counts: &[u32], scale_bits: u32) -> Vec<u32> {
    let total: u64 = counts.iter().map(|&c| u64::from(c)).sum();
    let m = 1u64 << scale_bits;
    let mut norm: Vec<u32> = counts
        .iter()
        .map(|&c| {
            if c == 0 {
                0
            } else {
                ((u64::from(c) * m) / total).max(1) as u32
            }
        })
        .collect();
    let mut sum: i64 = norm.iter().map(|&f| i64::from(f)).sum();
    // Hand the rounding error to the symbols that lose least, by cost delta per unit.
    while sum != m as i64 {
        let up = sum < m as i64;
        let mut best = usize::MAX;
        let mut best_cost = f64::INFINITY;
        for (i, &c) in counts.iter().enumerate() {
            if c == 0 || (!up && norm[i] <= 1) {
                continue;
            }
            let f = f64::from(norm[i]);
            let delta = if up {
                -(f64::from(c)) * ((f + 1.0) / f).log2()
            } else {
                f64::from(c) * (f / (f - 1.0)).log2()
            };
            if delta < best_cost {
                best_cost = delta;
                best = i;
            }
        }
        let step = ((sum - m as i64).abs() / 64).max(1).min(if up { i64::MAX } else { i64::from(norm[best]) - 1 });
        if up {
            norm[best] += step as u32;
            sum += step;
        } else {
            norm[best] -= step as u32;
            sum -= step;
        }
    }
    norm
}

/// Order-0 entropy in bits of a symbol stream with these counts.
pub fn entropy_bits(counts: &[u32]) -> f64 {
    let n: f64 = counts.iter().map(|&c| f64::from(c)).sum();
    counts
        .iter()
        .filter(|&&c| c > 0)
        .map(|&c| -f64::from(c) * (f64::from(c) / n).log2())
        .sum()
}
