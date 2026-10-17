// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Golomb-Rice and Exp-Golomb codes on values minus the block minimum, with one parameter per
//! miniblock. Each miniblock's bit offset is stored so miniblocks decode in parallel.

use crate::bits::BitReader;
use crate::bits::BitWriter;
use crate::bits::bits;

pub const MINI: usize = 128;
/// Unary prefixes at least this long escape to a raw value of the block's width.
const ESCAPE: u32 = 24;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Rice,
    ExpGolomb,
}

fn cost(kind: Kind, u: u64, k: u32, w: u32) -> u64 {
    match kind {
        Kind::Rice => {
            let q = u >> k;
            if q >= u64::from(ESCAPE) { u64::from(ESCAPE + w) } else { q + 1 + u64::from(k) }
        }
        Kind::ExpGolomb => {
            let z = bits((u >> k).saturating_add(1)) - 1;
            if z >= ESCAPE { u64::from(ESCAPE + w) } else { u64::from(2 * z + 1 + k) }
        }
    }
}

fn best_k(kind: Kind, vals: &[u64], w: u32) -> u32 {
    let mean = vals.iter().map(|&v| v as f64).sum::<f64>() / vals.len() as f64;
    let guess = (mean.max(1.0).log2() as i64).clamp(0, i64::from(w));
    let lo = ((guess - 3).max(0) as u32).min(63);
    let hi = ((guess + 2) as u32).min(w).min(63);
    (lo..=hi)
        .min_by_key(|&k| vals.iter().map(|&u| cost(kind, u, k, w)).sum::<u64>())
        .unwrap_or(0)
}

/// Encode one block. Header: min (8 bytes), width (1 byte), then per miniblock k (6 bits) and
/// its bit length (16 bits).
pub fn encode(kind: Kind, vals: &[u64]) -> Vec<u8> {
    let min = vals.iter().copied().min().unwrap_or(0);
    let w = bits(vals.iter().copied().max().unwrap_or(0) - min);
    let mut header = BitWriter::new();
    header.write(min, 64);
    header.write(u64::from(w), 8);
    let mut body = BitWriter::new();
    let mut deltas = Vec::with_capacity(MINI);
    for mini in vals.chunks(MINI) {
        deltas.clear();
        deltas.extend(mini.iter().map(|&v| v - min));
        let k = best_k(kind, &deltas, w);
        let start = body.bit_len();
        for &u in &deltas {
            match kind {
                Kind::Rice => {
                    let q = u >> k;
                    if q >= u64::from(ESCAPE) {
                        body.write((1 << ESCAPE) - 1, ESCAPE);
                        body.write(u, w);
                    } else {
                        body.write((1u64 << q) - 1, q as u32 + 1);
                        body.write(u, k);
                    }
                }
                Kind::ExpGolomb => {
                    let x = (u >> k).saturating_add(1);
                    let z = bits(x) - 1;
                    if z >= ESCAPE {
                        body.write(0, ESCAPE);
                        body.write(u, w);
                    } else {
                        body.write(1 << z, z + 1);
                        body.write(x, z);
                        body.write(u, k);
                    }
                }
            }
        }
        header.write(u64::from(k), 6);
        header.write((body.bit_len() - start) as u64, 16);
    }
    let mut out = header.finish();
    out.extend_from_slice(&body.finish());
    out
}

pub fn decode(kind: Kind, bytes: &[u8], out: &mut [u64]) {
    let mut header = BitReader::new(bytes);
    let min = header.read(64);
    let w = header.read(8) as u32;
    let n_mini = out.len().div_ceil(MINI);
    let base = (72 + n_mini * 22).div_ceil(8);
    let mut body = BitReader::new(&bytes[base..]);
    for mini in out.chunks_mut(MINI) {
        let k = header.read(6) as u32;
        header.read(16);
        for o in mini.iter_mut() {
            let u = match kind {
                Kind::Rice => {
                    let q = body.peek().trailing_ones();
                    if q >= ESCAPE {
                        body.skip(ESCAPE);
                        body.read(w)
                    } else {
                        body.skip(q + 1);
                        (u64::from(q) << k) | body.read(k)
                    }
                }
                Kind::ExpGolomb => {
                    let z = body.peek().trailing_zeros();
                    if z >= ESCAPE {
                        body.skip(ESCAPE);
                        body.read(w)
                    } else {
                        body.skip(z + 1);
                        let x = (1u64 << z) | body.read(z);
                        ((x - 1) << k) | body.read(k)
                    }
                }
            };
            *o = u + min;
        }
    }
}
