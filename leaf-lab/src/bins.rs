// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! pco bins, trained exactly as vortex-entropy-bins trains them (Classic mode, no delta, level 8,
//! at most 64 bins), and the split of each value into a bin id and a row-order packed offset.

use pco::ChunkConfig;
use pco::DeltaSpec;
use pco::ModeSpec;
use pco::metadata::DynBins;
use pco::wrapped::FileCompressor;

use crate::bits::BitReader;
use crate::bits::BitWriter;

pub const MAX_BINS: usize = 64;

#[derive(Clone, Debug)]
pub struct Bins {
    pub lowers: Vec<u64>,
    pub widths: Vec<u32>,
}

impl Bins {
    pub fn train(latents: &[u64], level: usize) -> Self {
        let mut lvl = level;
        loop {
            let b = Self::train_at(latents, lvl);
            if b.lowers.len() <= MAX_BINS || lvl == 0 {
                return b;
            }
            lvl -= 1;
        }
    }

    fn train_at(latents: &[u64], level: usize) -> Self {
        let config = ChunkConfig::default()
            .with_compression_level(level)
            .with_mode_spec(ModeSpec::Classic)
            .with_delta_spec(DeltaSpec::NoOp);
        let fc = FileCompressor::default();
        let cc = fc.chunk_compressor(latents, &config).unwrap();
        let DynBins::U64(bins) = &cc.meta().per_latent_var.primary.bins else {
            panic!("u64 bins")
        };
        let mut bins: Vec<(u64, u32)> = bins.iter().map(|b| (b.lower, b.offset_bits)).collect();
        bins.sort_unstable();
        Self { lowers: bins.iter().map(|b| b.0).collect(), widths: bins.iter().map(|b| b.1).collect() }
    }

    pub fn len(&self) -> usize {
        self.lowers.len()
    }

    pub fn bin_of(&self, v: u64) -> usize {
        self.lowers.partition_point(|&l| l <= v) - 1
    }

    /// Serialized bins: lowers as varint gaps, widths one byte each.
    pub fn metadata_bytes(&self) -> usize {
        let mut prev = 0u64;
        self.lowers
            .iter()
            .map(|&l| {
                let gap = l - prev;
                prev = l;
                (crate::bits::bits(gap).max(1) as usize).div_ceil(7) + 1
            })
            .sum()
    }

    /// Bin ids, and the offsets packed in row order at their bin's width.
    pub fn split(&self, latents: &[u64]) -> (Vec<u16>, Vec<u8>) {
        let mut ids = Vec::with_capacity(latents.len());
        let mut w = BitWriter::new();
        for &v in latents {
            let b = self.bin_of(v);
            ids.push(b as u16);
            w.write(v - self.lowers[b], self.widths[b]);
        }
        (ids, w.finish())
    }

    pub fn merge(&self, ids: &[u16], offsets: &[u8], out: &mut [u64]) {
        let mut r = BitReader::new(offsets);
        for (o, &id) in out.iter_mut().zip(ids) {
            let id = id as usize;
            *o = self.lowers[id] + r.read(self.widths[id]);
        }
    }
}

/// Order-preserving u64 latent of an integer (sign bit flipped for signed types).
pub fn latent(v: i128, signed: bool) -> u64 {
    if signed { (v as i64 as u64) ^ (1 << 63) } else { v as u64 }
}
