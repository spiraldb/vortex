use std::collections::HashSet;
use std::fmt::Debug;

use log::info;
use once_cell::sync::Lazy;

use crate::array::constant::ConstantEncoding;
use crate::array::{Array, ArrayRef, Encoding, EncodingId, ENCODINGS};

pub trait ArrayCompression {
    fn compress(&self, ctx: CompressCtx) -> ArrayRef;
}

pub trait EncodingCompression {
    fn compressor(&self, array: &dyn Array, config: &CompressConfig)
        -> Option<&'static Compressor>;
}

pub type Compressor = fn(&dyn Array, CompressCtx) -> ArrayRef;

#[derive(Debug, Clone)]
pub struct CompressConfig {
    block_size: u32,
    sample_size: u16,
    sample_count: u16,
    max_depth: u8,
    // TODO(ngates): can each encoding define their own configs?
    pub ree_average_run_threshold: f32,
    encodings: HashSet<&'static EncodingId>,
    disabled_encodings: HashSet<&'static EncodingId>,
}

impl Default for CompressConfig {
    fn default() -> Self {
        // TODO(ngates): we should ensure that sample_size * sample_count <= block_size
        Self {
            block_size: 65536,
            sample_size: 64,
            sample_count: 10,
            max_depth: 3,
            ree_average_run_threshold: 2.0,
            encodings: HashSet::new(),
            disabled_encodings: HashSet::new(),
        }
    }
}

impl CompressConfig {
    pub fn new(
        encodings: HashSet<&'static EncodingId>,
        disabled_encodings: HashSet<&'static EncodingId>,
    ) -> Self {
        Self {
            encodings,
            disabled_encodings,
            ..CompressConfig::default()
        }
    }

    pub fn from_encodings(
        encodings: &[&'static dyn Encoding],
        disabled_encodings: &[&'static dyn Encoding],
    ) -> Self {
        Self::new(
            encodings.iter().map(|e| e.id()).collect(),
            disabled_encodings.iter().map(|e| e.id()).collect(),
        )
    }

    pub fn is_enabled(&self, kind: &EncodingId) -> bool {
        (self.encodings.is_empty() || self.encodings.contains(kind))
            && !self.disabled_encodings.contains(kind)
    }
}

static DEFAULT_COMPRESS_CONFIG: Lazy<CompressConfig> = Lazy::new(CompressConfig::default);

#[derive(Debug, Clone)]
pub struct CompressCtx<'a> {
    options: &'a CompressConfig,
    depth: u8,
}

impl<'a> CompressCtx<'a> {
    pub fn new(options: &'a CompressConfig) -> Self {
        Self { options, depth: 0 }
    }

    pub fn compress(&self, arr: &dyn Array) -> ArrayRef {
        info!(
            "Compressing array {:?} with depth {}",
            arr.encoding(),
            self.depth
        );

        if arr.is_empty() {
            return dyn_clone::clone_box(arr);
        }

        if self.depth >= self.options.max_depth {
            return dyn_clone::clone_box(arr);
        }

        if let Some(compression) = arr.compression() {
            let compressed = compression.compress(self.clone());
            // TODO(robert): Forward stats from arr to compressed
            self.next_level().compress(compressed.as_ref())
        } else {
            dyn_clone::clone_box(arr)
        }
    }

    fn next_level(&self) -> Self {
        let mut cloned = self.clone();
        cloned.depth += 1;
        cloned
    }

    pub fn options(&self) -> &CompressConfig {
        self.options
    }
}

impl Default for CompressCtx<'_> {
    fn default() -> Self {
        Self::new(&DEFAULT_COMPRESS_CONFIG)
    }
}

pub fn sampled_compression(
    array: &dyn Array,
    ctx: CompressCtx,
    sampler: fn(array: &dyn Array, sample_size: u16, sample_count: u16) -> ArrayRef,
) -> ArrayRef {
    // First, we try constant compression
    if let Some(compressor) = ConstantEncoding.compressor(array, ctx.options()) {
        return compressor(array, ctx);
    }

    let candidate_compressors: Vec<&Compressor> = ENCODINGS
        .iter()
        .filter_map(|encoding| encoding.compression())
        .filter_map(|compression| compression.compressor(array, ctx.options()))
        .collect();

    if candidate_compressors.is_empty() {
        return dyn_clone::clone_box(array);
    }

    if array.len() < ctx.options.block_size as usize {
        // We're either in a sample or we're operating over a sufficiently small array.
        let (_, compressed_sample) = candidate_compressors.iter().fold(
            (array.nbytes(), None),
            |(compressed_bytes, curr_best), compressor| {
                let compressed = compressor(array, ctx.clone());

                if compressed.nbytes() < compressed_bytes {
                    (compressed.nbytes(), Some(compressed))
                } else {
                    (compressed_bytes, curr_best)
                }
            },
        );

        return compressed_sample.unwrap_or_else(|| dyn_clone::clone_box(array));
    }

    // Otherwise, take the sample and try each compressor on it.
    let sample = sampler(array, ctx.options.sample_size, ctx.options.sample_count);
    let compression_ratios: Vec<(&Compressor, f32)> = candidate_compressors
        .iter()
        .map(|compressor| {
            (
                *compressor,
                compressor(array, ctx.clone()).nbytes() as f32 / sample.nbytes() as f32,
            )
        })
        .collect();

    compression_ratios
        .into_iter()
        .filter(|(_, ratio)| *ratio < 1.0)
        .min_by(|(_, first_ratio), (_, second_ratio)| first_ratio.total_cmp(second_ratio))
        .map(|(compressor, _)| compressor(array, ctx))
        .unwrap_or_else(|| dyn_clone::clone_box(array))
}
