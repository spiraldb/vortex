use std::collections::HashSet;
use std::fmt::Debug;

use once_cell::sync::Lazy;

use crate::array::{Array, ArrayEncoding, Encoding, EncodingId};

mod constant;
mod primitive;
mod ree;

#[derive(Debug, Clone)]
pub struct CompressConfig {
    pub block_size: u32,
    pub sample_size: u16,
    pub sample_count: u16,
    pub max_depth: u8,
    pub ree_average_run_threshold: f32,
    encodings: HashSet<&'static EncodingId>,
    disabled_encodings: HashSet<&'static EncodingId>,
}

impl Default for CompressConfig {
    fn default() -> Self {
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
        encodings: &[&'static dyn CompressedEncoding],
        disabled_encodings: &[&'static dyn CompressedEncoding],
    ) -> Self {
        Self {
            encodings: encodings.iter().map(|e| e.id()).collect(),
            disabled_encodings: disabled_encodings.iter().map(|e| e.id()).collect(),
            ..CompressConfig::default()
        }
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
    is_sample: bool,
}

impl CompressCtx<'_> {
    pub fn for_sample(&self) -> Self {
        let mut cloned = self.clone();
        cloned.is_sample = true;
        cloned
    }

    pub fn next_level(&self) -> Self {
        let mut cloned = self.clone();
        cloned.depth += 1;
        cloned
    }
}

impl Default for CompressCtx<'_> {
    fn default() -> Self {
        Self {
            options: &DEFAULT_COMPRESS_CONFIG,
            depth: 0,
            is_sample: false,
        }
    }
}

pub trait Compressible {
    fn compress(&self, opts: CompressCtx) -> Array;
}

pub type Compressor = fn(&Array, CompressCtx) -> Array;

pub trait CompressedEncoding: Encoding + 'static {
    fn compressor(&self, array: &Array, config: &CompressConfig) -> Option<&'static Compressor>;
}

pub fn compress(arr: &Array, opts: CompressCtx) -> Array {
    if arr.is_empty() {
        return arr.clone();
    }

    if opts.depth == opts.options.max_depth {
        return arr.clone();
    }

    // Otherwise, we invoke the compression strategy for the array.
    match arr {
        Array::Primitive(a) => a.compress(opts.clone()),
        _ => unimplemented!(),
    }
}
