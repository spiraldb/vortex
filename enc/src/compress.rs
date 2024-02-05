use std::collections::HashSet;
use std::fmt::Debug;

use once_cell::sync::Lazy;

use crate::array::{Array, ArrayRef, Encoding, EncodingId};

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
    is_sample: bool,
}

impl<'a> CompressCtx<'a> {
    pub fn new(options: &'a CompressConfig) -> Self {
        Self {
            options,
            depth: 0,
            is_sample: false,
        }
    }

    pub fn compress(&self, arr: &dyn Array) -> ArrayRef {
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

    pub fn for_sample(&self) -> Self {
        let mut cloned = self.clone();
        cloned.is_sample = true;
        cloned
    }

    fn next_level(&self) -> Self {
        let mut cloned = self.clone();
        cloned.depth += 1;
        cloned
    }

    pub fn options(&self) -> &CompressConfig {
        self.options
    }

    pub fn is_sample(&self) -> bool {
        self.is_sample
    }
}

impl Default for CompressCtx<'_> {
    fn default() -> Self {
        Self::new(&DEFAULT_COMPRESS_CONFIG)
    }
}
