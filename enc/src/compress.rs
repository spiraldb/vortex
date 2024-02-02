use std::collections::HashSet;
use std::fmt::Debug;

use once_cell::sync::Lazy;

use crate::array::{Array, ArrayKind, ArrayRef, Encoding, EncodingId};

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
        encodings: &[&'static dyn CompressedEncoding],
        disabled_encodings: &[&'static dyn CompressedEncoding],
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

pub trait Compressible {
    fn compress(&self, opts: CompressCtx) -> ArrayRef;
}

impl<T> Compressible for &T
where
    T: Compressible,
{
    fn compress(&self, opts: CompressCtx) -> ArrayRef {
        (*self).compress(opts)
    }
}

pub trait CompressorFor<T: Array> {
    fn compress(array: &T) -> ArrayRef;
}

pub type Compressor = fn(&dyn Array, CompressCtx) -> ArrayRef;

pub trait CompressedEncoding: Encoding + 'static {
    fn compressor(&self, array: &dyn Array, config: &CompressConfig)
        -> Option<&'static Compressor>;
}

pub fn compress(arr: &dyn Array, opts: CompressCtx) -> ArrayRef {
    match ArrayKind::from(arr) {
        ArrayKind::Primitive(p) => compress_array(p, opts),
        ArrayKind::ZigZag(p) => compress_array(p, opts),
        _ => dyn_clone::clone_box(arr),
    }
}

pub fn compress_array<T: AsRef<dyn Array> + Compressible>(arr: T, opts: CompressCtx) -> ArrayRef {
    if arr.as_ref().is_empty() {
        return dyn_clone::clone_box(arr.as_ref());
    }

    if opts.depth == opts.options.max_depth {
        return dyn_clone::clone_box(arr.as_ref());
    }

    arr.compress(opts)
}
