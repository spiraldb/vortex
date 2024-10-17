use std::fmt::Debug;
use std::sync::Arc;

use compressors::bitpacked::BITPACK_WITH_PATCHES;
use compressors::fsst::FSSTCompressor;
use compressors::CompressorRef;
use lazy_static::lazy_static;
use vortex::encoding::EncodingRef;
use vortex::Context;
use vortex_alp::{ALPEncoding, ALPRDEncoding};
use vortex_bytebool::ByteBoolEncoding;
use vortex_datetime_parts::DateTimePartsEncoding;
use vortex_dict::DictEncoding;
use vortex_fastlanes::{BitPackedEncoding, DeltaEncoding, FoREncoding};
use vortex_fsst::FSSTEncoding;
use vortex_roaring::{RoaringBoolEncoding, RoaringIntEncoding};
use vortex_runend::RunEndEncoding;
use vortex_runend_bool::RunEndBoolEncoding;
use vortex_zigzag::ZigZagEncoding;

use crate::compressors::alp::ALPCompressor;
use crate::compressors::date_time_parts::DateTimePartsCompressor;
use crate::compressors::dict::DictCompressor;
use crate::compressors::r#for::FoRCompressor;
use crate::compressors::roaring_bool::RoaringBoolCompressor;
use crate::compressors::roaring_int::RoaringIntCompressor;
use crate::compressors::runend::DEFAULT_RUN_END_COMPRESSOR;
use crate::compressors::sparse::SparseCompressor;
use crate::compressors::zigzag::ZigZagCompressor;

#[cfg(feature = "arbitrary")]
pub mod arbitrary;
pub mod compressors;
mod constants;
mod sampling;
mod sampling_compressor;

pub use sampling_compressor::*;

lazy_static! {
    pub static ref DEFAULT_COMPRESSORS: [CompressorRef<'static>; 11] = [
        &ALPCompressor as CompressorRef,
        &BITPACK_WITH_PATCHES,
        &DateTimePartsCompressor,
        &DEFAULT_RUN_END_COMPRESSOR,
        // &DeltaCompressor,
        &DictCompressor,
        &FoRCompressor,
        &FSSTCompressor,
        &RoaringBoolCompressor,
        &RoaringIntCompressor,
        &SparseCompressor,
        &ZigZagCompressor,
    ];

    pub static ref FASTEST_COMPRESSORS: [CompressorRef<'static>; 7] = [
        &BITPACK_WITH_PATCHES,
        &DateTimePartsCompressor,
        &DEFAULT_RUN_END_COMPRESSOR, // replace with FastLanes RLE
        &DictCompressor, // replace with FastLanes Dictionary
        &FoRCompressor,
        &SparseCompressor,
        &ZigZagCompressor,
    ];

    pub static ref ALL_COMPRESSORS_CONTEXT: Arc<Context> = Arc::new(Context::default().with_encodings([
        &ALPEncoding as EncodingRef,
        &ByteBoolEncoding,
        &DateTimePartsEncoding,
        &DictEncoding,
        &BitPackedEncoding,
        &DeltaEncoding,
        &FoREncoding,
        &FSSTEncoding,
        &RoaringBoolEncoding,
        &RoaringIntEncoding,
        &RunEndEncoding,
        &RunEndBoolEncoding,
        &ZigZagEncoding,
        &ALPRDEncoding,
    ]));
}

#[derive(Debug, Clone)]
pub struct ScanPerfConfig {
    /// MiB per second of download throughput
    mib_per_second: f64,
    /// Compression ratio to assume when calculating decompression time
    assumed_compression_ratio: f64,
}

impl ScanPerfConfig {
    pub fn download_time_ms(&self, nbytes: u64) -> f64 {
        Self::millis_from_throughput_and_size(self.mib_per_second, nbytes)
    }

    pub fn starting_value(&self, nbytes: u64) -> f64 {
        self.download_time_ms(nbytes) * 1.1
    }

    fn millis_from_throughput_and_size(mib_per_second: f64, nbytes: u64) -> f64 {
        const MS_PER_SEC: f64 = 1000.0;
        const BYTES_PER_MIB: f64 = (1 << 20) as f64;
        (MS_PER_SEC / mib_per_second) * (nbytes as f64 / BYTES_PER_MIB)
    }
}

impl Default for ScanPerfConfig {
    fn default() -> Self {
        Self {
            mib_per_second: 500.0,           // 500 MiB/s for object storage
            assumed_compression_ratio: 10.0, // 10:1 ratio of uncompressed data size to compressed data size
        }
    }
}

#[derive(Debug, Clone)]
pub enum Objective {
    MinSize,
    ScanPerf(ScanPerfConfig),
}

impl Objective {
    pub fn starting_value(&self, nbytes: u64) -> f64 {
        match self {
            Objective::MinSize => 1.0,
            Objective::ScanPerf(config) => config.starting_value(nbytes),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompressConfig {
    /// Size of each sample slice
    sample_size: u16,
    /// Number of sample slices
    sample_count: u16,
    /// Random number generator seed
    rng_seed: u64,

    // Maximum depth of compression tree
    max_cost: u8,
    // Are we minimizing size or maximizing performance?
    objective: Objective,
    /// Penalty in bytes per compression level
    overhead_bytes_per_array: u64,

    // Target chunk size in bytes
    target_block_bytesize: usize,
    // Target chunk size in row count
    target_block_size: usize,
}

impl Default for CompressConfig {
    fn default() -> Self {
        let kib = 1 << 10;
        let mib = 1 << 20;
        Self {
            // Sample length should always be multiple of 1024
            sample_size: 64,
            sample_count: 16,
            max_cost: 3,
            objective: Objective::ScanPerf(ScanPerfConfig::default()),
            overhead_bytes_per_array: 64,
            target_block_bytesize: 16 * mib,
            target_block_size: 64 * kib,
            rng_seed: 0,
        }
    }
}
