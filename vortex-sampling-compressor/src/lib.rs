use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};

use compressors::bitpacked::BITPACK_WITH_PATCHES;
use compressors::fsst::FSSTCompressor;
use compressors::struct_::StructCompressor;
use lazy_static::lazy_static;
use log::{debug, info, warn};
use rand::rngs::StdRng;
use rand::SeedableRng;
use vortex::array::{Chunked, ChunkedArray, Constant};
use vortex::compress::{check_dtype_unchanged, check_validity_unchanged, CompressionStrategy};
use vortex::compute::slice;
use vortex::encoding::EncodingRef;
use vortex::validity::Validity;
use vortex::{Array, ArrayDType, ArrayDef, IntoArray, IntoCanonical};
use vortex_error::{VortexExpect as _, VortexResult};

use crate::compressors::alp::ALPCompressor;
use crate::compressors::constant::ConstantCompressor;
use crate::compressors::date_time_parts::DateTimePartsCompressor;
use crate::compressors::dict::DictCompressor;
use crate::compressors::r#for::FoRCompressor;
use crate::compressors::roaring_bool::RoaringBoolCompressor;
use crate::compressors::roaring_int::RoaringIntCompressor;
use crate::compressors::runend::DEFAULT_RUN_END_COMPRESSOR;
use crate::compressors::sparse::SparseCompressor;
use crate::compressors::zigzag::ZigZagCompressor;
use crate::compressors::{CompressedArray, CompressionTree, CompressorRef, EncodingCompressor};
use crate::sampling::stratified_slices;

#[cfg(feature = "arbitrary")]
pub mod arbitrary;
pub mod compressors;
mod constants;
mod sampling;

lazy_static! {
    pub static ref DEFAULT_COMPRESSORS: [CompressorRef<'static>; 13] = [
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
        &StructCompressor,
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
}

#[derive(Debug, Clone)]
pub enum Objective {
    MinSize,
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
            objective: Objective::MinSize,
            overhead_bytes_per_array: 64,
            target_block_bytesize: 16 * mib,
            target_block_size: 64 * kib,
            rng_seed: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SamplingCompressor<'a> {
    compressors: HashSet<CompressorRef<'a>>,
    options: CompressConfig,

    path: Vec<String>,
    depth: u8,
    /// A set of encodings disabled for this ctx.
    disabled_compressors: HashSet<CompressorRef<'a>>,
}

impl Display for SamplingCompressor<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}|{}]", self.depth, self.path.join("."))
    }
}

impl CompressionStrategy for SamplingCompressor<'_> {
    #[allow(clippy::same_name_method)]
    fn compress(&self, array: &Array) -> VortexResult<Array> {
        Self::compress(self, array, None).map(compressors::CompressedArray::into_array)
    }

    fn used_encodings(&self) -> HashSet<EncodingRef> {
        self.compressors
            .iter()
            .flat_map(|c| c.used_encodings())
            .collect()
    }
}

impl Default for SamplingCompressor<'_> {
    fn default() -> Self {
        Self::new(HashSet::from(*DEFAULT_COMPRESSORS))
    }
}

impl<'a> SamplingCompressor<'a> {
    pub fn new(compressors: HashSet<CompressorRef<'a>>) -> Self {
        Self::new_with_options(compressors, Default::default())
    }

    pub fn new_with_options(
        compressors: HashSet<CompressorRef<'a>>,
        options: CompressConfig,
    ) -> Self {
        Self {
            compressors,
            options,
            path: Vec::new(),
            depth: 0,
            disabled_compressors: HashSet::new(),
        }
    }

    pub fn named(&self, name: &str) -> Self {
        let mut cloned = self.clone();
        cloned.path.push(name.into());
        cloned
    }

    // Returns a new ctx used for compressing an auxiliary array.
    // In practice, this means resetting any disabled encodings back to the original config.
    pub fn auxiliary(&self, name: &str) -> Self {
        let mut cloned = self.clone();
        cloned.path.push(name.into());
        cloned.disabled_compressors = HashSet::new();
        cloned
    }

    pub fn for_compressor(&self, compression: &dyn EncodingCompressor) -> Self {
        let mut cloned = self.clone();
        cloned.depth += compression.cost();
        cloned
    }

    #[inline]
    pub fn options(&self) -> &CompressConfig {
        &self.options
    }

    pub fn excluding(&self, compressor: CompressorRef<'a>) -> Self {
        let mut cloned = self.clone();
        cloned.disabled_compressors.insert(compressor);
        cloned
    }

    pub fn including(&self, compressor: CompressorRef<'a>) -> Self {
        let mut cloned = self.clone();
        cloned.compressors.insert(compressor);
        cloned
    }

    #[allow(clippy::same_name_method)]
    pub fn compress(
        &self,
        arr: &Array,
        like: Option<&CompressionTree<'a>>,
    ) -> VortexResult<CompressedArray<'a>> {
        if arr.is_empty() {
            return Ok(CompressedArray::uncompressed(arr.clone()));
        }

        // Attempt to compress using the "like" array, otherwise fall back to sampled compression
        if let Some(l) = like {
            if let Some(compressed) = l.compress(arr, self) {
                let compressed = compressed?;

                check_validity_unchanged(arr, compressed.as_ref());
                check_dtype_unchanged(arr, compressed.as_ref());
                return Ok(compressed);
            } else {
                warn!(
                    "{} cannot find compressor to compress {} like {}",
                    self, arr, l
                );
            }
        }

        // Otherwise, attempt to compress the array
        let compressed = self.compress_array(arr)?;

        check_validity_unchanged(arr, compressed.as_ref());
        check_dtype_unchanged(arr, compressed.as_ref());
        Ok(compressed)
    }

    pub fn compress_validity(&self, validity: Validity) -> VortexResult<Validity> {
        match validity {
            Validity::Array(a) => Ok(Validity::Array(self.compress(&a, None)?.into_array())),
            a => Ok(a),
        }
    }

    fn compress_array(&self, arr: &Array) -> VortexResult<CompressedArray<'a>> {
        match arr.encoding().id() {
            Chunked::ID => {
                let chunked = ChunkedArray::try_from(arr)?;
                let less_chunked = chunked.rechunk(
                    self.options().target_block_bytesize,
                    self.options().target_block_size,
                )?;
                let mut compressed_chunks = Vec::with_capacity(less_chunked.nchunks());
                let mut previous: Option<CompressionTree> = None;
                for (index, chunk) in less_chunked.chunks().enumerate() {
                    if let Some(previous) = &previous {
                        debug!(
                            "using previous compression to save time: {} {}",
                            previous, chunk
                        );
                    }
                    let (compressed_chunk, tree) = self
                        .named(&format!("chunk-{}", index))
                        .compress(&chunk, previous.as_ref())?
                        .into_parts();
                    previous = tree;
                    compressed_chunks.push(compressed_chunk);
                }
                Ok(CompressedArray::uncompressed(
                    ChunkedArray::try_new(compressed_chunks, chunked.dtype().clone())?.into_array(),
                ))
            }
            Constant::ID => {
                // Not much better we can do than constant!
                Ok(CompressedArray::uncompressed(arr.clone()))
            }
            _ => {
                // Otherwise, we run sampled compression over pluggable encodings
                let mut rng = StdRng::seed_from_u64(self.options.rng_seed);
                let sampled = sampled_compression(arr, self, &mut rng)?;
                Ok(sampled.unwrap_or_else(|| CompressedArray::uncompressed(arr.clone())))
            }
        }
    }
}

fn sampled_compression<'a>(
    array: &Array,
    compressor: &SamplingCompressor<'a>,
    rng: &mut StdRng,
) -> VortexResult<Option<CompressedArray<'a>>> {
    // First, we try constant compression and shortcut any sampling.
    if let Some(cc) = ConstantCompressor.can_compress(array) {
        return cc.compress(array, None, compressor.clone()).map(Some);
    }

    let mut candidates: Vec<&dyn EncodingCompressor> = compressor
        .compressors
        .iter()
        .filter(|&encoding| !compressor.disabled_compressors.contains(encoding))
        .filter(|compression| {
            if compression.can_compress(array).is_some() {
                if compressor.depth + compression.cost() > compressor.options.max_cost {
                    debug!(
                        "{} skipping encoding {} due to depth",
                        compressor,
                        compression.id()
                    );
                    return false;
                }
                true
            } else {
                false
            }
        })
        .copied()
        .collect();
    debug!("{} candidates for {}: {:?}", compressor, array, candidates);

    if candidates.is_empty() {
        debug!(
            "{} no compressors for array with dtype: {} and encoding: {}",
            compressor,
            array.dtype(),
            array.encoding().id(),
        );
        return Ok(None);
    }

    // We prefer all other candidates to the array's own encoding.
    // This is because we assume that the array's own encoding is the least efficient, but useful
    // to destructure an array in the final stages of compression. e.g. VarBin would be DictEncoded
    // but then the dictionary itself remains a VarBin array. DictEncoding excludes itself from the
    // dictionary, but we still have a large offsets array that should be compressed.
    // TODO(ngates): we actually probably want some way to prefer dict encoding over other varbin
    //  encodings, e.g. FSST.
    if candidates.len() > 1 {
        candidates.retain(|&compression| compression.id() != array.encoding().id().as_ref());
    }

    if array.len()
        <= (compressor.options.sample_size as usize * compressor.options.sample_count as usize)
    {
        // We're either already within a sample, or we're operating over a sufficiently small array.
        return find_best_compression(candidates, array, compressor).map(Some);
    }

    // Take a sample of the array, then ask codecs for their best compression estimate.
    let sample = ChunkedArray::try_new(
        stratified_slices(
            array.len(),
            compressor.options.sample_size,
            compressor.options.sample_count,
            rng,
        )
        .into_iter()
        .map(|(start, stop)| slice(array, start, stop))
        .collect::<VortexResult<Vec<Array>>>()?,
        array.dtype().clone(),
    )?
    .into_canonical()?
    .into();

    find_best_compression(candidates, &sample, compressor)?
        .into_path()
        .map(|best_compressor| {
            info!("Compressing array {} with {}", array, best_compressor);
            best_compressor.compress_unchecked(array, compressor)
        })
        .transpose()
}

fn find_best_compression<'a>(
    candidates: Vec<&'a dyn EncodingCompressor>,
    sample: &Array,
    ctx: &SamplingCompressor<'a>,
) -> VortexResult<CompressedArray<'a>> {
    let mut best = None;
    let mut best_objective = 1.0;
    let mut best_objective_ratio = 1.0;
    // for logging
    let mut best_ratio = 1.0;
    let mut best_ratio_sample = None;

    for compression in candidates {
        debug!(
            "{} trying candidate {} for {}",
            ctx,
            compression.id(),
            sample
        );
        if compression.can_compress(sample).is_none() {
            continue;
        }
        let compressed_sample =
            compression.compress(sample, None, ctx.for_compressor(compression))?;

        let ratio = (compressed_sample.nbytes() as f64) / (sample.nbytes() as f64);
        let objective = objective_function(&compressed_sample, sample.nbytes(), ctx.options());

        // track the compression ratio, just for logging
        if ratio < best_ratio {
            best_ratio = ratio;

            // if we find one with a better compression ratio but worse objective value, save it
            // for debug logging later.
            if ratio < best_objective_ratio && objective >= best_objective {
                best_ratio_sample = Some(compressed_sample.clone());
            }
        }

        if objective < best_objective {
            best_objective = objective;
            best_objective_ratio = ratio;
            best = Some(compressed_sample);
        }

        debug!(
            "{} with {}: ratio ({}), objective fn value ({}); best so far: ratio ({}), objective fn value ({})",
            ctx,
            compression.id(),
            ratio,
            objective,
            best_ratio,
            best_objective
        );
    }

    let best = best.unwrap_or_else(|| CompressedArray::uncompressed(sample.clone()));
    if best_ratio < best_objective_ratio && best_ratio_sample.is_some() {
        let best_ratio_sample =
            best_ratio_sample.vortex_expect("already checked that this Option is Some");
        debug!(
            "{} best objective fn value ({}) has ratio {} from {}",
            ctx,
            best_objective,
            best_ratio,
            best.array().tree_display()
        );
        debug!(
            "{} best ratio ({}) has objective fn value {} from {}",
            ctx,
            best_ratio,
            best_objective,
            best_ratio_sample.array().tree_display()
        );
    }

    Ok(best)
}

fn objective_function(
    array: &CompressedArray,
    base_size_bytes: usize,
    config: &CompressConfig,
) -> f64 {
    let num_descendants = array
        .path()
        .as_ref()
        .map(CompressionTree::num_descendants)
        .unwrap_or(0) as u64;
    let overhead_bytes = num_descendants * config.overhead_bytes_per_array;
    let size_in_bytes = array.nbytes() as u64 + overhead_bytes;

    match &config.objective {
        Objective::MinSize => (size_in_bytes as f64) / (base_size_bytes as f64),
    }
}
