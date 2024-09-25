use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};

use compressors::fsst::FSSTCompressor;
use lazy_static::lazy_static;
use log::{debug, info, warn};
use rand::rngs::StdRng;
use rand::SeedableRng;
use vortex::array::{Chunked, ChunkedArray, Constant, Struct, StructArray};
use vortex::compress::{check_dtype_unchanged, check_validity_unchanged, CompressionStrategy};
use vortex::compute::slice;
use vortex::encoding::EncodingRef;
use vortex::validity::Validity;
use vortex::variants::StructArrayTrait;
use vortex::{Array, ArrayDType, ArrayDef, IntoArray, IntoCanonical};
use vortex_error::VortexResult;

// use crate::compressors::alp::ALPCompressor;
// use crate::compressors::bitpacked::BitPackedCompressor;
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
mod sampling;

lazy_static! {
    pub static ref ALL_COMPRESSORS: [CompressorRef<'static>; 9] = [
        // &ALPCompressor as CompressorRef,
        // &BitPackedCompressor,
        &DateTimePartsCompressor,
        &DEFAULT_RUN_END_COMPRESSOR,
        // TODO(robert): Implement minimal compute for DeltaArrays - scalar_at and slice
        // &DeltaCompressor,
        &DictCompressor,
        &FoRCompressor,
        &FSSTCompressor,
        &RoaringBoolCompressor,
        &RoaringIntCompressor,
        &SparseCompressor,
        &ZigZagCompressor,
    ];
}

#[derive(Debug, Clone)]
pub struct CompressConfig {
    sample_size: u16,
    sample_count: u16,
    max_depth: u8,
    target_block_bytesize: usize,
    target_block_size: usize,
    rng_seed: u64,
}

impl Default for CompressConfig {
    fn default() -> Self {
        let kib = 1 << 10;
        let mib = 1 << 20;
        Self {
            // Sample length should always be multiple of 1024
            sample_size: 128,
            sample_count: 8,
            max_depth: 3,
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
        Self::new(HashSet::from(*ALL_COMPRESSORS))
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

    pub fn is_enabled_on(&self, encoding: &dyn EncodingCompressor) -> bool {
        !self.disabled_compressors.contains(encoding)
    }

    pub fn named(&self, name: &str) -> Self {
        let mut cloned = self.clone();
        cloned.path.push(name.into());
        cloned
    }

    // Returns a new ctx used for compressing an auxiliary arrays.
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
        let maybe_compressed = like.map(|x| (x, x.compress(arr, self)));
        let compressed = match maybe_compressed {
            Some((_, Some(compressed_result))) => compressed_result?,
            Some((like, None)) => {
                warn!(
                    "{} cannot find compressor to compress {} like {}",
                    self, arr, like
                );
                self.compress_array(arr)?
            }
            None => self.compress_array(arr)?,
        };

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

    fn compress_all<I>(&self, iter: I) -> VortexResult<Vec<Array>>
    where
        I: Iterator<Item = Array>,
    {
        iter.map(|chunk| self.compress_array(&chunk).map(|x| x.into_array()))
            .collect()
    }

    fn compress_array(&self, arr: &Array) -> VortexResult<CompressedArray<'a>> {
        fn done<'b>(array: Array) -> VortexResult<CompressedArray<'b>> {
            Ok(CompressedArray::uncompressed(array))
        }

        match arr.encoding().id() {
            Chunked::ID => {
                let chunked = ChunkedArray::try_from(arr)?;
                let less_chunked = chunked.rechunk(
                    self.options().target_block_bytesize,
                    self.options().target_block_size,
                )?;
                let compressed_chunks = self.compress_all(less_chunked.chunks())?;
                done(
                    ChunkedArray::try_new(compressed_chunks, chunked.dtype().clone())?.into_array(),
                )
            }
            Constant::ID => {
                // Not much better we can do than constant!
                done(arr.clone())
            }
            Struct::ID => {
                // For struct arrays, we compress each field individually
                let strct = StructArray::try_from(arr)?;
                let compressed_fields = self.compress_all(strct.children())?;
                let validity = self.compress_validity(strct.validity())?;
                done(
                    StructArray::try_new(
                        strct.names().clone(),
                        compressed_fields,
                        strct.len(),
                        validity,
                    )?
                    .into_array(),
                )
            }
            _ => {
                // Otherwise, we run sampled compression over pluggable encodings

                let mut rng = StdRng::seed_from_u64(self.options.rng_seed);
                let sampled = sampled_compression(arr, self, &mut rng)?;
                match sampled {
                    Some(compressed) => Ok(compressed),
                    None => done(arr.clone()),
                }
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

    let mut candidates = compressor
        .compressors
        .iter()
        .filter(|&&x| compressor.is_enabled_on(x))
        .filter(|compression| {
            if compression.can_compress(array).is_some() {
                if compressor.depth + compression.cost() > compressor.options.max_depth {
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
        .collect::<Vec<_>>();
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
    let mut best_ratio = 1.0;
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
            compression.recursively_compress(sample, None, ctx.for_compressor(compression))?;
        let ratio = compressed_sample.nbytes() as f32 / sample.nbytes() as f32;
        debug!("{} ratio for {}: {}", ctx, compression.id(), ratio);
        if ratio < best_ratio {
            best_ratio = ratio;
            best = Some(compressed_sample)
        }
    }
    Ok(best.unwrap_or_else(|| CompressedArray::uncompressed(sample.clone())))
}
