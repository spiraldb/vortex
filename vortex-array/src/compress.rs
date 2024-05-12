use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};

use log::{debug, info, warn};
use vortex_error::{vortex_bail, VortexResult};

use crate::array::chunked::{Chunked, ChunkedArray};
use crate::array::constant::{Constant, ConstantArray};
use crate::array::r#struct::{Struct, StructArray};
use crate::compute::scalar_at::scalar_at;
use crate::compute::slice::slice;
use crate::encoding::{ArrayEncoding, EncodingRef};
use crate::sampling::stratified_slices;
use crate::stats::ArrayStatistics;
use crate::validity::Validity;
use crate::{compute, Array, ArrayDType, ArrayDef, ArrayTrait, Context, IntoArray};

pub trait EncodingCompression: ArrayEncoding {
    fn cost(&self) -> u8 {
        1
    }

    fn can_compress(
        &self,
        _array: &Array,
        _config: &CompressConfig,
    ) -> Option<&dyn EncodingCompression> {
        None
    }

    fn compress(
        &self,
        _array: &Array,
        _like: Option<&Array>,
        _ctx: Compressor,
    ) -> VortexResult<Array> {
        vortex_bail!(NotImplemented: "compress", self.id())
    }

    // For an array returned by this encoding, give the size in bytes minus any constant overheads.
    fn compressed_nbytes(&self, array: &Array) -> usize {
        array.with_dyn(|a| a.nbytes())
    }
}

#[derive(Debug, Clone)]
pub struct CompressConfig {
    #[allow(dead_code)]
    block_size: u32,
    sample_size: u16,
    sample_count: u16,
    max_depth: u8,
    // TODO(ngates): can each encoding define their own configs?
    pub ree_average_run_threshold: f32,
}

impl Default for CompressConfig {
    fn default() -> Self {
        // TODO(ngates): we should ensure that sample_size * sample_count <= block_size
        Self {
            block_size: 65_536,
            // Sample length should always be multiple of 1024
            sample_size: 128,
            sample_count: 8,
            max_depth: 3,
            ree_average_run_threshold: 2.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Compressor<'a> {
    ctx: &'a Context,
    options: CompressConfig,

    path: Vec<String>,
    depth: u8,
    /// A set of encodings disabled for this ctx.
    disabled_encodings: HashSet<EncodingRef>,
}

impl Display for Compressor<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}|{}]", self.depth, self.path.join("."))
    }
}

impl<'a> Compressor<'a> {
    pub fn new(ctx: &'a Context) -> Self {
        Self::new_with_options(ctx, Default::default())
    }

    pub fn new_with_options(ctx: &'a Context, options: CompressConfig) -> Self {
        Self {
            ctx,
            options,
            path: Vec::new(),
            depth: 0,
            disabled_encodings: HashSet::new(),
        }
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
        cloned.disabled_encodings = HashSet::new();
        cloned
    }

    pub fn for_encoding(&self, compression: &dyn EncodingCompression) -> Self {
        let mut cloned = self.clone();
        cloned.depth += compression.cost();
        cloned
    }

    #[inline]
    pub fn options(&self) -> &CompressConfig {
        &self.options
    }

    pub fn excluding(&self, encoding: EncodingRef) -> Self {
        let mut cloned = self.clone();
        cloned.disabled_encodings.insert(encoding);
        cloned
    }

    pub fn compress(&self, arr: &Array, like: Option<&Array>) -> VortexResult<Array> {
        if arr.is_empty() {
            return Ok(arr.clone());
        }

        // Attempt to compress using the "like" array, otherwise fall back to sampled compression
        if let Some(l) = like {
            if let Some(compressed) = l
                .encoding()
                .compression()
                .can_compress(arr, self.options())
                .map(|c| c.compress(arr, Some(l), self.for_encoding(l.encoding().compression())))
            {
                let compressed = compressed?;
                if compressed.dtype() != arr.dtype() {
                    panic!(
                        "Compression changed dtype: {:?} -> {:?} for {}",
                        arr.dtype(),
                        compressed.dtype(),
                        compressed.tree_display(),
                    );
                }
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
        if compressed.dtype() != arr.dtype() {
            panic!(
                "Compression changed dtype: {:?} -> {:?} for {}",
                arr.dtype(),
                compressed.dtype(),
                compressed.tree_display(),
            );
        }
        Ok(compressed)
    }

    pub fn compress_validity(&self, validity: Validity) -> VortexResult<Validity> {
        match validity {
            Validity::Array(a) => Ok(Validity::Array(self.compress(&a, None)?)),
            a => Ok(a),
        }
    }

    fn compress_array(&self, arr: &Array) -> VortexResult<Array> {
        match arr.encoding().id() {
            Chunked::ID => {
                // For chunked arrays, we compress each chunk individually
                let chunked = ChunkedArray::try_from(arr)?;
                let compressed_chunks: VortexResult<Vec<Array>> = chunked
                    .chunks()
                    .map(|chunk| self.compress_array(&chunk))
                    .collect();
                Ok(
                    ChunkedArray::try_new(compressed_chunks?, chunked.dtype().clone())?
                        .into_array(),
                )
            }
            Constant::ID => {
                // Not much better we can do than constant!
                Ok(arr.clone())
            }
            Struct::ID => {
                // For struct arrays, we compress each field individually
                let strct = StructArray::try_from(arr)?;
                let compressed_fields = strct
                    .children()
                    .map(|field| self.compress_array(&field))
                    .collect::<VortexResult<Vec<_>>>()?;
                let validity = self.compress_validity(strct.validity())?;
                Ok(StructArray::try_new(
                    strct.names().clone(),
                    compressed_fields,
                    strct.len(),
                    validity,
                )?
                .into_array())
            }
            _ => {
                // Otherwise, we run sampled compression over pluggable encodings
                let sampled = sampled_compression(arr, self)?;
                Ok(sampled.unwrap_or_else(|| arr.clone()))
            }
        }
    }
}

pub fn sampled_compression(array: &Array, compressor: &Compressor) -> VortexResult<Option<Array>> {
    // First, we try constant compression and shortcut any sampling.
    if !array.is_empty() && array.statistics().compute_is_constant().unwrap_or(false) {
        return Ok(Some(
            ConstantArray::new(scalar_at(array, 0)?, array.len()).into_array(),
        ));
    }

    let mut candidates: Vec<&dyn EncodingCompression> = compressor
        .ctx
        .encodings()
        .filter(|&encoding| !compressor.disabled_encodings.contains(encoding))
        .map(|encoding| encoding.compression())
        .filter(|compression| {
            if compression
                .can_compress(array, compressor.options())
                .is_some()
            {
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
        candidates.retain(|&compression| compression.id() != array.encoding().id());
    }

    if array.len()
        <= (compressor.options.sample_size as usize * compressor.options.sample_count as usize)
    {
        // We're either already within a sample, or we're operating over a sufficiently small array.
        return find_best_compression(candidates, array, compressor)
            .map(|best| best.map(|(_compression, best)| best));
    }

    // Take a sample of the array, then ask codecs for their best compression estimate.
    let sample = compute::as_contiguous::as_contiguous(
        &stratified_slices(
            array.len(),
            compressor.options.sample_size,
            compressor.options.sample_count,
        )
        .into_iter()
        .map(|(start, stop)| slice(array, start, stop).unwrap())
        .collect::<Vec<_>>(),
    )?;

    find_best_compression(candidates, &sample, compressor)?
        .map(|(compression, best)| {
            info!("{} compressing array {} like {}", compressor, array, best);
            compressor
                .for_encoding(compression)
                .compress(array, Some(&best))
        })
        .transpose()
}

fn find_best_compression<'a>(
    candidates: Vec<&'a dyn EncodingCompression>,
    sample: &Array,
    ctx: &Compressor,
) -> VortexResult<Option<(&'a dyn EncodingCompression, Array)>> {
    let mut best = None;
    let mut best_ratio = 1.0;
    for compression in candidates {
        debug!(
            "{} trying candidate {} for {}",
            ctx,
            compression.id(),
            sample
        );
        if compression.can_compress(sample, ctx.options()).is_none() {
            continue;
        }
        let compressed_sample =
            compression.compress(sample, None, ctx.for_encoding(compression))?;
        let compressed_size = compression.compressed_nbytes(&compressed_sample);
        let ratio = compressed_size as f32 / sample.with_dyn(|a| a.nbytes()) as f32;
        debug!("{} ratio for {}: {}", ctx, compression.id(), ratio);
        if ratio < best_ratio {
            best_ratio = ratio;
            best = Some((compression, compressed_sample))
        }
    }
    Ok(best)
}
