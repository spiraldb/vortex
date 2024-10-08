use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use vortex::encoding::EncodingRef;
use vortex::Array;
use vortex_error::VortexResult;

use crate::SamplingCompressor;

pub mod alp;
pub mod alp_rd;
pub mod bitpacked;
pub mod constant;
pub mod date_time_parts;
pub mod delta;
pub mod dict;
pub mod r#for;
pub mod fsst;
pub mod roaring_bool;
pub mod roaring_int;
pub mod runend;
pub mod sparse;
pub mod zigzag;

pub trait EncodingCompressor: Sync + Send + Debug {
    fn id(&self) -> &str;

    fn cost(&self) -> u8 {
        1
    }

    fn can_compress(&self, array: &Array) -> Option<&dyn EncodingCompressor>;

    fn compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>>;

    fn used_encodings(&self) -> HashSet<EncodingRef>;
}

pub type CompressorRef<'a> = &'a dyn EncodingCompressor;

impl PartialEq for dyn EncodingCompressor + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}
impl Eq for dyn EncodingCompressor + '_ {}
impl Hash for dyn EncodingCompressor + '_ {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }
}

#[derive(Clone)]
pub struct CompressionTree<'a> {
    compressor: &'a dyn EncodingCompressor,
    children: Vec<Option<CompressionTree<'a>>>,
    metadata: Option<Arc<dyn EncoderMetadata>>,
}

impl Debug for CompressionTree<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

/// Metadata that can optionally be attached to a compression tree.
///
/// This enables codecs to cache trained parameters from the sampling runs to reuse for
/// the large run.
pub trait EncoderMetadata {
    fn as_any(&self) -> &dyn Any;
}

impl Display for CompressionTree<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.compressor.id(), f)
    }
}

impl<'a> CompressionTree<'a> {
    pub fn flat(compressor: &'a dyn EncodingCompressor) -> Self {
        Self::new(compressor, vec![])
    }

    pub fn new(
        compressor: &'a dyn EncodingCompressor,
        children: Vec<Option<CompressionTree<'a>>>,
    ) -> Self {
        Self {
            compressor,
            children,
            metadata: None,
        }
    }

    /// Save a piece of metadata as part of the compression tree.
    ///
    /// This can be specific encoder parameters that were discovered at sample time
    /// that should be reused when compressing the full array.
    pub(crate) fn new_with_metadata(
        compressor: &'a dyn EncodingCompressor,
        children: Vec<Option<CompressionTree<'a>>>,
        metadata: Arc<dyn EncoderMetadata>,
    ) -> Self {
        Self {
            compressor,
            children,
            metadata: Some(metadata),
        }
    }

    pub fn child(&self, idx: usize) -> Option<&CompressionTree<'a>> {
        self.children[idx].as_ref()
    }

    /// Compresses array with our compressor without verifying that the compressor can compress this array
    pub fn compress_unchecked(
        &self,
        array: &Array,
        ctx: &SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        self.compressor.compress(
            array,
            Some(self.clone()),
            ctx.for_compressor(self.compressor),
        )
    }

    pub fn compress(
        &self,
        array: &Array,
        ctx: &SamplingCompressor<'a>,
    ) -> Option<VortexResult<CompressedArray<'a>>> {
        self.compressor
            .can_compress(array)
            .map(|c| c.compress(array, Some(self.clone()), ctx.for_compressor(c)))
    }

    /// Access the saved opaque metadata.
    ///
    /// This will consume the owned metadata, giving the caller ownership of
    /// the Box.
    ///
    /// The value of `T` will almost always be `EncodingCompressor`-specific.
    pub fn metadata(&mut self) -> Option<Arc<dyn EncoderMetadata>> {
        std::mem::take(&mut self.metadata)
    }

    pub fn child_count_recursive(&self) -> usize {
        self.children
            .iter()
            .filter_map(|child| child.as_ref().map(|c| c.child_count_recursive() + 1))
            .sum::<usize>()
    }
}

#[derive(Debug, Clone)]
pub struct CompressedArray<'a> {
    array: Array,
    path: Option<CompressionTree<'a>>,
}

impl<'a> CompressedArray<'a> {
    pub fn uncompressed(array: Array) -> Self {
        Self::new(array, None)
    }

    pub fn new(array: Array, path: Option<CompressionTree<'a>>) -> Self {
        Self { array, path }
    }

    #[inline]
    pub fn into_array(self) -> Array {
        self.array
    }

    #[inline]
    pub fn path(&self) -> &Option<CompressionTree> {
        &self.path
    }

    #[inline]
    pub fn into_path(self) -> Option<CompressionTree<'a>> {
        self.path
    }

    #[inline]
    pub fn nbytes(&self) -> usize {
        self.array.nbytes()
    }
}

impl AsRef<Array> for CompressedArray<'_> {
    fn as_ref(&self) -> &Array {
        &self.array
    }
}
