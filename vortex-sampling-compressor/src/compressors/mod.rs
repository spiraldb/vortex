use std::any::Any;
use std::collections::HashSet;
use std::fmt::{Debug, Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use rand::rngs::StdRng;
use rand::SeedableRng;
use vortex::encoding::EncodingRef;
use vortex::Array;
use vortex_error::{vortex_err, VortexResult};

use crate::{sampled_compression, SamplingCompressor};

pub mod alp;
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

    fn recursively_compress<'a>(
        &'a self,
        array: &Array,
        like: Option<CompressionTree<'a>>,
        ctx: SamplingCompressor<'a>,
    ) -> VortexResult<CompressedArray<'a>> {
        let dyn_self = self.can_compress(array).ok_or(vortex_err!(
            "Encoding {} cannot compress {}",
            self.id(),
            array
        ))?;
        let CompressedArray { array, .. } = dyn_self.compress(array, like.clone(), ctx.clone())?;
        let child_contexts = (0..array.nchildren())
            .map(|index| ctx.auxiliary(&format!("child_{}", index)))
            .collect::<Vec<_>>();
        match like {
            Some(CompressionTree { children, .. }) => {
                let array_children = array.children();
                let arrays_and_trees = array_children.iter().zip(children).zip(child_contexts);
                let mut compressed_children = Vec::with_capacity(array.nchildren());
                let mut compressed_trees = Vec::with_capacity(array.nchildren());
                for ((array, like), ctx) in arrays_and_trees {
                    let compressed = dyn_self.compress(array, like, ctx)?;
                    compressed_children.push(compressed.array);
                    compressed_trees.push(compressed.path);
                }

                let with_compressed_children = array.with_new_children(compressed_children)?;
                Ok(CompressedArray::new(
                    with_compressed_children,
                    Some(CompressionTree::new(dyn_self, compressed_trees)),
                ))
            }
            None => {
                let mut compressed_children = Vec::with_capacity(array.nchildren());
                let mut compressed_trees = Vec::with_capacity(array.nchildren());
                for (array, ctx) in array.children().into_iter().zip(child_contexts.into_iter()) {
                    let maybe_compressed =
                        sampled_compression(&array, &ctx, &mut StdRng::seed_from_u64(0))?;
                    match maybe_compressed {
                        Some(compressed) => {
                            compressed_children.push(compressed.array);
                            compressed_trees.push(compressed.path);
                        }
                        None => {
                            compressed_children.push(array);
                            compressed_trees.push(None);
                        }
                    }
                }

                let with_compressed_children = array.with_new_children(compressed_children)?;
                Ok(CompressedArray::new(
                    with_compressed_children,
                    Some(CompressionTree::new(dyn_self, compressed_trees)),
                ))
            }
        }
    }

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
