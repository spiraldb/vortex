use std::sync::{Arc, RwLock};

use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::{vortex_bail, vortex_panic, VortexResult};
use vortex_scalar::Scalar;

use crate::arc_slice::SharedVec;
use crate::encoding::EncodingRef;
use crate::stats::{Stat, Statistics, StatsSet};
use crate::{Array, ArrayDType, ArrayMetadata, ToArray};

#[derive(Clone, Debug)]
pub struct ArrayData {
    encoding: EncodingRef,
    dtype: DType, // FIXME(ngates): Arc?
    len: usize,
    metadata: Arc<dyn ArrayMetadata>,
    buffer: Option<Buffer>,
    children: SharedVec<Array>,
    stats_map: Arc<RwLock<StatsSet>>,
}

impl ArrayData {
    pub fn try_new(
        encoding: EncodingRef,
        dtype: DType,
        len: usize,
        metadata: Arc<dyn ArrayMetadata>,
        buffer: Option<Buffer>,
        children: SharedVec<Array>,
        statistics: StatsSet,
    ) -> VortexResult<Self> {
        let data = Self {
            encoding,
            dtype,
            len,
            metadata,
            buffer,
            children,
            stats_map: Arc::new(RwLock::new(statistics)),
        };

        let array = Array::from(data);
        // Validate here that the metadata correctly parses, so that an encoding can infallibly
        // FIXME(robert): Encoding::with_dyn no longer eagerly validates metadata, come up with a way to validate metadata
        encoding.with_dyn(&array, &mut |_| Ok(()))?;

        Ok(array.into())
    }

    pub fn encoding(&self) -> EncodingRef {
        self.encoding
    }

    pub fn dtype(&self) -> &DType {
        &self.dtype
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn metadata(&self) -> &Arc<dyn ArrayMetadata> {
        &self.metadata
    }

    pub fn buffer(&self) -> Option<&Buffer> {
        self.buffer.as_ref()
    }

    pub fn into_buffer(self) -> Option<Buffer> {
        self.buffer
    }

    // We want to allow these panics because they are indicative of implementation error.
    #[allow(clippy::panic_in_result_fn)]
    pub fn child(&self, index: usize, dtype: &DType, len: usize) -> VortexResult<&Array> {
        match self.children.get(index) {
            None => vortex_bail!(
                "ArrayData::child({}): child {index} not found",
                self.encoding.id().as_ref()
            ),
            Some(child) => {
                assert_eq!(
                    child.dtype(),
                    dtype,
                    "child {index} requested with incorrect dtype for encoding {}",
                    self.encoding().id().as_ref(),
                );
                assert_eq!(
                    child.len(),
                    len,
                    "child {index} requested with incorrect length for encoding {}",
                    self.encoding.id().as_ref(),
                );
                Ok(child)
            }
        }
    }

    pub fn nchildren(&self) -> usize {
        self.children.len()
    }

    pub fn children(&self) -> &[Array] {
        &self.children
    }

    pub fn statistics(&self) -> &dyn Statistics {
        self
    }
}

impl ToArray for ArrayData {
    fn to_array(&self) -> Array {
        Array::Data(self.clone())
    }
}

impl From<Array> for ArrayData {
    fn from(value: Array) -> ArrayData {
        match value {
            Array::Data(d) => d,
            Array::View(_) => value.with_dyn(|v| v.to_array_data()),
        }
    }
}

impl From<ArrayData> for Array {
    fn from(value: ArrayData) -> Array {
        Array::Data(value)
    }
}

impl Statistics for ArrayData {
    fn get(&self, stat: Stat) -> Option<Scalar> {
        self.stats_map
            .read()
            .unwrap_or_else(|_| {
                vortex_panic!(
                    "Failed to acquire read lock on stats map while getting {}",
                    stat
                )
            })
            .get(stat)
            .cloned()
    }

    fn to_set(&self) -> StatsSet {
        self.stats_map
            .read()
            .unwrap_or_else(|_| vortex_panic!("Failed to acquire read lock on stats map"))
            .clone()
    }

    fn set(&self, stat: Stat, value: Scalar) {
        self.stats_map
            .write()
            .unwrap_or_else(|_| {
                vortex_panic!(
                    "Failed to acquire write lock on stats map while setting {} to {}",
                    stat,
                    value
                )
            })
            .set(stat, value);
    }

    fn compute(&self, stat: Stat) -> Option<Scalar> {
        if let Some(s) = self.get(stat) {
            return Some(s);
        }

        self.stats_map
            .write()
            .unwrap_or_else(|_| {
                vortex_panic!("Failed to write to stats map while computing {}", stat)
            })
            .extend(
                self.to_array()
                    .with_dyn(|a| a.compute_statistics(stat))
                    .ok()?,
            );
        self.get(stat)
    }
}
