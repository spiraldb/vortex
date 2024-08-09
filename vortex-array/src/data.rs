use std::sync::{Arc, RwLock};

use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

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
    children: Arc<[Array]>,
    stats_map: Arc<RwLock<StatsSet>>,
}

impl ArrayData {
    pub fn try_new(
        encoding: EncodingRef,
        dtype: DType,
        len: usize,
        metadata: Arc<dyn ArrayMetadata>,
        buffer: Option<Buffer>,
        children: Arc<[Array]>,
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

        // Validate here that the metadata correctly parses, so that an encoding can infallibly
        let array = data.to_array();
        // FIXME(ngates): run some validation function
        encoding.with_dyn(&array, &mut |_| Ok(()))?;

        Ok(data)
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

    pub fn child(&self, index: usize, dtype: &DType, len: usize) -> Option<&Array> {
        match self.children.get(index) {
            None => None,
            Some(child) => {
                assert_eq!(child.dtype(), dtype, "Child requested with incorrect dtype");
                assert_eq!(child.len(), len, "Child requested with incorrect length");
                Some(child)
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
        match &value {
            Array::Data(d) => d.clone(),
            Array::View(_) => value.clone().into(),
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
        self.stats_map.read().ok()?.get(stat).cloned()
    }

    fn to_set(&self) -> StatsSet {
        self.stats_map.read().unwrap().clone()
    }

    fn set(&self, stat: Stat, value: Scalar) {
        self.stats_map.write().unwrap().set(stat, value);
    }

    fn compute(&self, stat: Stat) -> Option<Scalar> {
        if let Some(s) = self.get(stat) {
            return Some(s);
        }

        self.stats_map
            .write()
            .unwrap_or_else(|_| panic!("Failed to write to stats map"))
            .extend(
                self.to_array()
                    .with_dyn(|a| a.compute_statistics(stat))
                    .ok()?,
            );
        self.get(stat)
    }
}
