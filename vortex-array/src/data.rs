use std::sync::{Arc, RwLock};

use vortex_buffer::Buffer;
use vortex_dtype::DType;
use vortex_error::VortexResult;
use vortex_scalar::Scalar;

use crate::encoding::EncodingRef;
use crate::stats::{Stat, Statistics, StatsSet};
use crate::{Array, ArrayDType, ArrayMetadata, IntoArray, ToArray};

#[derive(Clone, Debug)]
pub struct ArrayData {
    encoding: EncodingRef,
    dtype: DType, // FIXME(ngates): Arc?
    metadata: Arc<dyn ArrayMetadata>,
    buffer: Option<Buffer>,
    children: Arc<[Array]>,
    stats_map: Arc<RwLock<StatsSet>>,
}

impl ArrayData {
    pub fn try_new(
        encoding: EncodingRef,
        dtype: DType,
        metadata: Arc<dyn ArrayMetadata>,
        buffer: Option<Buffer>,
        children: Arc<[Array]>,
        statistics: StatsSet,
    ) -> VortexResult<Self> {
        let data = Self {
            encoding,
            dtype,
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

    pub fn metadata(&self) -> &Arc<dyn ArrayMetadata> {
        &self.metadata
    }

    pub fn buffer(&self) -> Option<&Buffer> {
        self.buffer.as_ref()
    }

    pub fn into_buffer(self) -> Option<Buffer> {
        self.buffer
    }

    pub fn child(&self, index: usize, dtype: &DType) -> Option<&Array> {
        match self.children.get(index) {
            None => None,
            Some(child) => {
                assert_eq!(child.dtype(), dtype, "Child requested with incorrect dtype");
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

impl IntoArray for ArrayData {
    fn into_array(self) -> Array {
        Array::Data(self)
    }
}

impl Statistics for ArrayData {
    fn get(&self, stat: Stat) -> Option<Scalar> {
        self.stats_map.read().unwrap().get(stat).cloned()
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

        self.stats_map.write().unwrap().extend(
            self.to_array()
                .with_dyn(|a| a.compute_statistics(stat))
                .ok()?,
        );
        self.get(stat)
    }
}
