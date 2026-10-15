// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright the Vortex contributors

//! Reuse deserialized layout data across layout nodes that share their serialized metadata.
//!
//! Wide files repeat the same layout node for every column. Layouts that opt in with
//! [`VTable::reuse_data`](crate::VTable::reuse_data) deserialize and validate each distinct
//! node once per file instead of once per column.

use std::any::Any;
use std::env;
use std::sync::Arc;

use parking_lot::Mutex;
use vortex_array::dtype::DType;
use vortex_utils::aliases::hash_map::HashMap;

use crate::LayoutId;

/// How [`LayoutBuildCache`] decides two layout nodes are the same.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LayoutBuildCacheMode {
    /// Never reuse layout data.
    Off,
    /// Nodes are the same if their metadata is the same bytes in the layout flatbuffer, which
    /// happens when the writer shares identical metadata by offset.
    Index,
    /// Nodes are the same if their metadata bytes are equal, wherever they are stored.
    Semantic,
}

impl LayoutBuildCacheMode {
    /// Read the mode from `VORTEX_LAYOUT_BUILD_CACHE` (`off`, `index` or `semantic`).
    pub fn from_env() -> Self {
        match env::var("VORTEX_LAYOUT_BUILD_CACHE").as_deref() {
            Ok("index") => Self::Index,
            Ok("semantic") => Self::Semantic,
            _ => Self::Off,
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
enum MetadataKey {
    /// Address and length of the metadata within the layout flatbuffer.
    Index(usize, usize),
    Semantic(Vec<u8>),
}

#[derive(PartialEq, Eq, Hash)]
struct CacheKey {
    layout: LayoutId,
    metadata: MetadataKey,
    dtype: DType,
    row_count: u64,
}

/// Per-file cache of deserialized layout data, keyed by layout id, metadata, dtype and row count.
pub struct LayoutBuildCache {
    mode: LayoutBuildCacheMode,
    entries: Mutex<HashMap<CacheKey, Arc<dyn Any + Send + Sync>>>,
}

impl LayoutBuildCache {
    /// Create an empty cache. Returns `None` for [`LayoutBuildCacheMode::Off`].
    pub fn new(mode: LayoutBuildCacheMode) -> Option<Arc<Self>> {
        (mode != LayoutBuildCacheMode::Off).then(|| {
            Arc::new(Self {
                mode,
                entries: Mutex::new(HashMap::default()),
            })
        })
    }

    fn key(&self, layout: LayoutId, metadata: &[u8], dtype: &DType, row_count: u64) -> CacheKey {
        let metadata = match self.mode {
            LayoutBuildCacheMode::Index | LayoutBuildCacheMode::Off => {
                MetadataKey::Index(metadata.as_ptr() as usize, metadata.len())
            }
            LayoutBuildCacheMode::Semantic => MetadataKey::Semantic(metadata.to_vec()),
        };
        CacheKey {
            layout,
            metadata,
            dtype: dtype.clone(),
            row_count,
        }
    }

    pub(crate) fn get<T: Clone + 'static>(
        &self,
        layout: LayoutId,
        metadata: &[u8],
        dtype: &DType,
        row_count: u64,
    ) -> Option<T> {
        let key = self.key(layout, metadata, dtype, row_count);
        self.entries
            .lock()
            .get(&key)
            .and_then(|data| data.downcast_ref::<T>())
            .cloned()
    }

    pub(crate) fn insert<T: Clone + Send + Sync + 'static>(
        &self,
        layout: LayoutId,
        metadata: &[u8],
        dtype: &DType,
        row_count: u64,
        data: &T,
    ) {
        let key = self.key(layout, metadata, dtype, row_count);
        self.entries.lock().insert(key, Arc::new(data.clone()));
    }
}
