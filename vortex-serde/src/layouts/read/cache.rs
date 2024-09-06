use std::sync::{Arc, RwLock};

use ahash::HashMap;
use bytes::Bytes;
use vortex_dtype::DType;
use vortex_error::vortex_panic;

use crate::layouts::read::{LayoutPartId, MessageId};

#[derive(Default, Debug)]
pub struct LayoutMessageCache {
    cache: HashMap<MessageId, Bytes>,
}

impl LayoutMessageCache {
    pub fn get(&self, path: &[LayoutPartId]) -> Option<Bytes> {
        self.cache.get(path).cloned()
    }

    pub fn remove(&mut self, path: &[LayoutPartId]) -> Option<Bytes> {
        self.cache.remove(path)
    }

    pub fn set(&mut self, path: MessageId, value: Bytes) {
        self.cache.insert(path, value);
    }
}

#[derive(Debug)]
pub struct RelativeLayoutCache {
    root: Arc<RwLock<LayoutMessageCache>>,
    dtype: DType,
    path: MessageId,
}

impl RelativeLayoutCache {
    pub fn new(root: Arc<RwLock<LayoutMessageCache>>, dtype: DType) -> Self {
        Self {
            root,
            dtype,
            path: Vec::new(),
        }
    }

    pub fn relative(&self, id: LayoutPartId, dtype: DType) -> Self {
        let mut new_path = self.path.clone();
        new_path.push(id);
        Self {
            root: self.root.clone(),
            path: new_path,
            dtype,
        }
    }

    pub fn get(&self, path: &[LayoutPartId]) -> Option<Bytes> {
        self.root
            .read()
            .unwrap_or_else(|poison| {
                vortex_panic!(
                    "Failed to read from layout cache at path {:?} with error {}",
                    path,
                    poison
                );
            })
            .get(&self.absolute_id(path))
    }

    pub fn remove(&mut self, path: &[LayoutPartId]) -> Option<Bytes> {
        self.root
            .write()
            .unwrap_or_else(|poison| {
                vortex_panic!(
                    "Failed to write to layout cache at path {:?} with error {}",
                    path,
                    poison
                )
            })
            .remove(&self.absolute_id(path))
    }

    pub fn dtype(&self) -> DType {
        self.dtype.clone()
    }

    pub fn absolute_id(&self, path: &[LayoutPartId]) -> MessageId {
        let mut lookup_key = self.path.clone();
        lookup_key.extend_from_slice(path);
        lookup_key
    }
}
