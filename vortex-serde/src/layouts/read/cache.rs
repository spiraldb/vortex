use std::sync::{Arc, RwLock};

use ahash::HashMap;
use bytes::Bytes;
use vortex_dtype::DType;
use vortex_error::{vortex_panic, VortexExpect};

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
    dtype: Option<DType>,
    path: MessageId,
}

impl RelativeLayoutCache {
    pub fn new(root: Arc<RwLock<LayoutMessageCache>>, dtype: DType) -> Self {
        Self {
            root,
            dtype: Some(dtype),
            path: Vec::new(),
        }
    }

    pub fn relative(&self, id: LayoutPartId, dtype: DType) -> Self {
        let mut new_path = self.path.clone();
        new_path.push(id);
        Self {
            root: self.root.clone(),
            path: new_path,
            dtype: Some(dtype),
        }
    }

    pub fn relative_stored_dtype(&self, id: LayoutPartId) -> Self {
        let mut new_path = self.path.clone();
        new_path.push(id);
        Self {
            root: self.root.clone(),
            path: new_path,
            dtype: None,
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

    pub fn has_dtype(&self) -> bool {
        self.dtype.is_some()
    }

    pub fn dtype(&self) -> &DType {
        self.dtype.as_ref().vortex_expect("Must have dtype")
    }

    pub fn absolute_id(&self, path: &[LayoutPartId]) -> MessageId {
        let mut lookup_key = Vec::with_capacity(self.path.len() + path.len());
        lookup_key.clone_from(&self.path);
        lookup_key.extend_from_slice(path);
        lookup_key
    }
}
