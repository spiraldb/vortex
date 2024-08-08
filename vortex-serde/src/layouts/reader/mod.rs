use std::fmt::Debug;
use std::sync::{Arc, RwLock};

use ahash::HashMap;
use bytes::Bytes;
pub use layouts::{ChunkedLayoutSpec, ColumnLayoutSpec};
use projections::Projection;
use vortex::Array;
use vortex_dtype::DType;
use vortex_error::VortexResult;

use crate::layouts::reader::filtering::RowFilter;
use crate::writer::ByteRange;

pub mod batch;
pub mod buffered;
pub mod builder;
pub mod context;
pub mod filtering;
mod footer;
mod layouts;
pub mod projections;
pub mod schema;
pub mod stream;

const DEFAULT_BATCH_SIZE: usize = 65536;

#[derive(Debug, Clone)]
pub struct Scan {
    indices: Option<Array>,
    projection: Projection,
    filter: Option<RowFilter>,
    batch_size: usize,
}

/// Unique identifier for a message within a layout
pub type LayoutPartId = u16;
pub type MessageId = Vec<LayoutPartId>;

#[derive(Debug)]
pub enum ReadResult {
    GetMsgs(Vec<(MessageId, ByteRange)>),
    Batch(Array),
}

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
        self.root.read().unwrap().get(&self.absolute_id(path))
    }

    pub fn remove(&mut self, path: &[LayoutPartId]) -> Option<Bytes> {
        self.root.write().unwrap().remove(&self.absolute_id(path))
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

pub trait Layout: Debug + Send {
    /// Reads the data from the underlying layout
    ///
    /// The layout can either return a batch data, i.e. an Array or ask for more layout messages to
    /// be read. When requesting messages to be read the caller should populate the messages in the cache
    /// and then call back into this function.
    ///
    /// The layout is finished reading when it returns None
    fn read(&mut self) -> VortexResult<Option<ReadResult>>;

    // TODO(robert): Support stats pruning via planning. Requires propagating all the metadata
    //  to top level and then pushing down the result of it
    // Try to use metadata of the layout to perform pruning given the passed `Scan` object.
    //
    // The layout should perform any planning that's cheap and doesn't require reading the data.
    // fn plan(&mut self, scan: Scan) -> VortexResult<Option<PlanResult>>;
}
