use std::fmt::Debug;

pub use layouts::{ChunkedLayoutSpec, ColumnLayoutSpec};
use projections::Projection;
use vortex::Array;
use vortex_error::VortexResult;

use crate::layouts::reader::filtering::RowFilter;
use crate::writer::ByteRange;

pub mod batch;
pub mod buffered;
pub mod builder;
mod cache;
pub mod context;
pub mod filtering;
mod footer;
mod layouts;
pub mod projections;
pub mod schema;
pub mod stream;

// Recommended read-size according to the AWS performance guide
const INITIAL_READ_SIZE: usize = 8 * 1024 * 1024;
const DEFAULT_BATCH_SIZE: usize = 65536;
const FILE_POSTSCRIPT_SIZE: usize = 20;

#[allow(dead_code)]
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
