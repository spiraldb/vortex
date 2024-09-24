use std::fmt::Debug;

pub use layouts::{ChunkedLayoutSpec, ColumnLayoutSpec};
use vortex::array::BoolArray;
use vortex::validity::Validity;
use vortex::{Array, IntoArray as _, IntoArrayVariant as _};
use vortex_error::VortexResult;

mod batch;
mod buffered;
mod builder;
mod cache;
mod context;
mod filtering;
mod footer;
mod layouts;
mod stream;

pub use builder::LayoutReaderBuilder;
pub use cache::LayoutMessageCache;
pub use context::*;
pub use filtering::RowFilter;
pub use stream::LayoutBatchStream;
pub use vortex_schema::projection::Projection;
pub use vortex_schema::Schema;

use crate::stream_writer::ByteRange;

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
    ReadMore(Vec<(MessageId, ByteRange)>),
    Batch(Array),
}

pub trait LayoutReader: Debug + Send {
    /// Reads the data from the underlying layout
    ///
    /// The layout can either return a batch data, i.e. an Array or ask for more layout messages to
    /// be read. When requesting messages to be read the caller should populate the messages in the cache
    /// and then call back into this function.
    ///
    /// The layout is finished reading when it returns None
    fn read_next(&mut self) -> VortexResult<Option<ReadResult>>;

    // TODO(robert): Support stats pruning via planning. Requires propagating all the metadata
    //  to top level and then pushing down the result of it
    // Try to use metadata of the layout to perform pruning given the passed `Scan` object.
    //
    // The layout should perform any planning that's cheap and doesn't require reading the data.
    // fn plan(&mut self, scan: Scan) -> VortexResult<Option<PlanResult>>;
}

pub fn null_as_false(array: BoolArray) -> VortexResult<Array> {
    match array.validity() {
        Validity::NonNullable => Ok(array.into_array()),
        Validity::AllValid => {
            Ok(BoolArray::try_new(array.boolean_buffer(), Validity::NonNullable)?.into_array())
        }
        Validity::AllInvalid => Ok(BoolArray::from(vec![false; array.len()]).into_array()),
        Validity::Array(v) => {
            let bool_buffer = &array.boolean_buffer() & &v.into_bool()?.boolean_buffer();
            Ok(BoolArray::from(bool_buffer).into_array())
        }
    }
}

#[cfg(test)]
mod tests {
    use vortex::array::BoolArray;
    use vortex::validity::Validity;
    use vortex::IntoArrayVariant;

    use super::*;

    #[test]
    fn coerces_nulls() {
        let bool_array = BoolArray::from_vec(
            vec![true, true, false, false],
            Validity::Array(BoolArray::from(vec![true, false, true, false]).into()),
        );
        let non_null_array = null_as_false(bool_array).unwrap().into_bool().unwrap();
        assert_eq!(
            non_null_array.boolean_buffer().iter().collect::<Vec<_>>(),
            vec![true, false, false, false]
        );
    }
}
